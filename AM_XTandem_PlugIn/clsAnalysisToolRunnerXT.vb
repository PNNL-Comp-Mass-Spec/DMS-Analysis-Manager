Option Strict On

'*********************************************************************************************************
' Written by Matt Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2006, Battelle Memorial Institute
' Created 06/07/2006
'
' Last modified 02/19/2008
' Last modified 06/15/2009 JDS - Added logging using log4net
'*********************************************************************************************************

imports AnalysisManagerBase
Imports PRISM.Files
Imports AnalysisManagerBase.clsGlobal

Public Class clsAnalysisToolRunnerXT
    Inherits clsAnalysisToolRunnerBase

	'*********************************************************************************************************
	'Class for running XTandem analysis
	'*********************************************************************************************************

#Region "Module Variables"
    Protected Const XTANDEM_CONSOLE_OUTPUT As String = "XTandem_ConsoleOutput.txt"

    Protected Const PROGRESS_PCT_XTANDEM_STARTING As Single = 1
    Protected Const PROGRESS_PCT_XTANDEM_LOADING_SPECTRA As Single = 5
    Protected Const PROGRESS_PCT_XTANDEM_COMPUTING_MODELS As Single = 10
    Protected Const PROGRESS_PCT_XTANDEM_REFINEMENT As Single = 50
    Protected Const PROGRESS_PCT_XTANDEM_REFINEMENT_PARTIAL_CLEAVAGE As Single = 50
    Protected Const PROGRESS_PCT_XTANDEM_REFINEMENT_UNANTICIPATED_CLEAVAGE As Single = 70
    Protected Const PROGRESS_PCT_XTANDEM_REFINEMENT_FINISHING As Single = 85
    Protected Const PROGRESS_PCT_XTANDEM_MERGING_RESULTS As Single = 90
    Protected Const PROGRESS_PCT_XTANDEM_CREATING_REPORT As Single = 95
    Protected Const PROGRESS_PCT_XTANDEM_COMPLETE As Single = 99

    Protected WithEvents CmdRunner As clsRunDosProgram

    Protected mToolVersionWritten As Boolean
    Protected mXTandemVersion As String = String.Empty

#End Region

#Region "Methods"
	''' <summary>
	''' Runs XTandem tool
	''' </summary>
	''' <returns>CloseOutType enum indicating success or failure</returns>
	''' <remarks></remarks>
    Public Overrides Function RunTool() As IJobParams.CloseOutType

        Dim CmdStr As String
        Dim result As IJobParams.CloseOutType
        Dim blnSuccess As Boolean

        'Do the base class stuff
        If Not MyBase.RunTool = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        ' Note: we will store the XTandem version info in the database after the first line is written to file XTandem_ConsoleOutput.txt
        mToolVersionWritten = False
        mXTandemVersion = String.Empty

        ' Make sure the _DTA.txt file is valid
        If Not ValidateCDTAFile() Then
            Return IJobParams.CloseOutType.CLOSEOUT_NO_DTA_FILES
        End If

        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Running XTandem")

        CmdRunner = New clsRunDosProgram(m_WorkDir)

        If m_DebugLevel > 4 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerXT.OperateAnalysisTool(): Enter")
        End If

        ' verify that program file exists
        Dim progLoc As String = m_mgrParams.GetParam("xtprogloc")
        If Not System.IO.File.Exists(progLoc) Then
            If progLoc.Length = 0 Then progLoc = "Parameter 'xtprogloc' not defined for this manager"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Cannot find XTandem program file: " & progLoc)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        'Set up and execute a program runner to run X!Tandem
        CmdStr = "input.xml"

        With CmdRunner
            .CreateNoWindow = True
            .CacheStandardOutput = True
            .EchoOutputToConsole = True

            .WriteConsoleOutputToFile = True
            .ConsoleOutputFilePath = System.IO.Path.Combine(m_WorkDir, XTANDEM_CONSOLE_OUTPUT)
        End With

        m_progress = PROGRESS_PCT_XTANDEM_STARTING

        blnSuccess = CmdRunner.RunProgram(progLoc, CmdStr, "XTandem", True)

        If Not mToolVersionWritten Then
            If String.IsNullOrWhiteSpace(mXTandemVersion) Then
                ParseConsoleOutputFile(System.IO.Path.Combine(m_WorkDir, XTANDEM_CONSOLE_OUTPUT))
            End If
            mToolVersionWritten = StoreToolVersionInfo()
        End If

        If Not blnSuccess Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error running XTandem, job " & m_JobNum)

            If CmdRunner.ExitCode <> 0 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Tandem.exe returned a non-zero exit code: " & CmdRunner.ExitCode.ToString)
            Else
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Call to Tandem.exe failed (but exit code is 0)")
            End If

            ' Note: Job 553883 returned error code -1073740777, which indicated that the _xt.xml file was not fully written

            ' Move the source files and any results to the Failed Job folder
            ' Useful for debugging XTandem problems
            CopyFailedResultsToArchiveFolder()

            Return IJobParams.CloseOutType.CLOSEOUT_FAILED

        End If

        'Stop the job timer
        m_StopTime = System.DateTime.Now

        'Add the current job data to the summary file
        If Not UpdateSummaryFile() Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Error creating summary file, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step"))
        End If

        'Make sure objects are released
        System.Threading.Thread.Sleep(2000)        '2 second delay
        GC.Collect()
        GC.WaitForPendingFinalizers()

        'Zip the output file
        result = ZipMainOutputFile()
        If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            ' Move the source files and any results to the Failed Job folder
            ' Useful for debugging XTandem problems
            CopyFailedResultsToArchiveFolder()
            Return result
        End If

        result = MakeResultsFolder()
        If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            'TODO: What do we do here?           
            Return result
        End If

        result = MoveResultFiles()
        If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            'TODO: What do we do here?
            ' Note that MoveResultFiles should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
            Return result
        End If

        result = CopyResultsFolderToServer()
        If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            'TODO: What do we do here?
            ' Note that CopyResultsFolderToServer should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
            Return result
        End If

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS 'ZipResult

    End Function


    Protected Sub CopyFailedResultsToArchiveFolder()

        Dim result As IJobParams.CloseOutType

        Dim strFailedResultsFolderPath As String = m_mgrParams.GetParam("FailedResultsFolderPath")
        If String.IsNullOrWhiteSpace(strFailedResultsFolderPath) Then strFailedResultsFolderPath = "??Not Defined??"

        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Processing interrupted; copying results to archive folder: " & strFailedResultsFolderPath)

        ' Bump up the debug level if less than 2
        If m_DebugLevel < 2 Then m_DebugLevel = 2

        ' Try to save whatever files are in the work directory (however, delete the _DTA.txt and _DTA.zip files first)
        Dim strFolderPathToArchive As String
        strFolderPathToArchive = String.Copy(m_WorkDir)

        Try
            System.IO.File.Delete(System.IO.Path.Combine(m_WorkDir, m_Dataset & "_dta.zip"))
            System.IO.File.Delete(System.IO.Path.Combine(m_WorkDir, m_Dataset & "_dta.txt"))
        Catch ex As Exception
            ' Ignore errors here
        End Try

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
        Dim ioAppFileInfo As System.IO.FileInfo = New System.IO.FileInfo(System.Reflection.Assembly.GetExecutingAssembly().Location)

        If m_DebugLevel >= 2 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info")
        End If

        strToolVersionInfo = String.Copy(mXTandemVersion)

        ' Store paths to key files in ioToolFiles
        Dim ioToolFiles As New System.Collections.Generic.List(Of System.IO.FileInfo)
        ioToolFiles.Add(New System.IO.FileInfo(m_mgrParams.GetParam("xtprogloc")))

        Try
            Return MyBase.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles)
        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion: " & ex.Message)
            Return False
        End Try

    End Function

    ''' <summary>
    ''' Parse the X!Tandem console output file to determine the X!Tandem version and to track the search progress
    ''' </summary>
    ''' <param name="strConsoleOutputFilePath"></param>
    ''' <remarks></remarks>
    Private Sub ParseConsoleOutputFile(ByVal strConsoleOutputFilePath As String)

        Try

            If Not System.IO.File.Exists(strConsoleOutputFilePath) Then
                If m_DebugLevel >= 4 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Console output file not found: " & strConsoleOutputFilePath)
                End If

                Exit Sub
            End If

            If m_DebugLevel >= 3 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Parsing file " & strConsoleOutputFilePath)
            End If

          
            Dim srInFile As System.IO.StreamReader
            Dim strLineIn As String
            Dim intLinesRead As Integer

            srInFile = New System.IO.StreamReader(New System.IO.FileStream(strConsoleOutputFilePath, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.ReadWrite))

            intLinesRead = 0
            Do While srInFile.Peek() >= 0
                strLineIn = srInFile.ReadLine()
                intLinesRead += 1

                If Not String.IsNullOrWhiteSpace(strLineIn) Then
                    If intLinesRead = 1 Then
                        ' The first line is the X!Tandem version

                        If m_DebugLevel >= 2 AndAlso String.IsNullOrWhiteSpace(mXTandemVersion) Then
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "X!Tandem version: " & strLineIn)
                        End If

                        mXTandemVersion = String.Copy(strLineIn)

                    Else

                        ' Update progress if the line starts with one of the expected phrases
                        If strLineIn.StartsWith("Loading spectra") Then
                            m_progress = PROGRESS_PCT_XTANDEM_LOADING_SPECTRA

                        ElseIf strLineIn.StartsWith("Computing models") Then
                            m_progress = PROGRESS_PCT_XTANDEM_COMPUTING_MODELS

                        ElseIf strLineIn.StartsWith("Model refinement") Then
                            m_progress = PROGRESS_PCT_XTANDEM_REFINEMENT

                        ElseIf strLineIn.StartsWith("	partial cleavage") Then
                            m_progress = PROGRESS_PCT_XTANDEM_REFINEMENT_PARTIAL_CLEAVAGE

                        ElseIf strLineIn.StartsWith("	unanticipated cleavage") Then
                            m_progress = PROGRESS_PCT_XTANDEM_REFINEMENT_UNANTICIPATED_CLEAVAGE

                        ElseIf strLineIn.StartsWith("	finishing refinement ") Then
                            m_progress = PROGRESS_PCT_XTANDEM_REFINEMENT_FINISHING

                        ElseIf strLineIn.StartsWith("Merging results") Then
                            m_progress = PROGRESS_PCT_XTANDEM_MERGING_RESULTS

                        ElseIf strLineIn.StartsWith("Creating report") Then
                            m_progress = PROGRESS_PCT_XTANDEM_CREATING_REPORT

                        ElseIf strLineIn.StartsWith("Estimated false positives") Then
                            m_progress = PROGRESS_PCT_XTANDEM_COMPLETE

                        End If
                    End If
                End If
            Loop

            srInFile.Close()

        Catch ex As Exception
            ' Ignore errors here
            If m_DebugLevel >= 2 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error parsing console output file (" & strConsoleOutputFilePath & "): " & ex.Message)
            End If
        End Try

    End Sub


    ''' <summary>
    ''' Make sure the _DTA.txt file exists and has at least one spectrum in it
    ''' </summary>
    ''' <returns>True if success; false if failure</returns>
    ''' <remarks></remarks>
    Protected Function ValidateCDTAFile() As Boolean
        Dim strInputFilePath As String
        Dim srReader As System.IO.StreamReader

        Dim blnDataFound As Boolean = False

        Try
            strInputFilePath = System.IO.Path.Combine(m_WorkDir, m_Dataset & "_dta.txt")

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
    ''' Zips concatenated XML output file
    ''' </summary>
    ''' <returns>CloseOutType enum indicating success or failure</returns>
    ''' <remarks></remarks>
    Private Function ZipMainOutputFile() As IJobParams.CloseOutType
        Dim TmpFile As String
        Dim FileList() As String
        Dim TmpFilePath As String

        Try
            FileList = System.IO.Directory.GetFiles(m_WorkDir, "*_xt.xml")
            For Each TmpFile In FileList
                TmpFilePath = System.IO.Path.Combine(m_WorkDir, System.IO.Path.GetFileName(TmpFile))
                If Not MyBase.ZipFile(TmpFilePath, True) Then
                    Dim Msg As String = "Error zipping output files, job " & m_JobNum
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
                    m_message = AppendToComment(m_message, "Error zipping output files")
                    Return IJobParams.CloseOutType.CLOSEOUT_FAILED
                End If
            Next
        Catch ex As Exception
            Dim Msg As String = "clsAnalysisToolRunnerXT.ZipMainOutputFile, Exception zipping output files, job " & m_JobNum & ": " & ex.Message
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
            m_message = AppendToComment(m_message, "Error zipping output files")
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        ' Make sure the XML output files have been deleted (the call to MyBase.ZipFile() above should have done this)
        Try
            FileList = System.IO.Directory.GetFiles(m_WorkDir, "*_xt.xml")
            For Each TmpFile In FileList
                System.IO.File.SetAttributes(TmpFile, System.IO.File.GetAttributes(TmpFile) And (Not System.IO.FileAttributes.ReadOnly))
                System.IO.File.Delete(TmpFile)
            Next
        Catch Err As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerXT.ZipMainOutputFile, Error deleting _xt.xml file, job " & m_JobNum & Err.Message)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    ''' <summary>
    ''' Event handler for CmdRunner.LoopWaiting event
    ''' </summary>
    ''' <remarks></remarks>
    Private Sub CmdRunner_LoopWaiting() Handles CmdRunner.LoopWaiting
        Static dtLastStatusUpdate As System.DateTime = System.DateTime.Now
        Static dtLastConsoleOutputParse As System.DateTime = System.DateTime.Now

        ' Synchronize the stored Debug level with the value stored in the database
        Const MGR_SETTINGS_UPDATE_INTERVAL_SECONDS As Integer = 300
        MyBase.GetCurrentMgrSettingsFromDB(MGR_SETTINGS_UPDATE_INTERVAL_SECONDS)

        'Update the status file (limit the updates to every 5 seconds)
        If System.DateTime.Now.Subtract(dtLastStatusUpdate).TotalSeconds >= 5 Then
            dtLastStatusUpdate = System.DateTime.Now
            m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.RUNNING_TOOL, m_progress, 0, "", "", "", False)
        End If

        If System.DateTime.Now.Subtract(dtLastConsoleOutputParse).TotalSeconds >= 15 Then
            dtLastConsoleOutputParse = System.DateTime.Now

            ParseConsoleOutputFile(System.IO.Path.Combine(m_WorkDir, XTANDEM_CONSOLE_OUTPUT))
            If Not mToolVersionWritten AndAlso Not String.IsNullOrWhiteSpace(mXTandemVersion) Then
                mToolVersionWritten = StoreToolVersionInfo()
            End If

        End If

    End Sub

#End Region

End Class
