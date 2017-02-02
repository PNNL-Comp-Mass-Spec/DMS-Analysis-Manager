'*********************************************************************************************************
' Written by Matt Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2006, Battelle Memorial Institute
' Created 06/07/2006
'
'*********************************************************************************************************

Option Strict On

Imports System.Collections.Generic
Imports System.IO
Imports System.Text.RegularExpressions
Imports System.Threading
Imports AnalysisManagerBase
Imports PRISM.Processes

''' <summary>
''' Class for running XTandem analysis
''' </summary>
''' <remarks></remarks>
Public Class clsAnalysisToolRunnerXT
    Inherits clsAnalysisToolRunnerBase

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

    Protected mCmdRunner As clsRunDosProgram

    Protected mToolVersionWritten As Boolean
    Protected mXTandemVersion As String = String.Empty
    Protected mXTandemResultsCount As Integer               ' This is initially set to -1; it will be updated to the value reported by "Valid models" in the X!Tandem Console Output file

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
        Dim blnNoResults As Boolean

        'Do the base class stuff
        If Not MyBase.RunTool = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        ' Note: we will store the XTandem version info in the database after the first line is written to file XTandem_ConsoleOutput.txt
        mToolVersionWritten = False
        mXTandemVersion = String.Empty
        mXTandemResultsCount = -1
        blnNoResults = False

        ' Make sure the _DTA.txt file is valid
        If Not ValidateCDTAFile() Then
            Return IJobParams.CloseOutType.CLOSEOUT_NO_DTA_FILES
        End If

        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Running XTandem")

        mCmdRunner = New clsRunDosProgram(m_WorkDir)
        RegisterEvents(mCmdRunner)
        AddHandler mCmdRunner.LoopWaiting, AddressOf CmdRunner_LoopWaiting

        If m_DebugLevel > 4 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerXT.OperateAnalysisTool(): Enter")
        End If

        ' Define the path to the X!Tandem .Exe
        Dim progLoc As String = m_mgrParams.GetParam("xtprogloc")
        If progLoc.Length = 0 Then
            m_message = "Parameter 'xtprogloc' not defined for this manager"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        ' Check whether we need to update the program location to use a specific version of X!Tandem
        progLoc = DetermineXTandemProgramLocation(progLoc)

        If String.IsNullOrWhiteSpace(progLoc) Then
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        ElseIf Not File.Exists(progLoc) Then
            m_message = "Cannot find XTandem program file"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & progLoc)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        'Set up and execute a program runner to run X!Tandem
        CmdStr = "input.xml"

        With mCmdRunner
            .CreateNoWindow = True
            .CacheStandardOutput = True
            .EchoOutputToConsole = True

            .WriteConsoleOutputToFile = True
            .ConsoleOutputFilePath = Path.Combine(m_WorkDir, XTANDEM_CONSOLE_OUTPUT)
        End With

        m_progress = PROGRESS_PCT_XTANDEM_STARTING

        blnSuccess = mCmdRunner.RunProgram(progLoc, CmdStr, "XTandem", True)

        ' Parse the console output file one more time to determine the number of peptides found
        ParseConsoleOutputFile(Path.Combine(m_WorkDir, XTANDEM_CONSOLE_OUTPUT))

        If Not mToolVersionWritten Then
            mToolVersionWritten = StoreToolVersionInfo()
        End If

        If Not blnSuccess Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error running XTandem, job " & m_JobNum)

            If mCmdRunner.ExitCode <> 0 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Tandem.exe returned a non-zero exit code: " & mCmdRunner.ExitCode.ToString)
            Else
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Call to Tandem.exe failed (but exit code is 0)")
            End If

            ' Note: Job 553883 returned error code -1073740777, which indicated that the _xt.xml file was not fully written

            ' Move the source files and any results to the Failed Job folder
            ' Useful for debugging XTandem problems
            CopyFailedResultsToArchiveFolder()

            Return IJobParams.CloseOutType.CLOSEOUT_FAILED

        End If

        If mXTandemResultsCount < 0 Then
            m_message = "X!Tandem did not report a ""Valid models"" count"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
            blnNoResults = True
        ElseIf mXTandemResultsCount = 0 Then
            m_message = "No results above threshold"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
            blnNoResults = True
        End If

        'Stop the job timer
        m_StopTime = DateTime.UtcNow

        'Add the current job data to the summary file
        If Not UpdateSummaryFile() Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Error creating summary file, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step"))
        End If

        'Make sure objects are released
        Thread.Sleep(500)        ' 500 msec delay
        clsProgRunner.GarbageCollectNow()

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

        If blnNoResults Then
            Return IJobParams.CloseOutType.CLOSEOUT_NO_DATA
        Else
            Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS 'ZipResult
        End If


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
            File.Delete(Path.Combine(m_WorkDir, m_Dataset & "_dta.zip"))
            File.Delete(Path.Combine(m_WorkDir, m_Dataset & "_dta.txt"))
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
                strFolderPathToArchive = Path.Combine(m_WorkDir, m_ResFolderName)
            End If
        End If

        ' Copy the results folder to the Archive folder
        Dim objAnalysisResults = New clsAnalysisResults(m_mgrParams, m_jobParams)
        objAnalysisResults.CopyFailedResultsToArchiveFolder(strFolderPathToArchive)


    End Sub

    ''' <summary>
    ''' Stores the tool version info in the database
    ''' </summary>
    ''' <remarks></remarks>
    Protected Function StoreToolVersionInfo() As Boolean

        If m_DebugLevel >= 2 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info")
        End If

        Dim strToolVersionInfo = String.Copy(mXTandemVersion)

        ' Store paths to key files in ioToolFiles
        Dim ioToolFiles As New List(Of FileInfo)
        ioToolFiles.Add(New FileInfo(m_mgrParams.GetParam("xtprogloc")))

        Try
            Return MyBase.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles, blnSaveToolVersionTextFile:=True)
        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion: " & ex.Message)
            Return False
        End Try

    End Function

    Protected Function DetermineXTandemProgramLocation(progLoc As String) As String

        ' Check whether the settings file specifies that a specific version of the step tool be used
        Dim strXTandemStepToolVersion As String = m_jobParams.GetParam("XTandem_Version")

        If Not String.IsNullOrWhiteSpace(strXTandemStepToolVersion) Then
            ' progLoc is currently "C:\DMS_Programs\DMS5\XTandem\bin\Tandem.exe" or "C:\DMS_Programs\XTandem\bin\x64\Tandem.exe"
            ' strXTandemStepToolVersion will be similar to "v2011.12.1.1"
            ' Insert the specific version just before \bin\ in progLoc

            Dim intInsertIndex As Integer
            intInsertIndex = progLoc.ToLower().IndexOf("\bin\", StringComparison.Ordinal)

            If intInsertIndex > 0 Then
                Dim strNewProgLoc As String
                strNewProgLoc = Path.Combine(progLoc.Substring(0, intInsertIndex), strXTandemStepToolVersion)
                strNewProgLoc = Path.Combine(strNewProgLoc, progLoc.Substring(intInsertIndex + 1))
                progLoc = String.Copy(strNewProgLoc)
            Else
                m_message = "XTandem program path does not contain \bin\"
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & progLoc)
                progLoc = String.Empty
            End If
        End If

        Return progLoc

    End Function

    ''' <summary>
    ''' Parse the X!Tandem console output file to determine the X!Tandem version and to track the search progress
    ''' </summary>
    ''' <param name="strConsoleOutputFilePath"></param>
    ''' <remarks></remarks>
    Private Sub ParseConsoleOutputFile(strConsoleOutputFilePath As String)

        Dim reExtraceValue = New Regex("= *(\d+)", RegexOptions.Compiled)

        Try

            If Not File.Exists(strConsoleOutputFilePath) Then
                If m_DebugLevel >= 4 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Console output file not found: " & strConsoleOutputFilePath)
                End If

                Exit Sub
            End If

            If m_DebugLevel >= 3 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Parsing file " & strConsoleOutputFilePath)
            End If


            Using srInFile = New StreamReader(New FileStream(strConsoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))

                Dim intLinesRead = 0
                Do While Not srInFile.EndOfStream
                    Dim strLineIn = srInFile.ReadLine()
                    intLinesRead += 1

                    If String.IsNullOrWhiteSpace(strLineIn) Then Continue Do

                    If intLinesRead <= 4 AndAlso String.IsNullOrEmpty(mXTandemVersion) AndAlso strLineIn.StartsWith("X!") Then
                        ' Originally the first line was the X!Tandem version
                        ' Starting in November 2016, the first line is the command line and the second line is a separator (series of dashes)
                        ' The third or fourth line should be the X!Tandem version

                        If m_DebugLevel >= 2 Then
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

                        ElseIf strLineIn.StartsWith("Valid models") Then
                            Dim reMatch = reExtraceValue.Match(strLineIn)
                            If reMatch.Success Then
                                Integer.TryParse(reMatch.Groups(1).Value, mXTandemResultsCount)
                            End If
                        End If
                    End If

                Loop

            End Using

        Catch ex As Exception
            ' Ignore errors here
            If m_DebugLevel >= 2 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error parsing console output file (" & strConsoleOutputFilePath & "): " & ex.Message)
            End If
        End Try

    End Sub

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
            FileList = Directory.GetFiles(m_WorkDir, "*_xt.xml")
            For Each TmpFile In FileList
                TmpFilePath = Path.Combine(m_WorkDir, Path.GetFileName(TmpFile))
                If Not MyBase.ZipFile(TmpFilePath, True) Then
                    Dim Msg As String = "Error zipping output files, job " & m_JobNum
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
                    m_message = clsGlobal.AppendToComment(m_message, "Error zipping output files")
                    Return IJobParams.CloseOutType.CLOSEOUT_FAILED
                End If
            Next
        Catch ex As Exception
            Dim Msg As String = "clsAnalysisToolRunnerXT.ZipMainOutputFile, Exception zipping output files, job " & m_JobNum & ": " & ex.Message
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
            m_message = clsGlobal.AppendToComment(m_message, "Error zipping output files")
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        ' Make sure the XML output files have been deleted (the call to MyBase.ZipFile() above should have done this)
        Try
            FileList = Directory.GetFiles(m_WorkDir, "*_xt.xml")
            For Each TmpFile In FileList
                File.SetAttributes(TmpFile, File.GetAttributes(TmpFile) And (Not FileAttributes.ReadOnly))
                File.Delete(TmpFile)
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
    Private Sub CmdRunner_LoopWaiting()

        Static dtLastConsoleOutputParse As DateTime = DateTime.UtcNow

        UpdateStatusFile()

        If DateTime.UtcNow.Subtract(dtLastConsoleOutputParse).TotalSeconds >= 15 Then
            dtLastConsoleOutputParse = DateTime.UtcNow

            ParseConsoleOutputFile(Path.Combine(m_WorkDir, XTANDEM_CONSOLE_OUTPUT))
            If Not mToolVersionWritten AndAlso Not String.IsNullOrWhiteSpace(mXTandemVersion) Then
                mToolVersionWritten = StoreToolVersionInfo()
            End If

            LogProgress("XTandem")
        End If

    End Sub

#End Region

End Class
