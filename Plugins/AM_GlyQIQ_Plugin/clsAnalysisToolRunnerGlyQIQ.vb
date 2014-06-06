' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Created 05/29/2014
'
'*********************************************************************************************************

Option Strict On

Imports AnalysisManagerBase
Imports System.IO
Imports System.Text.RegularExpressions

Public Class clsAnalysisToolRunnerGlyQIQ
    Inherits clsAnalysisToolRunnerBase

    '*********************************************************************************************************
    'Class for running the GlyQ-IQ
    '*********************************************************************************************************

#Region "Constants and Enums"
    Protected Const MASTER_SOURCE_APPLICATION_FOLDER As String = "\\pnl\projects\OmicsSW\DMS_Programs\GlyQ-IQ\ApplicationFiles"

    Protected Const GLYQ_IQ_CONSOLE_OUTPUT As String = "GlyQ-IQ_ConsoleOutput.txt"

    Protected Const PROGRESS_PCT_STARTING As Single = 1
    Protected Const PROGRESS_PCT_SEARCH_COMPLETE As Single = 95
    Protected Const PROGRESS_PCT_CLEAR_REMOTE_WORKDIR As Single = 97
    Protected Const PROGRESS_PCT_COMPLETE As Single = 99

#End Region

#Region "Module Variables"

    Protected mConsoleOutputErrorMsg As String

    Protected mGlyQIQApplicationFilesFolderPath As String

    Protected WithEvents CmdRunner As clsRunDosProgram

#End Region

#Region "Methods"
    ''' <summary>
    ''' Runs GlyQ-IQ
    ''' </summary>
    ''' <returns>CloseOutType enum indicating success or failure</returns>
    ''' <remarks></remarks>
    Public Overrides Function RunTool() As IJobParams.CloseOutType

        Dim result As IJobParams.CloseOutType

        Try
            'Call base class for initial setup
            If Not MyBase.RunTool = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            If m_DebugLevel > 4 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerGlyQIQ.RunTool(): Enter")
            End If

            ' Lookup the HPC options
            Dim udtHPCOptions As clsAnalysisResources.udtHPCOptionsType = clsAnalysisResources.GetHPCOptions(m_jobParams, m_MachName)

            ' Make sure the remote workdir is empty
            If Not ClearFolder(udtHPCOptions.WorkDirPath) Then
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            mGlyQIQApplicationFilesFolderPath = String.Empty

            ' Determine the path to the GlyQIQ application files folder
            mGlyQIQApplicationFilesFolderPath = clsAnalysisResourcesGlyQIQ.GetGlyQIQAppFilesPath(udtHPCOptions)

            ' Make sure the GlyQ-IQ application files are up-to-date
            If Not SynchronizeGlyQIQApplicationFiles(udtHPCOptions) Then
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            ' Store the GlyQ-IQ version info in the database
            m_message = String.Empty
            If Not StoreToolVersionInfo() Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Aborting since StoreToolVersionInfo returned false")
                If String.IsNullOrEmpty(m_message) Then
                    m_message = "Error determining GlyQ-IQ version"
                End If
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            ' Synchronize the working directory with HPC
            If Not CopyDataFilesToHPC(udtHPCOptions) Then
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            ' Run GlyQ-IQ
            Dim blnSuccess = RunGlyQIQ()

            ' Copy the results to the local computer
            blnSuccess = RetrieveGlyQIQResults(udtHPCOptions)

            If Not blnSuccess Then
                ' Retrieve the log files and any results files so that we have a copy of them
                RetrieveResultsSubfolder(udtHPCOptions, "Results")
            End If

            ' Zip up the settings files and batch files so we have a record of them
            PackageResults(udtHPCOptions)

            m_progress = PROGRESS_PCT_CLEAR_REMOTE_WORKDIR

            ' Delete all files in the remote workdir
            ClearFolder(udtHPCOptions.WorkDirPath)

            m_progress = PROGRESS_PCT_COMPLETE

            'Stop the job timer
            m_StopTime = DateTime.UtcNow

            'Add the current job data to the summary file
            If Not UpdateSummaryFile() Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Error creating summary file, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step"))
            End If

            'Make sure objects are released
            Threading.Thread.Sleep(2000)        '2 second delay
            PRISM.Processes.clsProgRunner.GarbageCollectNow()

            If Not blnSuccess Then
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            result = MakeResultsFolder()
            If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                'MakeResultsFolder handles posting to local log, so set database error message and exit
                m_message = "Error making results folder"
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            result = MoveResultFiles()
            If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                ' Note that MoveResultFiles should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
                m_message = "Error moving files into results folder"
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            result = CopyResultsFolderToServer()
            If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                ' Note that CopyResultsFolderToServer should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

        Catch ex As Exception
            m_message = "Error in GlyQIQ->RunTool"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    Private Function ClearFolder(ByVal folderPath As String) As Boolean

        Try

            Dim diFolder As DirectoryInfo = New DirectoryInfo(folderPath)

            Dim lstFiles = diFolder.GetFiles("*")

            For Each fiFile In lstFiles
                fiFile.Delete()
            Next

            Dim lstSubFolders = diFolder.GetDirectories()
            For Each diSubFolder In lstSubFolders
                ClearFolder(diSubFolder.FullName)
                diSubFolder.Delete()
            Next

            Return True

        Catch ex As Exception
            m_message = "Exception deleting files from " & folderPath
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message)
            Return False
        End Try

    End Function

    Private Function CopyDataFilesToHPC(ByVal udtHPCOptions As clsAnalysisResources.udtHPCOptionsType) As Boolean

        Try
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Synchronizing working directory with " & udtHPCOptions.WorkDirPath)
            Dim success = SynchronizeFolders(m_WorkDir, udtHPCOptions.WorkDirPath, True)

            Return success

        Catch ex As Exception
            m_message = "Exception copying files to the remote HPC working directory"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message)
            Return False
        End Try

    End Function

    Private Function PackageResults(udtHPCOptions As clsAnalysisResources.udtHPCOptionsType) As Boolean

        Dim diTempZipFolder = New DirectoryInfo(Path.Combine(m_WorkDir, "FilesToZip"))

        Try

            If Not diTempZipFolder.Exists Then
                diTempZipFolder.Create()
            End If

            ' Move the batch files and specific text files into the FilesToZip folder
            Dim diWorkDir = New DirectoryInfo(m_WorkDir)
            Dim lstFilesToMove = New List(Of FileInfo)

            Dim lstFiles = diWorkDir.GetFiles("*.bat")
            lstFilesToMove.AddRange(lstFiles)

            lstFiles = diWorkDir.GetFiles("*.txt")
            For Each fiFile In lstFiles
                If fiFile.Name.Contains("HPC") Then
                    lstFilesToMove.Add(fiFile)
                ElseIf fiFile.Name.StartsWith("0y_HPC_OperationParameters") Then
                    lstFilesToMove.Add(fiFile)
                ElseIf fiFile.Name.StartsWith("GlyQIQ_Params") Then
                    lstFilesToMove.Add(fiFile)
                End If
            Next

            For Each fiFile In lstFilesToMove
                fiFile.MoveTo(Path.Combine(diTempZipFolder.FullName, fiFile.Name))
            Next

            ' Move selected files from the WorkingParameters folder
            Dim diWorkingParamsSource = New DirectoryInfo(Path.Combine(m_WorkDir, "WorkingParameters"))
            Dim diWorkingParamsTarget = New DirectoryInfo(Path.Combine(diTempZipFolder.FullName, "WorkingParameters"))
            If Not diWorkingParamsTarget.Exists Then
                diWorkingParamsTarget.Create()
            End If

            Dim iqParamFileName = m_jobParams.GetJobParameter("ParmFileName", "")
            For Each fiFile In diWorkingParamsSource.GetFiles()
                Dim blnMoveFile = False

                If String.Compare(fiFile.Name, iqParamFileName, True) = 0 Then
                    blnMoveFile = True
                ElseIf fiFile.Name.StartsWith("GlyQIQ_Params") Then
                    blnMoveFile = True
                ElseIf fiFile.Name.StartsWith("AlignmentParameters") Then
                    blnMoveFile = True
                ElseIf fiFile.Name.StartsWith("ExecutorParametersSK") Then
                    blnMoveFile = True
                End If

                If blnMoveFile Then
                    fiFile.MoveTo(Path.Combine(diWorkingParamsTarget.FullName, fiFile.Name))
                End If
            Next

            Dim strZipFilePath = Path.Combine(m_WorkDir, "GlyQIq_Automation_Files.zip")

            m_IonicZipTools.ZipDirectory(diTempZipFolder.FullName, strZipFilePath)

        Catch ex As Exception
            m_message = "Exception creating GlyQIq_Automation_Files.zip"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message)
        End Try

        Try
            ' Clear the TempZipFolder so we can re-use it
            Threading.Thread.Sleep(250)
            diTempZipFolder.Delete(True)
            Threading.Thread.Sleep(250)
            diTempZipFolder.Create()

            ' Zip up all of the files in the G_GridResultsBreakDown folder
            Dim diGridResults = New DirectoryInfo(Path.Combine(m_WorkDir, "G_GridResultsBreakDown"))
            For Each fiFile In diGridResults.GetFiles("*", SearchOption.AllDirectories)
                fiFile.MoveTo(Path.Combine(diTempZipFolder.FullName, fiFile.Name))
            Next

            Dim strZipFilePath = Path.Combine(m_WorkDir, "G_GridResultsBreakDown.zip")

            m_IonicZipTools.ZipDirectory(diTempZipFolder.FullName, strZipFilePath)

        Catch ex As Exception
            m_message = "Exception zipping results"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message)
        End Try

        Try

            ' Move the ResultsSummary files up one folder
            Dim diResultsSummaryFolder = New DirectoryInfo(Path.Combine(m_WorkDir, "ResultsSummary"))
            For Each fiFile In diResultsSummaryFolder.GetFiles()
                fiFile.MoveTo(Path.Combine(m_WorkDir, fiFile.Name))
            Next

        Catch ex As Exception
            m_message = "Exception zipping results"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message)
        End Try

        Try

            ' Remove extraneous messages from the _ConsoleOutput.txt file
            Dim strConsoleOutputFilePath = Path.Combine(m_WorkDir, GLYQ_IQ_CONSOLE_OUTPUT)
            Dim fiConsoleOutputFileOld = New FileInfo(strConsoleOutputFilePath)
            Dim fiConsoleOutputFileNew = New FileInfo(strConsoleOutputFilePath & ".new")

            Using srInFile = New StreamReader(New FileStream(fiConsoleOutputFileOld.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))

                Dim strLookingForPrefix = "Looking for: " + udtHPCOptions.WorkDirPath

                Using swOutfile = New StreamWriter(New FileStream(fiConsoleOutputFileNew.FullName, FileMode.Create, FileAccess.Write, FileShare.Read))
                    While srInFile.Peek > -1
                        Dim strLineIn = srInFile.ReadLine()

                        If strLineIn.StartsWith(strLookingForPrefix) Then
                            ' Skip this line
                            Continue While
                        End If

                        swOutfile.WriteLine(strLineIn)
                    End While
                End Using
            End Using

            fiConsoleOutputFileOld.MoveTo(fiConsoleOutputFileOld.FullName & ".old")
            fiConsoleOutputFileOld.Refresh()
            m_jobParams.AddResultFileToSkip(fiConsoleOutputFileOld.Name)

            fiConsoleOutputFileNew.MoveTo(strConsoleOutputFilePath)

        Catch ex As Exception
            m_message = "Exception updating the console output file"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message)
        End Try

        Return True

    End Function

    Private Function RetrieveGlyQIQResults(ByVal udtHPCOptions As clsAnalysisResources.udtHPCOptionsType) As Boolean

        ' Copy all files from the ResultsSummary folder in the remote working directory
        Dim blnSuccess = RetrieveResultsSubfolder(udtHPCOptions, "ResultsSummary")
        If Not blnSuccess Then Return False

        ' Retrieve the GridResultsBreakDown files
        RetrieveResultsSubfolder(udtHPCOptions, "G_GridResultsBreakDown")

        ' Also copy all files in the remote working directory
        SynchronizeFolders(udtHPCOptions.WorkDirPath, m_WorkDir, False)

        Return True

    End Function

    Private Function RetrieveResultsSubfolder(ByVal udtHPCOptions As clsAnalysisResources.udtHPCOptionsType, ByVal folderName As String) As Boolean

        Try
            Dim resultsFolderSource As String = Path.Combine(udtHPCOptions.WorkDirPath, folderName)
            Dim resultsFolderTarget = Path.Combine(m_WorkDir, folderName)

            Dim success = SynchronizeFolders(resultsFolderSource, resultsFolderTarget, True)

            If Not success AndAlso String.IsNullOrEmpty(m_message) Then
                m_message = "SynchronizeFolders returned false for " & folderName
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
            End If

            Return success

        Catch ex As Exception
            m_message = "Exception retrieving the results files from " & folderName
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message)
            Return False
        End Try

    End Function


    ''' <summary>
    ''' Parse the GlyQ-IQ console output file to track the search progress
    ''' </summary>
    ''' <param name="strConsoleOutputFilePath"></param>
    ''' <remarks></remarks>
    Private Sub ParseConsoleOutputFile(ByVal strConsoleOutputFilePath As String)

        ' Example Console output, looking for lines like this
        '

        Static reProgressStats As Regex = New Regex("We found (?<Done>\d+) out of (?<Total>\d+)", RegexOptions.Compiled Or RegexOptions.IgnoreCase)
        Static dtLastProgressWriteTime As DateTime = DateTime.UtcNow

        Try
            If Not File.Exists(strConsoleOutputFilePath) Then
                If m_DebugLevel >= 4 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Console output file not found: " & strConsoleOutputFilePath)
                End If

                Exit Sub
            End If

            If m_DebugLevel >= 4 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Parsing file " & strConsoleOutputFilePath)
            End If

            Dim strLineIn As String
            Dim glyqIqProgress As Integer = 0

            Using srInFile = New StreamReader(New FileStream(strConsoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))

                Do While srInFile.Peek() >= 0
                    strLineIn = srInFile.ReadLine()

                    If Not String.IsNullOrWhiteSpace(strLineIn) Then

                        Dim reMatch = reProgressStats.Match(strLineIn)

                        If reMatch.Success Then
                            Dim intDone As Integer
                            Dim intTotal As Integer

                            If Integer.TryParse(reMatch.Groups("Done").Value, intDone) Then
                                If Integer.TryParse(reMatch.Groups("Total").Value, intTotal) Then
                                    glyqIqProgress = CInt(intDone / CSng(intTotal) * 100)
                                End If
                            End If
                        End If

                    End If
                Loop

            End Using

            Dim sngActualProgress = ComputeIncrementalProgress(PROGRESS_PCT_STARTING, PROGRESS_PCT_SEARCH_COMPLETE, glyqIqProgress, 100)

            If m_progress < sngActualProgress Then
                m_progress = sngActualProgress
                If m_DebugLevel >= 3 OrElse DateTime.UtcNow.Subtract(dtLastProgressWriteTime).TotalMinutes >= 20 Then
                    dtLastProgressWriteTime = DateTime.UtcNow
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, " ... " & m_progress.ToString("0") & "% complete")
                End If
            End If

        Catch ex As Exception
            ' Ignore errors here
            If m_DebugLevel >= 2 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error parsing console output file (" & strConsoleOutputFilePath & "): " & ex.Message)
            End If
        End Try

    End Sub

    Protected Function RunGlyQIQ() As Boolean

        Dim blnSuccess As Boolean

        Dim remoteBatchFilePath = m_jobParams.GetParam(clsAnalysisResourcesGlyQIQ.GLYQ_IQ_LAUNCHER_FILE_PARAM_NAME, "")

        mConsoleOutputErrorMsg = String.Empty

        Dim rawDataType As String = m_jobParams.GetParam("RawDataType")
        Dim eRawDataType = clsAnalysisResources.GetRawDataType(rawDataType)

        If eRawDataType = clsAnalysisResources.eRawDataTypeConstants.ThermoRawFile Then
            m_jobParams.AddResultFileExtensionToSkip(clsAnalysisResources.DOT_RAW_EXTENSION)
        Else
            m_message = "GlyQ-IQ presently only supports Thermo .Raw files"
            Return False
        End If

        Dim fiLauncherFile = New FileInfo(remoteBatchFilePath)

        ' Set up and execute a program runner to run the batch file that launches GlyQ-IQ
        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Launching GlyQ-IQ using " & fiLauncherFile.FullName)

        CmdRunner = New clsRunDosProgram(fiLauncherFile.Directory.FullName)

        With CmdRunner
            .CreateNoWindow = True
            .CacheStandardOutput = False
            .EchoOutputToConsole = True

            .WriteConsoleOutputToFile = True
            .ConsoleOutputFilePath = Path.Combine(m_WorkDir, GLYQ_IQ_CONSOLE_OUTPUT)
        End With

        m_progress = PROGRESS_PCT_STARTING

        Dim CmdStr As String = String.Empty
        blnSuccess = CmdRunner.RunProgram(fiLauncherFile.FullName, CmdStr, "GlyQ-IQ", True)

        If Not String.IsNullOrEmpty(mConsoleOutputErrorMsg) Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mConsoleOutputErrorMsg)
        End If

        ' Note that the console output will end with these messages
        ' This is normal, and is nothing to worry about
        '
        ' CMD.EXE was started with the above path as the current directory.
        ' UNC paths are not supported.  Defaulting to Windows directory.
        ' The system cannot find the file specified.

        If Not blnSuccess Then
            Dim Msg As String
            Msg = "Error running GlyQ-IQ"
            m_message = clsGlobal.AppendToComment(m_message, Msg)

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg & ", job " & m_JobNum)

            If CmdRunner.ExitCode <> 0 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "GlyQ-IQ returned a non-zero exit code: " & CmdRunner.ExitCode.ToString)
            Else
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Call to GlyQ-IQ failed (but exit code is 0)")
            End If

            Return False
        End If

        m_progress = PROGRESS_PCT_SEARCH_COMPLETE
        m_StatusTools.UpdateAndWrite(m_progress)
        If m_DebugLevel >= 3 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "GlyQ-IQ Analysis Complete")
        End If

        Return True

    End Function

    Protected Function SynchronizeGlyQIQApplicationFiles(udtHPCOptions As clsAnalysisResources.udtHPCOptionsType) As Boolean

        Try
            Const appFolderSource As String = MASTER_SOURCE_APPLICATION_FOLDER
            Dim appFolderTarget = mGlyQIQApplicationFilesFolderPath

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Synchronizing GlyQ-IQ application files using " & appFolderSource)

            Dim success = SynchronizeFolders(appFolderSource, appFolderTarget, True)

            If Not success AndAlso String.IsNullOrEmpty(m_message) Then
                m_message = "SynchronizeFolders returned false for GlyQ-IQ Application Files"
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
                Return False
            End If

            ' Copy theRemoteThermo folder into the working directory
            Dim remoteThermoFolderSource = Path.Combine(MASTER_SOURCE_APPLICATION_FOLDER, "RemoteThermo")
            Dim remoteThermoFolderTarget = Path.Combine(udtHPCOptions.WorkDirPath, "RemoteThermo")

            success = SynchronizeFolders(remoteThermoFolderSource, remoteThermoFolderTarget, True)

            If Not success AndAlso String.IsNullOrEmpty(m_message) Then
                m_message = "SynchronizeFolders returned false for RemoteThermo"
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
                Return False
            End If

            Return success

        Catch ex As Exception
            m_message = "Exception synchronizing the GlyQ-IQ application files"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message)
            Return False
        End Try

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

        Dim applicationFolderPath = Path.Combine(mGlyQIQApplicationFilesFolderPath, "GlyQ-IQ_Application\Release")
        Dim consoleAppPath = Path.Combine(applicationFolderPath, "IQGlyQ_Console.exe")

        ' Lookup the version of the GlyQ application
        Dim blnSuccess = MyBase.StoreToolVersionInfoOneFile(strToolVersionInfo, consoleAppPath)
        If Not blnSuccess Then Return False

        ' Store paths to key files in ioToolFiles
        Dim ioToolFiles As New List(Of FileInfo)
        ioToolFiles.Add(New FileInfo(consoleAppPath))

        Dim lstDLLs = New List(Of String)
        lstDLLs.Add("IQGlyQ.dll")
        lstDLLs.Add("IQ2.dll")
        lstDLLs.Add("Run32.dll")

        For Each dllName In lstDLLs
            ioToolFiles.Add(New FileInfo(Path.Combine(applicationFolderPath, dllName)))
        Next

        Try
            Return MyBase.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles)
        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion: " & ex.Message)
            Return False
        End Try

    End Function

    Private Sub UpdateStatusRunning(ByVal sngPercentComplete As Single)
        m_progress = sngPercentComplete
        m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.RUNNING_TOOL, sngPercentComplete, 0, "", "", "", False)
    End Sub

#End Region

#Region "Event Handlers"

    ''' <summary>
    ''' Event handler for CmdRunner.LoopWaiting event
    ''' </summary>
    ''' <remarks></remarks>
    Private Sub CmdRunner_LoopWaiting() Handles CmdRunner.LoopWaiting
        Static dtLastStatusUpdate As DateTime = DateTime.UtcNow
        Static dtLastConsoleOutputParse As DateTime = DateTime.UtcNow

        ' Synchronize the stored Debug level with the value stored in the database
        Const MGR_SETTINGS_UPDATE_INTERVAL_SECONDS As Integer = 300
        MyBase.GetCurrentMgrSettingsFromDB(MGR_SETTINGS_UPDATE_INTERVAL_SECONDS)

        'Update the status file (limit the updates to every 5 seconds)
        If DateTime.UtcNow.Subtract(dtLastStatusUpdate).TotalSeconds >= 5 Then
            dtLastStatusUpdate = DateTime.UtcNow
            UpdateStatusRunning(m_progress)
        End If

        If DateTime.UtcNow.Subtract(dtLastConsoleOutputParse).TotalSeconds >= 15 Then
            dtLastConsoleOutputParse = DateTime.UtcNow

            ParseConsoleOutputFile(Path.Combine(m_WorkDir, GLYQ_IQ_CONSOLE_OUTPUT))

        End If

    End Sub

#End Region

End Class
