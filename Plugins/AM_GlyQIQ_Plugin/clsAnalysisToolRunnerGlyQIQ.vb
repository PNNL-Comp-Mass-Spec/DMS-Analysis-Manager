' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Created 05/29/2014
'
'*********************************************************************************************************

Option Strict On

Imports AnalysisManagerBase
Imports System.IO
Imports System.Text.RegularExpressions
Imports System.Threading


Public Class clsAnalysisToolRunnerGlyQIQ
    Inherits clsAnalysisToolRunnerBase

    '*********************************************************************************************************
    'Class for running the GlyQ-IQ
    '*********************************************************************************************************

#Region "Constants and Enums"
    Protected Const MASTER_SOURCE_APPLICATION_FOLDER As String = "\\pnl\projects\OmicsSW\DMS_Programs\GlyQ-IQ\ApplicationFiles"

    Protected Const PROGRESS_PCT_STARTING As Single = 1
    Protected Const PROGRESS_PCT_SEARCH_COMPLETE As Single = 95
    Protected Const PROGRESS_PCT_CLEAR_REMOTE_WORKDIR As Single = 97
    Protected Const PROGRESS_PCT_COMPLETE As Single = 99

    Protected Const USE_THREADING As Boolean = True

#End Region

#Region "Module Variables"

    Protected mCoreCount As Integer
    Protected mGlyQRunners As Dictionary(Of Integer, clsGlyQIqRunner)

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

            ' Determine the path to the IQGlyQ program
            Dim progLoc As String
            progLoc = DetermineProgramLocation("GlyQIQ", "GlyQIQProgLoc", "IQGlyQ_Console.exe")

            If String.IsNullOrWhiteSpace(progLoc) Then
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            ' Store the GlyQ-IQ version info in the database            
            If Not StoreToolVersionInfo(progLoc) Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Aborting since StoreToolVersionInfo returned false")
                If String.IsNullOrEmpty(m_message) Then
                    m_message = "Error determining GlyQ-IQ version"
                End If
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            ' Run GlyQ-IQ
            Dim blnSuccess = RunGlyQIQ()

            If blnSuccess Then

                ' Combine the results files
                Dim diResultsFolder = New DirectoryInfo(Path.Combine(m_WorkDir, "Results_" & m_Dataset))
                If Not diResultsFolder.Exists Then
                    m_message = "Results folder not found"
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
                    blnSuccess = False
                End If

                If blnSuccess Then

                    Using swWriter = New StreamWriter(New FileStream(Path.Combine(m_WorkDir, m_Dataset & "_iqResults_.txt"), FileMode.Create, FileAccess.Write, FileShare.ReadWrite))

                        For core = 1 To mCoreCount
                            Dim fiResultFile = New FileInfo(Path.Combine(diResultsFolder.FullName, m_Dataset & "_iqResults_" & core & ".txt"))

                            If Not fiResultFile.Exists Then
                                If String.IsNullOrEmpty(m_message) Then
                                    m_message = "Result file not found: " & fiResultFile.Name
                                End If
                                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Result file not found: " & fiResultFile.FullName)
                                blnSuccess = False
                                Continue For
                            End If

                            Dim linesRead = 0
                            Using srReader = New StreamReader(New FileStream(fiResultFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                                While Not srReader.EndOfStream
                                    Dim lineIn = srReader.ReadLine()
                                    linesRead += 1

                                    If linesRead = 1 AndAlso core > 1 Then
                                        Continue While
                                    End If

                                    swWriter.WriteLine(lineIn)

                                End While


                            End Using
                        Next

                    End Using
                End If
            End If


            ' Zip up the settings files and batch files so we have a record of them
            PackageResults()

            m_progress = PROGRESS_PCT_CLEAR_REMOTE_WORKDIR


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

    Private Function PackageResults() As Boolean

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
                If fiFile.Name.StartsWith("GlyQ-IQ_ConsoleOutput") Then
                    lstFilesToMove.Add(fiFile)
                End If
            Next

            For Each fiFile In lstFilesToMove
                fiFile.MoveTo(Path.Combine(diTempZipFolder.FullName, fiFile.Name))
            Next

            ' Move selected files from the first WorkingParameters folder
            Dim diWorkingParamsSource = New DirectoryInfo(Path.Combine(m_WorkDir, "WorkingParametersCore1"))
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
                ElseIf fiFile.Name.StartsWith("ExecutorParameters") Then
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
            Return False
        End Try

        Try
            ' Clear the TempZipFolder so we can re-use it
            Threading.Thread.Sleep(250)
            diTempZipFolder.Delete(True)

            Threading.Thread.Sleep(250)
            diTempZipFolder.Create()


        Catch ex As Exception
            m_message = "Exception zipping results"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message)
            Return False
        End Try

        'Try

        '    ' Remove extraneous messages from the _ConsoleOutput.txt file
        '    If mGlyQRunners Is Nothing Then Exit Try

        '    For Each glyqRunner In mGlyQRunners

        '        Dim strConsoleOutputFilePath = String.Copy(glyqRunner.Value.ConsoleOutputFilePath)
        '        Dim fiConsoleOutputFileOld = New FileInfo(strConsoleOutputFilePath)
        '        Dim fiConsoleOutputFileNew = New FileInfo(strConsoleOutputFilePath & ".new")

        '        Using srInFile = New StreamReader(New FileStream(fiConsoleOutputFileOld.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))

        '            Dim strLookingForPrefix = "Looking for: " + m_WorkDir

        '            Using swOutfile = New StreamWriter(New FileStream(fiConsoleOutputFileNew.FullName, FileMode.Create, FileAccess.Write, FileShare.Read))
        '                While srInFile.Peek > -1
        '                    Dim strLineIn = srInFile.ReadLine()

        '                    If strLineIn.StartsWith(strLookingForPrefix)  Then
        '                        ' Skip this line
        '                        Continue While
        '                    End If

        '                    swOutfile.WriteLine(strLineIn)
        '                End While
        '            End Using
        '        End Using

        '        fiConsoleOutputFileOld.MoveTo(fiConsoleOutputFileOld.FullName & ".old")
        '        fiConsoleOutputFileOld.Refresh()
        '        m_jobParams.AddResultFileToSkip(fiConsoleOutputFileOld.Name)

        '        fiConsoleOutputFileNew.MoveTo(strConsoleOutputFilePath)

        '    Next
        'Catch ex As Exception
        '    m_message = "Exception updating the console output file"
        '    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message)
        'End Try

        Return True

    End Function

    Protected Function RunGlyQIQ() As Boolean

        Dim blnSuccess As Boolean

        mCoreCount = m_jobParams.GetJobParameter(clsAnalysisResourcesGlyQIQ.JOB_PARAM_ACTUAL_CORE_COUNT, 0)
        If mCoreCount < 1 Then
            m_message = "Core count reported by " & clsAnalysisResourcesGlyQIQ.JOB_PARAM_ACTUAL_CORE_COUNT & " is 0; unable to continue"
            Return False
        End If

        Dim rawDataType As String = m_jobParams.GetParam("RawDataType")
        Dim eRawDataType = clsAnalysisResources.GetRawDataType(rawDataType)

        If eRawDataType = clsAnalysisResources.eRawDataTypeConstants.ThermoRawFile Then
            m_jobParams.AddResultFileExtensionToSkip(clsAnalysisResources.DOT_RAW_EXTENSION)
        Else
            m_message = "GlyQ-IQ presently only supports Thermo .Raw files"
            Return False
        End If

        ' Set up and execute a program runner to run each batch file that launches GlyQ-IQ

        m_progress = PROGRESS_PCT_STARTING

        mGlyQRunners = New Dictionary(Of Integer, clsGlyQIqRunner)()
        Dim lstThreads As New List(Of Thread)

        For core = 1 To mCoreCount

            Dim batchFilePath = Path.Combine(m_WorkDir, clsAnalysisResourcesGlyQIQ.START_PROGRAM_BATCH_FILE_PREFIX & core & ".bat")

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Launching GlyQ-IQ, core " & core & ": " & batchFilePath)

            Dim glyQRunner = New clsGlyQIqRunner(m_WorkDir, core, batchFilePath)
            AddHandler glyQRunner.CmdRunnerWaiting, AddressOf CmdRunner_LoopWaiting
            mGlyQRunners.Add(core, glyQRunner)

            If USE_THREADING Then
                Dim newThread As New Thread(New ThreadStart(AddressOf glyQRunner.StartAnalysis))
                newThread.Priority = Threading.ThreadPriority.Normal
                newThread.Start()
                lstThreads.Add(newThread)
            Else
                glyQRunner.StartAnalysis()

                If glyQRunner.Status = clsGlyQIqRunner.GlyQIqRunnerStatusCodes.Failure Then
                    m_message = "Error running " & Path.GetFileName(batchFilePath)
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
                    Return False
                End If
            End If
        Next

        If USE_THREADING Then
            ' Wait for all of the threads to exit
            ' Run for a maximum of 14 days

            Dim dtStartTime = DateTime.UtcNow
            While DateTime.UtcNow.Subtract(dtStartTime).TotalDays < 14
                ' Poll the status of each of the threads

                Dim stepsComplete = 0

                For Each glyQRunner In mGlyQRunners
                    Dim eStatus = glyQRunner.Value.Status
                    If eStatus >= clsGlyQIqRunner.GlyQIqRunnerStatusCodes.Success Then
                        ' Analysis completed (or failed)
                        stepsComplete += 1
                    End If
                Next

                If stepsComplete >= mGlyQRunners.Count Then
                    ' All threads are done
                    Exit While
                End If

                Thread.Sleep(2000)

            End While
        End If
     

        blnSuccess = True
        Dim exitCode As Integer = 0

        ' Look for any console output error messages
        For Each glyQRunner In mGlyQRunners

            Dim progRunner = glyQRunner.Value.ProgRunner

            If progRunner Is Nothing Then Continue For

            For Each cachedError In progRunner.CachedConsoleErrors
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Core " & glyQRunner.Key & ": " & cachedError)
                blnSuccess = False
            Next

            If progRunner.ExitCode <> 0 AndAlso exitCode = 0 Then
                exitCode = progRunner.ExitCode
            End If

        Next


        If Not blnSuccess Then
            Dim Msg As String
            Msg = "Error running GlyQ-IQ"
            m_message = clsGlobal.AppendToComment(m_message, Msg)

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg & ", job " & m_JobNum)

            If exitCode <> 0 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "GlyQ-IQ returned a non-zero exit code: " & exitCode.ToString())
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


    ''' <summary>
    ''' Stores the tool version info in the database
    ''' </summary>
    ''' <remarks></remarks>
    Protected Function StoreToolVersionInfo(ByVal strProgLoc As String) As Boolean

        Dim strToolVersionInfo As String = String.Empty
        Dim blnSuccess As Boolean

        If m_DebugLevel >= 2 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info")
        End If

        Dim fiProgram = New FileInfo(strProgLoc)
        If Not fiProgram.Exists Then
            Try
                strToolVersionInfo = "Unknown"
                Return MyBase.SetStepTaskToolVersion(strToolVersionInfo, New List(Of FileInfo), blnSaveToolVersionTextFile:=False)
            Catch ex As Exception
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion: " & ex.Message)
                Return False
            End Try

        End If

        ' Lookup the version of the .NET application
        blnSuccess = MyBase.StoreToolVersionInfoOneFile(strToolVersionInfo, fiProgram.FullName)
        If Not blnSuccess Then Return False

        ' Store paths to key DLLs in ioToolFiles
        Dim ioToolFiles = New List(Of FileInfo)
        ioToolFiles.Add(fiProgram)

        ioToolFiles.Add(New FileInfo(Path.Combine(fiProgram.Directory.FullName, "IQGlyQ.dll")))
        ioToolFiles.Add(New FileInfo(Path.Combine(fiProgram.Directory.FullName, "IQ2_x64.dll")))
        ioToolFiles.Add(New FileInfo(Path.Combine(fiProgram.Directory.FullName, "Run64.dll")))

        Try
            Return MyBase.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles, blnSaveToolVersionTextFile:=False)
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
    Private Sub CmdRunner_LoopWaiting()

        Static dtLastStatusUpdate As DateTime = DateTime.UtcNow

        ' Synchronize the stored Debug level with the value stored in the database
        Const MGR_SETTINGS_UPDATE_INTERVAL_SECONDS As Integer = 300
        MyBase.GetCurrentMgrSettingsFromDB(MGR_SETTINGS_UPDATE_INTERVAL_SECONDS)

        'Update the status file (limit the updates to every 5 seconds)
        If DateTime.UtcNow.Subtract(dtLastStatusUpdate).TotalSeconds >= 5 Then
            dtLastStatusUpdate = DateTime.UtcNow
            UpdateStatusRunning(m_progress)
        End If
    End Sub

#End Region

End Class
