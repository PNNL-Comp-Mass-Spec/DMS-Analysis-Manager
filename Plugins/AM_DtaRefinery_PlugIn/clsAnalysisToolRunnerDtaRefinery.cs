Option Strict On

'*********************************************************************************************************
' Written by Matt Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2009, Battelle Memorial Institute
'
'*********************************************************************************************************

Imports System.IO
Imports System.Collections.Generic
Imports System.Threading
Imports AnalysisManagerBase
Imports PRISM.Processes

Public Class clsAnalysisToolRunnerDtaRefinery
    Inherits clsAnalysisToolRunnerBase

    '*********************************************************************************************************
    'Class for running DTA_Refinery analysis
    '*********************************************************************************************************

#Region "Module Variables"
    Private Const PROGRESS_PCT_DTA_REFINERY_RUNNING As Single = 5

    Private mXTandemHasFinished As Boolean

    Private mCmdRunner As clsRunDosProgram

    '--------------------------------------------------------------------------------------------
    'Future section to monitor DTA_Refinery log file for progress determination
    '--------------------------------------------------------------------------------------------
    'Dim WithEvents m_StatFileWatch As FileSystemWatcher
    'Private m_XtSetupFile As String = "default_input.xml"
    '--------------------------------------------------------------------------------------------
    'End future section
    '--------------------------------------------------------------------------------------------
#End Region

#Region "Methods"
    ''' <summary>
    ''' Runs DTA_Refinery tool
    ''' </summary>
    ''' <returns>CloseOutType enum indicating success or failure</returns>
    ''' <remarks></remarks>
    Public Overrides Function RunTool() As CloseOutType

        Dim result As CloseOutType
        Dim OrgDBName As String = m_jobParams.GetParam("PeptideSearch", "generatedFastaName")
        Dim LocalOrgDBFolder As String = m_mgrParams.GetParam("orgdbdir")

        'Do the base class stuff
        If Not MyBase.RunTool = CloseOutType.CLOSEOUT_SUCCESS Then
            Return CloseOutType.CLOSEOUT_FAILED
        End If

        If m_DebugLevel > 4 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerDtaRefinery.RunTool(): Enter")
        End If

        ' Store the DTARefinery and X!Tandem version info in the database
        If Not StoreToolVersionInfo() Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Aborting since StoreToolVersionInfo returned false")
            m_message = "Error determining DTA Refinery version"
            Return CloseOutType.CLOSEOUT_FAILED
        End If

        ' Make sure the _DTA.txt file is valid
        If Not ValidateCDTAFile() Then
            Return CloseOutType.CLOSEOUT_NO_DTA_FILES
        End If

        If m_DebugLevel >= 2 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Running DTA_Refinery")
        End If

        mCmdRunner = New clsRunDosProgram(m_WorkDir)
        With mCmdRunner
            .CreateNoWindow = False
            .EchoOutputToConsole = False
            .CacheStandardOutput = False
            .WriteConsoleOutputToFile = False
        End With
        RegisterEvents(mCmdRunner)
        AddHandler mCmdRunner.LoopWaiting, AddressOf CmdRunner_LoopWaiting

        ' verify that program file exists
        ' DTARefineryLoc will be something like this: "c:\dms_programs\DTARefinery\dta_refinery.exe"
        Dim progLoc As String = m_mgrParams.GetParam("DTARefineryLoc")
        If Not File.Exists(progLoc) Then
            If progLoc.Length = 0 Then progLoc = "Parameter 'DTARefineryLoc' not defined for this manager"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Cannot find DTA_Refinery program file: " & progLoc)
            Return CloseOutType.CLOSEOUT_FAILED
        End If

        Dim CmdStr As String
        CmdStr = Path.Combine(m_WorkDir, m_jobParams.GetParam("DTARefineryXMLFile"))
        CmdStr &= " " & Path.Combine(m_WorkDir, m_Dataset & "_dta.txt")
        CmdStr &= " " & Path.Combine(LocalOrgDBFolder, OrgDBName)

        ' Create a batch file to run the command
        ' Capture the console output (including output to the error stream) via redirection symbols: 
        '    strExePath CmdStr > ConsoleOutputFile.txt 2>&1

        Dim strBatchFilePath As String = Path.Combine(m_WorkDir, "Run_DTARefinery.bat")
        Dim strConsoleOutputFileName = "DTARefinery_Console_Output.txt"
        m_jobParams.AddResultFileToSkip(Path.GetFileName(strBatchFilePath))

        Dim strBatchFileCmdLine As String = progLoc & " " & CmdStr & " > " & strConsoleOutputFileName & " 2>&1"

        ' Create the batch file
        Using swBatchFile = New StreamWriter(New FileStream(strBatchFilePath, FileMode.Create, FileAccess.Write, FileShare.Read))

            If m_DebugLevel >= 1 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, strBatchFileCmdLine)
            End If

            swBatchFile.WriteLine(strBatchFileCmdLine)
        End Using

        Thread.Sleep(100)

        m_progress = PROGRESS_PCT_DTA_REFINERY_RUNNING
        ResetProgRunnerCpuUsage()
        mXTandemHasFinished = False

        ' Start the program and wait for it to finish
        ' However, while it's running, LoopWaiting will get called via events
        Dim success = mCmdRunner.RunProgram(strBatchFilePath, String.Empty, "DTARefinery", True)

        If Not success Then

            Thread.Sleep(500)

            ' Open DTARefinery_Console_Output.txt and look for the last line with the text "error"
            Dim fiConsoleOutputFile = New FileInfo(Path.Combine(m_WorkDir, strConsoleOutputFileName))
            Dim consoleOutputErrorMessage = String.Empty

            If fiConsoleOutputFile.Exists Then
                Using consoleOutputReader = New StreamReader(New FileStream(fiConsoleOutputFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                    While Not consoleOutputReader.EndOfStream
                        Dim dataLine = consoleOutputReader.ReadLine()
                        If String.IsNullOrWhiteSpace(dataLine) Then
                            Continue While
                        End If

                        If dataLine.IndexOf("error", StringComparison.InvariantCultureIgnoreCase) >= 0 Then
                            consoleOutputErrorMessage = String.Copy(dataLine)
                        End If
                    End While
                End Using

            End If

            m_message = "Error running DTARefinery"
            If Not String.IsNullOrWhiteSpace(consoleOutputErrorMessage) Then
                m_message &= ": " & consoleOutputErrorMessage
            End If

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)

            ValidateDTARefineryLogFile()

            ' Move the source files and any results to the Failed Job folder
            ' Useful for debugging DTA_Refinery problems
            CopyFailedResultsToArchiveFolder()

            Return CloseOutType.CLOSEOUT_FAILED
        End If

        'Stop the job timer
        m_StopTime = DateTime.UtcNow

        'Add the current job data to the summary file
        If Not UpdateSummaryFile() Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN, "Error creating summary file, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step"))
        End If

        'Make sure objects are released
        Thread.Sleep(500)         ' 1 second delay
        clsProgRunner.GarbageCollectNow()

        If Not ValidateDTARefineryLogFile() Then
            result = CloseOutType.CLOSEOUT_NO_DATA
        Else
            Dim blnPostResultsToDB = True
            Dim oMassErrorExtractor = New clsDtaRefLogMassErrorExtractor(m_mgrParams, m_WorkDir, m_DebugLevel, blnPostResultsToDB)
            Dim blnSuccess As Boolean

            Dim intDatasetID As Integer = m_jobParams.GetJobParameter("DatasetID", 0)
            Dim intJob As Integer
            Integer.TryParse(m_JobNum, intJob)

            blnSuccess = oMassErrorExtractor.ParseDTARefineryLogFile(m_Dataset, intDatasetID, intJob)

            If Not blnSuccess Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error parsing DTA refinery log file to extract mass error stats, job " & m_JobNum)
            End If

            'Zip the output file
            result = ZipMainOutputFile()
        End If

        If result <> CloseOutType.CLOSEOUT_SUCCESS Then
            ' Move the source files and any results to the Failed Job folder
            ' Useful for debugging DTA_Refinery problems
            CopyFailedResultsToArchiveFolder()
            Return result
        End If

        result = MakeResultsFolder()
        If result <> CloseOutType.CLOSEOUT_SUCCESS Then
            'TODO: What do we do here?
            Return result
        End If

        result = MoveResultFiles()
        If result <> CloseOutType.CLOSEOUT_SUCCESS Then
            'TODO: What do we do here?
            ' Note that MoveResultFiles should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
            Return result
        End If

        result = CopyResultsFolderToServer()
        If result <> CloseOutType.CLOSEOUT_SUCCESS Then
            'TODO: What do we do here?
            ' Note that MoveResultFiles should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
            Return result
        End If

        Return CloseOutType.CLOSEOUT_SUCCESS 'ZipResult

    End Function

    Private Sub CopyFailedResultsToArchiveFolder()

        Dim result As CloseOutType

        Dim strFailedResultsFolderPath As String = m_mgrParams.GetParam("FailedResultsFolderPath")
        If String.IsNullOrEmpty(strFailedResultsFolderPath) Then strFailedResultsFolderPath = "??Not Defined??"

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
        If result = CloseOutType.CLOSEOUT_SUCCESS Then
            ' Move the result files into the result folder
            result = MoveResultFiles()
            If result = CloseOutType.CLOSEOUT_SUCCESS Then
                ' Move was a success; update strFolderPathToArchive
                strFolderPathToArchive = Path.Combine(m_WorkDir, m_ResFolderName)
            End If
        End If

        ' Copy the results folder to the Archive folder
        Dim objAnalysisResults = New clsAnalysisResults(m_mgrParams, m_jobParams)
        objAnalysisResults.CopyFailedResultsToArchiveFolder(strFolderPathToArchive)


    End Sub

    ''' <summary>
    ''' Parses the _DTARefineryLog.txt file to check for a message regarding X!Tandem being finished
    ''' </summary>
    ''' <returns>True if finished, false if not</returns>
    ''' <remarks></remarks>
    Private Function IsXTandemFinished() As Boolean

        Try

            Dim fiSourceFile = New FileInfo(Path.Combine(m_WorkDir, m_Dataset & "_dta_DtaRefineryLog.txt"))
            If Not fiSourceFile.Exists Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "DTA_Refinery log file not found by IsXtandenFinished: " & fiSourceFile.Name)
                Return False
            End If

            Dim tmpFilePath = fiSourceFile.FullName & ".tmp"
            fiSourceFile.CopyTo(tmpFilePath, True)
            m_jobParams.AddResultFileToSkip(tmpFilePath)
            Thread.Sleep(100)

            Using srSourceFile = New StreamReader(New FileStream(tmpFilePath, FileMode.Open, FileAccess.Read, FileShare.Read))

                Do While Not srSourceFile.EndOfStream
                    Dim strLineIn = srSourceFile.ReadLine()

                    If strLineIn.Contains("finished x!tandem") Then
                        Return True
                    End If

                Loop

            End Using

            Return False

        Catch ex As Exception
            m_message = "Exception in IsXTandemFinished"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message)
            Return False
        End Try

    End Function

    ''' <summary>
    ''' Stores the tool version info in the database
    ''' </summary>
    ''' <remarks></remarks>
    Private Function StoreToolVersionInfo() As Boolean

        Dim strToolVersionInfo As String = String.Empty

        If m_DebugLevel >= 2 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info")
        End If

        ' Store paths to key files in ioToolFiles
        Dim ioToolFiles As New List(Of FileInfo)
        Dim ioDtaRefineryFileInfo As New FileInfo(m_mgrParams.GetParam("DTARefineryLoc"))

        If ioDtaRefineryFileInfo.Exists Then
            ioToolFiles.Add(ioDtaRefineryFileInfo)

            Dim strXTandemModuleLoc As String = Path.Combine(ioDtaRefineryFileInfo.DirectoryName, "aux_xtandem_module\tandem_5digit_precision.exe")
            ioToolFiles.Add(New FileInfo(strXTandemModuleLoc))
        Else
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "DTARefinery not found: " & ioDtaRefineryFileInfo.FullName)
            Return False
        End If

        Try
            Return MyBase.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles, blnSaveToolVersionTextFile:=False)
        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion: " & ex.Message)
            Return False
        End Try

    End Function

    ''' <summary>
    ''' Parses the _DTARefineryLog.txt file to look for errors
    ''' </summary>
    ''' <returns>True if no errors, false if a problem</returns>
    ''' <remarks></remarks>
    Private Function ValidateDTARefineryLogFile() As Boolean

        Try

            Dim fiSourceFile = New FileInfo(Path.Combine(m_WorkDir, m_Dataset & "_dta_DtaRefineryLog.txt"))
            If Not fiSourceFile.Exists Then
                m_message = "DtaRefinery Log file not found"
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & " (" & fiSourceFile.Name & ")")
                Return False
            End If

            Using srSourceFile = New StreamReader(New FileStream(fiSourceFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))

                Do While Not srSourceFile.EndOfStream
                    Dim strLineIn = srSourceFile.ReadLine()

                    If strLineIn.StartsWith("number of spectra identified less than 2") Then
                        If Not srSourceFile.EndOfStream Then
                            strLineIn = srSourceFile.ReadLine()
                            If strLineIn.StartsWith("stop processing") Then
                                m_message = "X!Tandem identified fewer than 2 peptides; unable to use DTARefinery with this dataset"
                                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
                                Return False
                            End If
                        End If

                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Encountered message 'number of spectra identified less than 2' but did not find 'stop processing' on the next line; DTARefinery likely did not complete properly")
                    End If

                Loop

            End Using


        Catch ex As Exception
            m_message = "Exception in ValidateDTARefineryLogFile"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message)
            Return False
        End Try

        Return True

    End Function
    
    ''' <summary>
    ''' Zips concatenated XML output file
    ''' </summary>
    ''' <returns>CloseOutType enum indicating success or failure</returns>
    ''' <remarks></remarks>
    Private Function ZipMainOutputFile() As CloseOutType

        Dim ioWorkDirectory As DirectoryInfo
        Dim ioFiles() As FileInfo
        Dim ioFile As FileInfo
        Dim strFixedDTAFilePath As String

        'Do we want to zip these output files?  Yes, we keep them all
        '* _dta_DtaRefineryLog.txt 
        '* _dta_SETTINGS.xml
        '* _FIXED_dta.txt
        '* _HIST.png
        '* _HIST.txt
        ' * scan number: _scanNum.png
        ' * m/z: _mz.png
        ' * log10 of ion intensity in the ICR/Orbitrap cell: _logTrappedIonInt.png
        ' * total ion current in the ICR/Orbitrap cell: _trappedIonsTIC.png


        'Delete the original DTA files
        Try
            ioWorkDirectory = New DirectoryInfo(m_WorkDir)
            ioFiles = ioWorkDirectory.GetFiles("*_dta.*")

            For Each ioFile In ioFiles
                If Not ioFile.Name.ToUpper.EndsWith("_FIXED_dta.txt".ToUpper) Then
                    ioFile.Attributes = ioFile.Attributes And (Not FileAttributes.ReadOnly)
                    ioFile.Delete()
                End If
            Next

        Catch Err As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerDtaRefinery.ZipMainOutputFile, Error deleting _om.omx file, job " & m_JobNum & Err.Message)
            Return CloseOutType.CLOSEOUT_FAILED
        End Try

        Try
            strFixedDTAFilePath = Path.Combine(m_WorkDir, m_Dataset & "_FIXED_dta.txt")
            ioFile = New FileInfo(strFixedDTAFilePath)

            If Not ioFile.Exists Then
                Dim Msg = "DTARefinery output file not found"
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg & ": " & ioFile.Name)
                m_message = clsGlobal.AppendToComment(m_message, Msg)
                Return CloseOutType.CLOSEOUT_FAILED
            End If

            ioFile.MoveTo(Path.Combine(m_WorkDir, m_Dataset & "_dta.txt"))

            Try
                If Not MyBase.ZipFile(ioFile.FullName, True) Then
                    Dim Msg = "Error zipping DTARefinery output file"
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg & ": " & ioFile.FullName)
                    m_message = clsGlobal.AppendToComment(m_message, Msg)
                    Return CloseOutType.CLOSEOUT_FAILED
                End If

            Catch ex As Exception
                Dim Msg As String = "clsAnalysisToolRunnerDtaRefinery.ZipMainOutputFile, Error zipping DTARefinery output file: " & ex.Message
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
                m_message = clsGlobal.AppendToComment(m_message, "Error zipping DTARefinery output file")
                Return CloseOutType.CLOSEOUT_FAILED
            End Try

        Catch ex As Exception
            Dim Msg As String = "clsAnalysisToolRunnerDtaRefinery.ZipMainOutputFile, Error renaming DTARefinery output file: " & ex.Message
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
            m_message = clsGlobal.AppendToComment(m_message, "Error renaming DTARefinery output file")
            Return CloseOutType.CLOSEOUT_FAILED
        End Try

        Return CloseOutType.CLOSEOUT_SUCCESS

    End Function

    ''' <summary>
    ''' Event handler for CmdRunner.LoopWaiting event
    ''' </summary>
    ''' <remarks></remarks>
    Private Sub CmdRunner_LoopWaiting()

        Const DTA_REFINERY_PROCESS_NAME = "dta_refinery"
        Const XTANDEM_PROCESS_NAME = "tandem_5digit_precision"

        Const SECONDS_BETWEEN_UPDATE = 30
        Static dtLastCpuUsageCheck As DateTime = DateTime.UtcNow

        UpdateStatusFile()

        ' Push a new core usage value into the queue every 30 seconds
        If DateTime.UtcNow.Subtract(dtLastCpuUsageCheck).TotalSeconds >= SECONDS_BETWEEN_UPDATE Then
            dtLastCpuUsageCheck = DateTime.UtcNow

            If Not mXTandemHasFinished Then
                mXTandemHasFinished = IsXTandemFinished()
            End If

            If mXTandemHasFinished Then
                ' Determine the CPU usage of DTA_Refinery
                UpdateCpuUsageByProcessName(DTA_REFINERY_PROCESS_NAME, SECONDS_BETWEEN_UPDATE, mCmdRunner.ProcessID)
            Else
                ' Determine the CPU usage of X!Tandem
                UpdateCpuUsageByProcessName(XTANDEM_PROCESS_NAME, SECONDS_BETWEEN_UPDATE, mCmdRunner.ProcessID)
            End If

            LogProgress("DtaRefinery")
        End If

    End Sub

    '--------------------------------------------------------------------------------------------
    'Future section to monitor log file for progress determination
    '--------------------------------------------------------------------------------------------
    '	Private Sub StartFileWatcher(DirToWatch As String, FileToWatch As String)

    ''Watches the DTA_Refinery status file and reports changes

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
