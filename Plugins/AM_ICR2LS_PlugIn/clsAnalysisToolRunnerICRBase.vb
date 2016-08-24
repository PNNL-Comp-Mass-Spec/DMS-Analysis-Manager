Option Strict On

Imports System.Collections.Generic
Imports System.IO
Imports System.Text.RegularExpressions

Public MustInherit Class clsAnalysisToolRunnerICRBase
	Inherits clsAnalysisToolRunnerBase

    Public Const ICR2LS_STATE_UNKNOWN As String = "Unknown"
    Public Const ICR2LS_STATE_IDLE As String = "Idle"
    Public Const ICR2LS_STATE_PROCESSING As String = "Processing"
    Public Const ICR2LS_STATE_KILLED As String = "Killed"
    Public Const ICR2LS_STATE_ERROR As String = "Error"
    Public Const ICR2LS_STATE_FINISHED As String = "Finished"
    Public Const ICR2LS_STATE_GENERATING As String = "Generating"
    Public Const ICR2LS_STATE_TICGENERATION As String = "TICGeneration"
    Public Const ICR2LS_STATE_LCQTICGENERATION As String = "LCQTICGeneration"
    Public Const ICR2LS_STATE_QTOFPEKGENERATION As String = "QTOFPEKGeneration"
    Public Const ICR2LS_STATE_MMTOFPEKGENERATION As String = "MMTOFPEKGeneration"
    Public Const ICR2LS_STATE_LTQFTPEKGENERATION As String = "LTQFTPEKGeneration"

    Public Const PEK_TEMP_FILE As String = ".pek.tmp"

    Private Const APEX_ACQUISITION_METHOD_FILE As String = "apexAcquisition.method"

    Public Enum ICR2LSProcessingModeConstants
        LTQFTPEK = 0
        LTQFTTIC = 1
        SFoldersPEK = 2
        SFoldersTIC = 3
        SerFolderPEK = 4
        SerFolderTIC = 5
        SerFilePEK = 6
        SerFileTIC = 7
    End Enum

    Private Structure udtICR2LSStatusType
        Public StatusDate As DateTime
        Public ScansProcessed As Integer
        Public PercentComplete As Single
        Public ProcessingState As String            ' Typical values: Processing, Finished, etc.
        Public ProcessingStatus As String             ' Typical values: LTQFTPEKGENERATION, GENERATING
        Public ErrorMessage As String

        Public Sub Initialize()
            StatusDate = DateTime.Now
            ScansProcessed = 0
            PercentComplete = 0
            ProcessingState = ICR2LS_STATE_UNKNOWN
            ProcessingStatus = String.Empty
            ErrorMessage = String.Empty
        End Sub
    End Structure

    'Job running status variable
    Private m_JobRunning As Boolean
    Private mStatusFilePath As String = String.Empty

    ' Obsolete
    ' Private mMinScanOffset As Integer = 0

    Private mLastErrorPostingTime As DateTime
    Private mLastMissingStatusFiletime As DateTime
    Private mLastInvalidStatusFiletime As DateTime

    Private mLastStatusParseTime As DateTime = DateTime.UtcNow
    Private mLastStatusLogTime As DateTime = DateTime.UtcNow

    Private mPEKResultsFile As FileInfo
    Private mLastCheckpointTime As DateTime = DateTime.UtcNow

    Private mICR2LSStatus As udtICR2LSStatusType

    Private WithEvents mCmdRunner As clsRunDosProgram
    Private WithEvents mStatusFileWatcher As FileSystemWatcher

    Private WithEvents mPEKtoCSVConverter As PEKtoCSVConverter.PEKtoCSVConverter
    Private mLastPekToCsvPercentCompleteTime As DateTime

    Public Sub New()

        ResetStatusLogTimes()

        mICR2LSStatus.Initialize()
    End Sub

    Public Overrides Function RunTool() As IJobParams.CloseOutType

        ' Get the settings file info via the base class
        If Not MyBase.RunTool() = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then Return IJobParams.CloseOutType.CLOSEOUT_FAILED

        'Start the job timer
        m_StartTime = DateTime.UtcNow

        ResetStatusLogTimes()
        mICR2LSStatus.Initialize()

        ' Remainder of tasks are in subclass (which should call this using MyBase.Runtool)
        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    Private Function ConvertPekToCsv(pekFilePath As String) As Boolean
        Try

            Dim scansFilePath As String = Path.Combine(m_WorkDir, m_Dataset & "_scans.csv")
            Dim isosFilePath As String = Path.Combine(m_WorkDir, m_Dataset & "_isos.csv")
            Dim rawFilePath As String = Path.Combine(m_WorkDir, m_Dataset & clsAnalysisResources.DOT_RAW_EXTENSION)

            If Not File.Exists(rawFilePath) Then
                rawFilePath = String.Empty
            End If

            mPEKtoCSVConverter = New PEKtoCSVConverter.PEKtoCSVConverter(pekFilePath, scansFilePath, isosFilePath, rawFilePath)

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Creating _isos.csv and _scans.csv files using the PEK file")
            mLastPekToCsvPercentCompleteTime = DateTime.UtcNow

            Dim success = mPEKtoCSVConverter.Convert()

            mPEKtoCSVConverter = Nothing
            PRISM.Processes.clsProgRunner.GarbageCollectNow()

            Return success

        Catch ex As Exception
            m_message = "Error converting the PEK file to DeconTools-compatible _isos.csv"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message)
            Return False
        End Try

    End Function

    Private Sub CopyCheckpointFile()

        Try
            If mPEKResultsFile Is Nothing Then Exit Sub

            If Not mPEKResultsFile.Exists Then
                mPEKResultsFile.Refresh()
                If Not mPEKResultsFile.Exists Then Exit Sub
            End If

            Dim transferFolderPath = m_jobParams.GetParam("JobParameters", "transferFolderPath")
            If String.IsNullOrEmpty(transferFolderPath) Then Exit Sub

            transferFolderPath = Path.Combine(transferFolderPath, m_jobParams.GetParam("JobParameters", "DatasetFolderName"))
            transferFolderPath = Path.Combine(transferFolderPath, m_jobParams.GetParam("StepParameters", "OutputFolderName"))

            Dim diTransferFolder = New DirectoryInfo(transferFolderPath)
            If Not diTransferFolder.Exists Then
                diTransferFolder.Create()
            End If

            Dim fiTargetFileFinal = New FileInfo(Path.Combine(diTransferFolder.FullName, Path.GetFileNameWithoutExtension(mPEKResultsFile.Name) & PEK_TEMP_FILE))
            Dim fiTargetFileTemp = New FileInfo(fiTargetFileFinal.FullName & ".new")

            mPEKResultsFile.CopyTo(fiTargetFileTemp.FullName, True)

            Threading.Thread.Sleep(500)
            fiTargetFileTemp.Refresh()

            If fiTargetFileFinal.Exists Then fiTargetFileFinal.Delete()

            fiTargetFileTemp.MoveTo(fiTargetFileFinal.FullName)

            m_jobParams.AddServerFileToDelete(fiTargetFileFinal.FullName)

        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception copying the interim .PEK file to the transfer folder: " & ex.Message)
        End Try

    End Sub

    Protected MustOverride Function DeleteDataFile() As IJobParams.CloseOutType

    Private Function GetLastScanInPEKFile(pekTempFilePath As String) As Integer

        Dim currentScan = 0
        Dim lastValidScan = 0
        Dim reScanNumber = New Regex("Scan = (\d+)", RegexOptions.Compiled Or RegexOptions.IgnoreCase)
        Dim reScanNumberFromFilename = New Regex("Filename: .+ Scan.(\d+)", RegexOptions.Compiled Or RegexOptions.IgnoreCase)

        Dim lstClosingMessages = New List(Of String)
        lstClosingMessages.Add("Number of isotopic distributions detected")
        lstClosingMessages.Add("Processing stop time")
        lstClosingMessages.Add("Number of peaks in spectrum")

        Try
            Using srPekFile = New StreamReader(New FileStream(pekTempFilePath, FileMode.Open, FileAccess.Read, FileShare.Read))
                While Not srPekFile.EndOfStream
                    Dim dataLine = srPekFile.ReadLine
                    If String.IsNullOrWhiteSpace(dataLine) Then Continue While

                    Dim reMatch = reScanNumber.Match(dataLine)
                    If Not reMatch.Success Then
                        reMatch = reScanNumberFromFilename.Match(dataLine)
                    End If

                    If reMatch.Success Then
                        Integer.TryParse(reMatch.Groups(1).Value, currentScan)
                    End If

                    For Each closingMessage In lstClosingMessages
                        If dataLine.StartsWith(closingMessage) Then
                            lastValidScan = currentScan
                        End If
                    Next

                End While

            End Using

            Return lastValidScan

        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception in GetLastScanInPEKFile: " & ex.Message)
            Return 0
        End Try

    End Function

    ' Reads the ICR2LS Status file and updates mICR2LSStatus
    Private Function ParseICR2LSStatusFile(strStatusFilePath As String, blnForceParse As Boolean) As Boolean
        Const MINIMUM_PARSING_INTERVAL_SECONDS = 4

        Dim srInFile As StreamReader
        Dim strLineIn As String
        Dim intCharIndex As Integer
        Dim strKey As String
        Dim strValue As String

        Dim intResult As Integer
        Dim sngResult As Single
        Dim strProcessingState As String = mICR2LSStatus.ProcessingState
        Dim strProcessingStatus = String.Empty
        Dim intScansProcessed As Integer

        Dim strStatusDate = String.Empty
        Dim strStatusTime = String.Empty

        Dim blnSuccess As Boolean

        Try
            blnSuccess = False

            If strStatusFilePath Is Nothing OrElse strStatusFilePath.Length = 0 Then
                Exit Try
            End If

            If Not blnForceParse AndAlso DateTime.UtcNow.Subtract(mLastStatusParseTime).TotalSeconds < MINIMUM_PARSING_INTERVAL_SECONDS Then
                ' Not enough time has elapsed, exit the procedure (returning True)
                blnSuccess = True
                Exit Try
            End If

            mLastStatusParseTime = DateTime.UtcNow

            If File.Exists(strStatusFilePath) Then
                ' Read the file
                srInFile = New StreamReader(New FileStream(strStatusFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))

                Do While srInFile.Peek() >= 0
                    strLineIn = srInFile.ReadLine()

                    intCharIndex = strLineIn.IndexOf("="c)
                    If intCharIndex > 0 Then
                        strKey = strLineIn.Substring(0, intCharIndex).Trim
                        strValue = strLineIn.Substring(intCharIndex + 1).Trim


                        Select Case strKey.ToLower
                            Case "date"
                                strStatusDate = String.Copy(strValue)
                            Case "time"
                                strStatusTime = String.Copy(strValue)
                            Case "scansprocessed"
                                If Integer.TryParse(strValue, intResult) Then
                                    ' Old: The ScansProcessed value reported by ICR-2LS is actually the scan number of the most recently processed scan
                                    ' If we use /F to start with a scan other than 1, then this ScansProcessed value does not reflect reality
                                    ' To correct for this, subtract out mMinScanOffset
                                    ' intScansProcessed = intResult - mMinScanOffset

                                    ' New: ScansProcessed is truly the number of scans processed
                                    intScansProcessed = intResult
                                    If intScansProcessed < 0 Then
                                        intScansProcessed = intResult
                                    End If
                                End If

                            Case "percentcomplete"
                                If Single.TryParse(strValue, sngResult) Then
                                    mICR2LSStatus.PercentComplete = sngResult
                                End If

                            Case "state"
                                ' Example values: Processing, Finished
                                strProcessingState = String.Copy(strValue)

                            Case "status"
                                ' Example value: LTQFTPEKGENERATION
                                strProcessingStatus = String.Copy(strValue)

                            Case "errormessage"
                                mICR2LSStatus.ErrorMessage = String.Copy(strValue)

                            Case Else
                                ' Ignore the line
                        End Select
                    End If
                Loop

                srInFile.Close()

                If strStatusDate.Length > 0 AndAlso strStatusTime.Length > 0 Then
                    strStatusDate &= " " & strStatusTime
                    If Not DateTime.TryParse(strStatusDate, mICR2LSStatus.StatusDate) Then
                        mICR2LSStatus.StatusDate = DateTime.Now()
                    End If
                End If

                If intScansProcessed > mICR2LSStatus.ScansProcessed Then
                    ' Only update .ScansProcessed if the new value is larger than the previous one
                    ' This is necessary since ICR-2LS will set ScansProcessed to 0 when the state is Finished
                    mICR2LSStatus.ScansProcessed = intScansProcessed
                End If

                If Not strProcessingState Is Nothing AndAlso strProcessingState.Length > 0 Then
                    mICR2LSStatus.ProcessingState = strProcessingState

                    If Not ValidateICR2LSStatus(strProcessingState) Then
                        If DateTime.UtcNow.Subtract(mLastInvalidStatusFiletime).TotalMinutes >= 15 Then
                            mLastInvalidStatusFiletime = DateTime.UtcNow
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Invalid processing state reported by ICR2LS: " & strProcessingState)
                        End If
                    End If
                End If

                If Not strProcessingStatus Is Nothing AndAlso strProcessingStatus.Length > 0 Then
                    mICR2LSStatus.ProcessingStatus = strProcessingStatus
                End If

                m_progress = mICR2LSStatus.PercentComplete

                ' Update the local status file (and post the status to the message queue)
                UpdateStatusRunning(m_progress, mICR2LSStatus.ScansProcessed)

                blnSuccess = True
            Else
                ' Status.log file not found; if the job just started, this will be the case
                ' For this reason, ResetStatusLogTimes will set mLastMissingStatusFiletime to the time the job starts, meaning
                '  we won't log an error about a missing Status.log file until 60 minutes into a job
                If DateTime.UtcNow.Subtract(mLastMissingStatusFiletime).TotalMinutes >= 60 Then
                    mLastMissingStatusFiletime = DateTime.UtcNow
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "ICR2LS Status.Log file not found: " & strStatusFilePath)
                End If

                blnSuccess = True
            End If
        Catch ex As Exception
            ' Limit logging of errors to once every 60 minutes

            If DateTime.UtcNow.Subtract(mLastErrorPostingTime).TotalMinutes >= 60 Then
                mLastErrorPostingTime = DateTime.UtcNow
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Error reading the ICR2LS Status.Log file (" & strStatusFilePath & "): " & ex.Message)
            End If
        End Try

        Return blnSuccess

    End Function

    Private Sub InitializeStatusLogFileWatcher(strWorkDir As String, strFilenameToWatch As String)

        mStatusFileWatcher = New FileSystemWatcher()
        With mStatusFileWatcher
            .BeginInit()
            .Path = strWorkDir
            .IncludeSubdirectories = False
            .Filter = Path.GetFileName(strFilenameToWatch)
            .NotifyFilter = NotifyFilters.LastWrite Or NotifyFilters.Size
            .EndInit()
            .EnableRaisingEvents = True
        End With
    End Sub

    Protected Overridable Function PerfPostAnalysisTasks(blnCopyResultsToServer As Boolean) As IJobParams.CloseOutType

        Dim result As IJobParams.CloseOutType

        'Stop the job timer
        m_StopTime = DateTime.UtcNow

        If Not UpdateSummaryFile() Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN, "Error creating summary file, job " & m_JobNum)
        End If

        Dim pekConversionSuccess As Boolean

        ' Use the PEK file to create DeconTools compatible _isos.csv and _scans.csv files
        ' Create this CSV file even if ICR-2LS did not successfully finish
        Dim pekFilePath = Path.Combine(m_WorkDir, m_Dataset & ".pek")
        pekConversionSuccess = ConvertPekToCsv(pekFilePath)

        ' Get rid of raw data file
        result = DeleteDataFile()
        If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            ' Error deleting raw files; the error will have already been logged
            ' Since the results might still be good, we will not return an error at this point
        End If

        m_jobParams.AddResultFileToSkip("Status.log")

        ' Make results folder
        result = MakeResultsFolder()
        If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            Return result
        End If

        result = MoveResultFiles()
        If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            Return result
        End If

        If Not blnCopyResultsToServer Then
            Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
        End If

        result = CopyResultsFolderToServer()
        If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            Return result
        End If

        If pekConversionSuccess Then
            ' We can now safely delete the .pek.tmp file from the server
            MyBase.RemoveNonResultServerFiles()

            Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
        End If

        If String.IsNullOrEmpty(m_message) Then
            m_message = "Unknown error converting the .PEK file to a DeconTools-compatible _isos.csv file"
        End If
        Return IJobParams.CloseOutType.CLOSEOUT_FAILED

    End Function

    Private Sub ResetStatusLogTimes()
        ' Initialize the last error posting time to 2 hours before the present
        mLastErrorPostingTime = DateTime.UtcNow.Subtract(New TimeSpan(2, 0, 0))

        ' Initialize the last MissingStatusFileTime to the time the job starts
        mLastMissingStatusFiletime = DateTime.UtcNow

        mLastInvalidStatusFiletime = DateTime.UtcNow.Subtract(New TimeSpan(2, 0, 0))
    End Sub

    ''' <summary>
    ''' Starts ICR-2LS by running the .Exe at the command line
    ''' </summary>
    ''' <param name="DSNamePath"></param>
    ''' <param name="ParamFilePath"></param>
    ''' <param name="ResultsFileNamePath"></param>
    ''' <param name="eICR2LSMode"></param>
    ''' <returns>True if successfully started; otherwise false</returns>
    ''' <remarks></remarks>
    Protected Function StartICR2LS(DSNamePath As String,
                                   ParamFilePath As String,
                                   ResultsFileNamePath As String,
                                   eICR2LSMode As ICR2LSProcessingModeConstants) As Boolean
        Return StartICR2LS(DSNamePath, ParamFilePath, ResultsFileNamePath, eICR2LSMode, True, False, 0, 0)
    End Function

    ''' <summary>
    ''' Run ICR-2LS on the file (or 0.ser folder) specified by DSNamePath
    ''' </summary>
    ''' <param name="instrumentFilePath"></param>
    ''' <param name="paramFilePath"></param>
    ''' <param name="resultsFileNamePath"></param>
    ''' <param name="eICR2LSMode"></param>
    ''' <param name="useAllScans"></param>
    ''' <param name="minScan"></param>
    ''' <param name="maxScan"></param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Protected Function StartICR2LS(
      instrumentFilePath As String,
      paramFilePath As String,
      resultsFileNamePath As String,
      eICR2LSMode As ICR2LSProcessingModeConstants,
      useAllScans As Boolean,
      skipMS2 As Boolean,
      minScan As Integer,
      maxScan As Integer) As Boolean

        Const MONITOR_INTERVAL_SECONDS = 4

        Dim strExeFilePath As String
        Dim strArguments As String
        Dim strApexAcqFilePath As String

        Dim blnSuccess As Boolean
        Dim eLogLevel As clsLogTools.LogLevels

        mStatusFilePath = Path.Combine(Path.GetDirectoryName(resultsFileNamePath), "Status.log")

        ' Create a file watcher to monitor the status.log file created by ICR-2LS
        ' This file is updated after each scan is processed
        InitializeStatusLogFileWatcher(Path.GetDirectoryName(mStatusFilePath), Path.GetFileName(mStatusFilePath))

        m_JobRunning = True

        strExeFilePath = m_mgrParams.GetParam("ICR2LSprogloc")

        If strExeFilePath = "" Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Job parameter ICR2LSprogloc is not defined; unable to run ICR-2LS")
            Return False
        ElseIf Not File.Exists(strExeFilePath) Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "ICR-2LS path not found: " & strExeFilePath)
            Return False
        End If

        ' Look for an existing .pek.tmp file
        Dim scanToResumeAfter = 0

        mPEKResultsFile = New FileInfo(resultsFileNamePath)
        Dim pekTempFilePath = Path.Combine(mPEKResultsFile.Directory.FullName, Path.GetFileNameWithoutExtension(mPEKResultsFile.Name) & PEK_TEMP_FILE)

        Dim fiTempResultsFile = New FileInfo(pekTempFilePath)
        If fiTempResultsFile.Exists Then
            ' Open the .pek.tmp file and determine the last scan number that has "Number of isotopic distributions detected"
            scanToResumeAfter = GetLastScanInPEKFile(pekTempFilePath)

            If scanToResumeAfter > 0 Then
                useAllScans = False
                Threading.Thread.Sleep(200)
                fiTempResultsFile.MoveTo(resultsFileNamePath)
            End If
        End If

        ' Syntax for calling ICR-2LS via the command line:
        ' ICR-2LS.exe /I:InputFilePath /P:ParameterFilePath /O:OutputFilePath /M:[PEK|TIC] /T:[1|2] /F:FirstScan /L:LastScan /NoMS2
        '
        ' /M:PEK means to make a PEK file while /M:TIC means to generate the .TIC file
        ' /T:0 is likely auto-determine based on input file name
        ' /T:1 means the input file is a Thermo .Raw file, and /I specifies a file path
        ' /T:2 means the input files are s-files in s-folders (ICR-2LS file format), and thus /I specifies a folder path
        '
        ' /F and /L are optional.  They can be used to limit the range of scan numbers to process
        ' 
        ' /NoMS2 is optional.  When provided, /MS2 spectra will be skipped
        '
        ' See clsAnalysisToolRunnerICR for a description of the expected folder layout when processing S-folders 

        Select Case eICR2LSMode
            Case ICR2LSProcessingModeConstants.SerFolderPEK, ICR2LSProcessingModeConstants.SerFolderTIC
                strArguments = " /I:" & PossiblyQuotePath(instrumentFilePath) & "\acqus /P:" & PossiblyQuotePath(paramFilePath) & " /O:" & PossiblyQuotePath(resultsFileNamePath)

            Case ICR2LSProcessingModeConstants.SerFilePEK, ICR2LSProcessingModeConstants.SerFileTIC
                ' Need to find the location of the apexAcquisition.method file
                strApexAcqFilePath = clsAnalysisResources.FindFileInDirectoryTree(Path.GetDirectoryName(instrumentFilePath), APEX_ACQUISITION_METHOD_FILE)

                If String.IsNullOrEmpty(strApexAcqFilePath) Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Could not find the " & APEX_ACQUISITION_METHOD_FILE & " file in folder " & instrumentFilePath)
                    Return False
                Else
                    strArguments = " /I:" & PossiblyQuotePath(strApexAcqFilePath) & " /P:" & PossiblyQuotePath(paramFilePath) & " /O:" & PossiblyQuotePath(resultsFileNamePath)
                End If

            Case Else
                strArguments = " /I:" & PossiblyQuotePath(instrumentFilePath) & " /P:" & PossiblyQuotePath(paramFilePath) & " /O:" & PossiblyQuotePath(resultsFileNamePath)
        End Select


        Select Case eICR2LSMode
            Case ICR2LSProcessingModeConstants.LTQFTPEK
                strArguments &= " /M:PEK /T:1"
            Case ICR2LSProcessingModeConstants.LTQFTTIC
                strArguments &= " /M:TIC /T:1"
            Case ICR2LSProcessingModeConstants.SFoldersPEK
                strArguments &= " /M:PEK /T:2"
            Case ICR2LSProcessingModeConstants.SFoldersTIC
                strArguments &= " /M:TIC /T:2"
            Case ICR2LSProcessingModeConstants.SerFolderPEK, ICR2LSProcessingModeConstants.SerFilePEK
                strArguments &= " /M:PEK /T:0"
            Case ICR2LSProcessingModeConstants.SerFolderTIC, ICR2LSProcessingModeConstants.SerFileTIC
                strArguments &= " /M:TIC /T:0"
            Case Else
                ' Unknown mode
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Unknown ICR2LS processing Mode: " & eICR2LSMode.ToString)
                Return False
        End Select

        If useAllScans Then
            ' Obsolete
            ' mMinScanOffset = 0
        Else
            ' Obsolete
            ' mMinScanOffset = MinScan

            If scanToResumeAfter > 0 AndAlso minScan < scanToResumeAfter Then minScan = scanToResumeAfter + 1

            strArguments &= " /F:" & minScan.ToString & " /L:" & maxScan.ToString
        End If

        If skipMS2 Then
            strArguments &= " /NoMS2"
        End If

        ' Possibly enable preview mode (skips the actual deisotoping)
        If False And Environment.MachineName.ToLower.Contains("monroe") Then
            strArguments &= " /preview"
        End If

        ' Initialize the program runner
        mCmdRunner = New clsRunDosProgram(m_WorkDir)
        mCmdRunner.MonitorInterval = MONITOR_INTERVAL_SECONDS * 1000

        ' Set up and execute a program runner to run ICR2LS.exe
        If m_DebugLevel >= 1 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, strExeFilePath & strArguments)
        End If

        If strArguments.Length > 250 Then
            ' VB6 programs cannot parse command lines over 255 characters in length
            ' Save the arguments to a text file and then call ICR2LS using the /R switch

            Dim commandLineFilePath = Path.GetTempFileName()
            Using swCommandLineFile = New StreamWriter(New FileStream(commandLineFilePath, FileMode.Create, FileAccess.Write, FileShare.Read))
                swCommandLineFile.WriteLine(strArguments)
            End Using

            strArguments = "/R:" & PossiblyQuotePath(commandLineFilePath)

            If m_DebugLevel >= 1 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Command line is over 250 characters long; will use /R instead")
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "  " & strExeFilePath & " " & strArguments)
            End If

        End If

        ' Start ICR-2LS.  Note that .Runprogram will not return until after the ICR2LS.exe closes
        ' However, it will raise a Loop Waiting event every MONITOR_INTERVAL_SECONDS seconds (see CmdRunner_LoopWaiting)
        blnSuccess = mCmdRunner.RunProgram(strExeFilePath, strArguments, "ICR2LS.exe", True)

        ' Pause for another 500 msec to make sure ICR-2LS closes
        Threading.Thread.Sleep(500)

        ' Make sure the status file is parsed one final time
        ParseICR2LSStatusFile(mStatusFilePath, True)

        If Not mStatusFileWatcher Is Nothing Then
            mStatusFileWatcher.EnableRaisingEvents = False
            mStatusFileWatcher = Nothing
        End If

        'Stop the job timer
        m_StopTime = DateTime.UtcNow

        If Not blnSuccess Then
            ' ProgRunner returned false, check the Exit Code
            If mCmdRunner.ExitCode <> 0 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "ICR2LS.exe returned a non-zero exit code: " & mCmdRunner.ExitCode.ToString)
            Else
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Call to ICR2LS.exe failed (but exit code is 0)")
            End If

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Most recent ICR-2LS State: " & mICR2LSStatus.ProcessingState & " with " & mICR2LSStatus.ScansProcessed & " scans processed (" & mICR2LSStatus.PercentComplete.ToString("0.0") & "% done); Status = " & mICR2LSStatus.ProcessingStatus)

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error running ICR-2LS.exe: " & m_JobNum)
        Else

            'Verify ICR-2LS exited due to job completion

            If mICR2LSStatus.ProcessingState.ToLower <> ICR2LS_STATE_FINISHED.ToLower Then

                If mICR2LSStatus.ProcessingState.ToLower = ICR2LS_STATE_ERROR.ToLower Or
                   mICR2LSStatus.ProcessingState.ToLower = ICR2LS_STATE_KILLED.ToLower Or
                   m_progress < 100 Then
                    eLogLevel = clsLogTools.LogLevels.ERROR
                Else
                    eLogLevel = clsLogTools.LogLevels.WARN
                End If

                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, eLogLevel, "ICR-2LS processing state not Finished: " & mICR2LSStatus.ProcessingState & "; Processed " & mICR2LSStatus.ScansProcessed & " scans (" & mICR2LSStatus.PercentComplete.ToString("0.0") & "% complete); Status = " & mICR2LSStatus.ProcessingStatus)

                If m_progress >= 100 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Progress reported by ICR-2LS is 100%, so will assume the job is complete")
                    blnSuccess = True
                Else
                    blnSuccess = False
                End If

            Else
                If m_DebugLevel > 0 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Processing state Finished; Processed " & mICR2LSStatus.ScansProcessed & " scans")
                End If
                blnSuccess = True
            End If
        End If

        Return blnSuccess

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

        Dim progLoc = m_mgrParams.GetParam("ICR2LSprogloc")
        If String.IsNullOrEmpty(progLoc) Then
            m_message = "Manager parameter ICR2LSprogloc is not defined"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error in SetStepTaskToolVersion: " & m_message)
            Return False
        End If

        ' Store paths to key files in ioToolFiles
        Dim ioToolFiles As New List(Of FileInfo)
        ioToolFiles.Add(New FileInfo(progLoc))

        Try
            Return MyBase.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles)
        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion: " & ex.Message)
            Return False
        End Try

    End Function

    Private Function ValidateICR2LSStatus(strProcessingState As String) As Boolean
        Dim blnValid As Boolean

        Select Case strProcessingState.ToLower
            Case ICR2LS_STATE_UNKNOWN.ToLower()
                blnValid = True
            Case ICR2LS_STATE_IDLE.ToLower()
                blnValid = True
            Case ICR2LS_STATE_PROCESSING.ToLower()
                blnValid = True
            Case ICR2LS_STATE_KILLED.ToLower()
                blnValid = True
            Case ICR2LS_STATE_ERROR.ToLower()
                blnValid = True
            Case ICR2LS_STATE_FINISHED.ToLower()
                blnValid = True
            Case ICR2LS_STATE_GENERATING.ToLower()
                blnValid = True
            Case ICR2LS_STATE_TICGENERATION.ToLower()
                blnValid = True
            Case ICR2LS_STATE_LCQTICGENERATION.ToLower()
                blnValid = True
            Case ICR2LS_STATE_QTOFPEKGENERATION.ToLower()
                blnValid = True
            Case ICR2LS_STATE_MMTOFPEKGENERATION.ToLower()
                blnValid = True
            Case ICR2LS_STATE_LTQFTPEKGENERATION.ToLower()
                blnValid = True
            Case Else
                blnValid = False
        End Select

        Return blnValid
    End Function

    Protected Function VerifyPEKFileExists(strFolderPath As String, strDatasetName As String) As Boolean

        Dim fiFolder As DirectoryInfo
        Dim blnMatchFound As Boolean

        Try

            fiFolder = New DirectoryInfo(strFolderPath)

            If fiFolder.Exists Then
                If fiFolder.GetFiles(strDatasetName & "*.pek").Length > 0 Then
                    blnMatchFound = True
                End If
            Else
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error in VerifyPEKFileExists; folder not found: " & strFolderPath)
            End If

        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception in VerifyPEKFileExists: " & ex.Message)
        End Try

        Return blnMatchFound

    End Function

    Private Sub CmdRunner_LoopWaiting() Handles mCmdRunner.LoopWaiting
        Const NORMAL_LOG_INTERVAL_MINUTES = 30
        Const DEBUG_LOG_INTERVAL_MINUTES = 5

        Const CHECKPOINT_SAVE_INTERVAL_MINUTES = 1

        Dim dblMinutesElapsed As Double
        Dim blnLogStatus As Boolean

        dblMinutesElapsed = DateTime.UtcNow.Subtract(mLastStatusLogTime).TotalMinutes
        If m_DebugLevel > 0 Then
            If dblMinutesElapsed >= DEBUG_LOG_INTERVAL_MINUTES AndAlso m_DebugLevel >= 2 Then
                blnLogStatus = True
            ElseIf dblMinutesElapsed >= NORMAL_LOG_INTERVAL_MINUTES Then
                blnLogStatus = True
            End If

            If blnLogStatus Then
                mLastStatusLogTime = DateTime.UtcNow

                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerICRBase.CmdRunner_LoopWaiting(); " &
                                                           "Processing Time = " & DateTime.UtcNow.Subtract(m_StartTime).TotalMinutes.ToString("0.0") & " minutes; " &
                                                           "Progress = " & m_progress.ToString("0.00") & "; " &
                                                           "Scans Processed = " & mICR2LSStatus.ScansProcessed.ToString)
            End If
        End If

        If DateTime.UtcNow.Subtract(mLastCheckpointTime).TotalMinutes >= CHECKPOINT_SAVE_INTERVAL_MINUTES Then
            mLastCheckpointTime = DateTime.UtcNow
            CopyCheckpointFile()
        End If
    End Sub

    Private Sub mStatusFileWatcher_Changed(sender As Object, e As FileSystemEventArgs) Handles mStatusFileWatcher.Changed
        ParseICR2LSStatusFile(mStatusFilePath, False)
    End Sub

    Private Sub mPEKtoCSVConverter_ErrorEvent(sender As Object, e As PEKtoCSVConverter.PEKtoCSVConverter.MessageEventArgs) Handles mPEKtoCSVConverter.ErrorEvent
        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "PEKtoCSVConverter error: " & e.Message)
        clsGlobal.AppendToComment(m_message, e.Message)
    End Sub

    Private Sub mPEKtoCSVConverter_MessageEvent(sender As Object, e As PEKtoCSVConverter.PEKtoCSVConverter.MessageEventArgs) Handles mPEKtoCSVConverter.MessageEvent
        If e.Message.Contains("% complete; scan") Then
            ' Message is of the form: 35% complete; scan 2602
            ' Only log this message every 15 seconds
            If DateTime.UtcNow.Subtract(mLastPekToCsvPercentCompleteTime).TotalSeconds < 15 Then
                Return
            End If
            mLastPekToCsvPercentCompleteTime = DateTime.UtcNow
        End If

        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, e.Message)
    End Sub
End Class
