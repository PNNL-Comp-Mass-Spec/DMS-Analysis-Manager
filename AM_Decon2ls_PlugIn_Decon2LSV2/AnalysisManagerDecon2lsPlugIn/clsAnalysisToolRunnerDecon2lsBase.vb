Option Strict On

'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2006, Battelle Memorial Institute
' Created 09/14/2006
'
' Last modified 06/11/2009 JDS - Added logging using log4net
'*********************************************************************************************************

'Imports PRISM.Processes
Imports AnalysisManagerBase
'Imports AnalysisManagerBase.clsGlobal
'
'Imports Decon2LS.Readers -> DeconToolsV2.Readers
'Imports System.Exception
'Imports System.Runtime.Remoting
'Imports System.Runtime.Remoting.Channels
'Imports System.Runtime.Remoting.Channels.tcp
'Imports Decon2LSRemoter

Public MustInherit Class clsAnalysisToolRunnerDecon2lsBase
    Inherits clsAnalysisToolRunnerBase

    '*********************************************************************************************************
    'Base class for Decon2LS-specific tasks. Handles tasks common to using Decon2LS for deisotoping and TIC 
    'generation
    '
    '*********************************************************************************************************

#Region "Constants"
    Private Const DECON2LS_CORRUPTED_MEMORY_ERROR As String = "Corrupted memory error"

    Private Const DECON2LS_SCANS_FILE_SUFFIX As String = "_scans.csv"
    Private Const DECON2LS_ISOS_FILE_SUFFIX As String = "_isos.csv"
    Private Const DECON2LS_PEAKS_FILE_SUFFIX As String = "_peaks.dat"

#End Region

#Region "Module variables"

    ' This will be TDL or DLS (though I'm not sure it's actually used anywhere
    Protected m_AnalysisType As String

    Protected mInputFileName As String = String.Empty

    Protected mCurrentLoopParams As udtCurrentLoopParamsType

    ' The following variable is set to True if Decon2LS fails, but at least one loop of analysis succeeded
    Protected mDecon2LSFailedMidLooping As Boolean = False
    Protected mDecon2LSThreadAbortedSinceFinished As Boolean

    Private WithEvents mDeconToolsBackgroundWorker As System.ComponentModel.BackgroundWorker

    Protected mDeconToolsStatus As udtDeconToolsStatusType
   
#End Region

#Region "Enums and Structures"
    'Used for result file type
    Enum Decon2LSResultFileType
        DECON2LS_ISOS = 0
        DECON2LS_SCANS = 1
    End Enum

    Enum DeconToolsStateType
        Idle = 0
        Running = 1
        Complete = 2
        Cancelled = 3
        ErrorCaught = 4
    End Enum

    Protected Structure udtCurrentLoopParamsType
        Public InputFilePath As String
        Public OutputFilePath As String
        Public ParamFilePath As String
        Public DeconFileType As DeconTools.Backend.Globals.MSFileType
    End Structure

    Protected Structure udtDeconToolsStatusType
        Public CurrentState As DeconToolsStateType
        Public ErrorMessage As String
        Public CurrentLCScan As Integer     ' LC Scan number or IMS Frame Number
        Public CurrentIMSScan As Integer    ' Only used if IsUIMF = True
        Public IsUIMF As Boolean
        Public Sub Clear()
            CurrentState = DeconToolsStateType.Idle
            ErrorMessage = String.Empty
            CurrentLCScan = 0
            CurrentIMSScan = 0
            IsUIMF = False
        End Sub

    End Structure
#End Region

#Region "Methods"
    Public Sub New()

    End Sub

    Private Sub mDeconToolsBackgroundWorker_DoWork(ByVal sender As Object, _
                                                   ByVal e As System.ComponentModel.DoWorkEventArgs) Handles mDeconToolsBackgroundWorker.DoWork

        Dim bw As System.ComponentModel.BackgroundWorker

        bw = DirectCast(sender, System.ComponentModel.BackgroundWorker)

        StartDecon2LS(bw, mCurrentLoopParams)

        If bw.CancellationPending Then
            e.Cancel = True
        End If

    End Sub


    Private Function AssembleFiles(ByVal strCombinedFileName As String, _
                                   ByVal resFileType As Decon2LSResultFileType, _
                                   ByVal intNumResultFiles As Integer) As IJobParams.CloseOutType

        Dim tr As System.IO.StreamReader = Nothing
        Dim tw As System.IO.StreamWriter
        Dim s As String
        Dim DatasetName As String
        Dim fileNameCounter As Integer
        Dim ResultsFile As String = ""
        Dim intLinesRead As Integer

        Dim blnFilesContainHeaderLine As Boolean
        Dim blnHeaderLineWritten As Boolean
        Dim blnAddSegmentNumberToEachLine As Boolean = False

        Try
            DatasetName = m_jobParams.GetParam("datasetNum")

            tw = CreateNewExportFile(System.IO.Path.Combine(m_WorkDir, strCombinedFileName))
            If tw Is Nothing Then
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            For fileNameCounter = 1 To intNumResultFiles
                Select Case resFileType
                    Case Decon2LSResultFileType.DECON2LS_ISOS
                        ResultsFile = DatasetName & "_" & fileNameCounter & DECON2LS_ISOS_FILE_SUFFIX
                        clsGlobal.FilesToDelete.Add(ResultsFile)
                        blnFilesContainHeaderLine = True

                    Case Decon2LSResultFileType.DECON2LS_SCANS
                        ResultsFile = DatasetName & "_" & fileNameCounter & DECON2LS_SCANS_FILE_SUFFIX
                        clsGlobal.FilesToDelete.Add(ResultsFile)
                        blnFilesContainHeaderLine = True

                    Case Else
                        ' Unknown Decon2LSResultFileType
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerDecon2lsBase->AssembleFiles: Unknown Decon2ls Result File Type: " & resFileType.ToString)
                        Exit For
                End Select

                If Not System.IO.File.Exists(System.IO.Path.Combine(m_WorkDir, ResultsFile)) Then
                    ' Isos or Scans file is not found
                    If resFileType = Decon2LSResultFileType.DECON2LS_SCANS Then
                        ' Be sure to delete the _#_peaks.dat file (which Decon2LS likely created, but it doesn't contain any useful information)
                        ResultsFile = DatasetName & "_" & fileNameCounter & DECON2LS_PEAKS_FILE_SUFFIX
                        clsGlobal.FilesToDelete.Add(ResultsFile)
                    End If
                Else
                    intLinesRead = 0

                    tr = New System.IO.StreamReader(System.IO.Path.Combine(m_WorkDir, ResultsFile))

                    intLinesRead = 0
                    Do While tr.Peek() >= 0
                        s = tr.ReadLine

                        If Not s Is Nothing Then

                            intLinesRead += 1
                            If intLinesRead = 1 Then

                                If blnFilesContainHeaderLine Then
                                    ' The first line is the header line
                                    ' Only write it out for the first file processed
                                    If Not blnHeaderLineWritten Then
                                        tw.WriteLine(s)
                                        blnHeaderLineWritten = True
                                    End If
                                Else
                                    tw.WriteLine(s)
                                End If

                            Else
                                ' Write out the line
                                tw.WriteLine(s)
                            End If

                        End If
                    Loop

                    tr.Close()
                End If

            Next fileNameCounter

            'close the main result file
            tw.Close()

        Catch ex As System.Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "clsAnalysisToolRunnerDecon2lsBase.AssembleFiles, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step") & ": " & ex.Message)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    Private Function AssembleResults(ByVal blnLoopingEnabled As Boolean, ByVal intNumResultFiles As Integer) As IJobParams.CloseOutType
        Dim result As IJobParams.CloseOutType

        Dim DatasetName As String
        Dim ScansFilePath As String
        Dim IsosFilePath As String
        Dim PeaksFilePath As String

        Try
            DatasetName = m_jobParams.GetParam("datasetNum")

            ScansFilePath = System.IO.Path.Combine(m_WorkDir, DatasetName & DECON2LS_SCANS_FILE_SUFFIX)
            IsosFilePath = System.IO.Path.Combine(m_WorkDir, DatasetName & DECON2LS_ISOS_FILE_SUFFIX)
            PeaksFilePath = System.IO.Path.Combine(m_WorkDir, DatasetName & DECON2LS_PEAKS_FILE_SUFFIX)

            clsGlobal.m_ExceptionFiles.Add(ScansFilePath)
            clsGlobal.m_ExceptionFiles.Add(IsosFilePath)
            clsGlobal.m_ExceptionFiles.Add(PeaksFilePath)

            If blnLoopingEnabled Then
                If m_DebugLevel >= 3 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Assembling Decon2LS " & DECON2LS_SCANS_FILE_SUFFIX & " files")
                End If

                result = AssembleFiles(ScansFilePath, Decon2LSResultFileType.DECON2LS_SCANS, intNumResultFiles)
                If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                    Return result
                End If

                If m_DebugLevel >= 3 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Assembling Decon2LS " & DECON2LS_ISOS_FILE_SUFFIX & " files")
                End If

                result = AssembleFiles(IsosFilePath, Decon2LSResultFileType.DECON2LS_ISOS, intNumResultFiles)
                If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                    Return result
                End If
            End If

        Catch ex As System.Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerDecon2lsBase.AssembleResults, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step") & ": " & ex.Message)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    Private Function CreateNewExportFile(ByVal exportFileName As String) As System.IO.StreamWriter
        Dim ef As System.IO.StreamWriter

        If System.IO.File.Exists(exportFileName) Then
            'post error to log
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerDecon2lsBase->createNewExportFile: Export file already exists (" & exportFileName & "); this is unexpected")
            Return Nothing
        End If

        ef = New System.IO.StreamWriter(exportFileName, False)
        Return ef

    End Function

    Public Overrides Function RunTool() As IJobParams.CloseOutType

        'Runs the Decon2LS analysis tool. The actual tool version details (deconvolute or TIC) will be handled by a subclass

        Dim result As IJobParams.CloseOutType
        Dim RawDataType As String = m_jobParams.GetParam("RawDataType")
        'Dim TcpPort As Integer = CInt(m_mgrParams.GetParam("tcpport"))
        Dim eReturnCode As IJobParams.CloseOutType

        ' Set this to success for now
        eReturnCode = IJobParams.CloseOutType.CLOSEOUT_SUCCESS

        If m_DebugLevel > 3 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerDecon2LSBase.RunTool()")
        End If

        'Get the setup file by running the base class method
        If Not MyBase.RunTool = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            'Error message is generated in base class, so just exit with error
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        mDecon2LSFailedMidLooping = False

        'Run Decon2LS
        result = RunDecon2Ls()
        If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            ' Something went wrong
            ' In order to help diagnose things, we will move whatever files were created into the result folder, 
            '  archive it using CopyFailedResultsToArchiveFolder, then return IJobParams.CloseOutType.CLOSEOUT_FAILED
            m_message = "Error running Decon2LS"

            eReturnCode = IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        'Delete the raw data files
        If m_DebugLevel > 3 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerDecon2lsBase.RunTool(), Deleting raw data file")
        End If

        If DeleteRawDataFiles(m_jobParams.GetParam("RawDataType")) <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerDecon2lsBase.RunTool(), Problem deleting raw data files: " & m_message)
            m_message = "Error deleting raw data files"
            ' Don't treat this as a critical error; leave eReturnCode unchanged
        End If

        'Update the job summary file
        If m_DebugLevel > 3 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerDecon2lsBase.RunTool(), Updating summary file")
        End If
        UpdateSummaryFile()

        'Make the results folder
        If m_DebugLevel > 3 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerDecon2lsBase.RunTool(), Making results folder")
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

        If mDecon2LSFailedMidLooping Or eReturnCode = IJobParams.CloseOutType.CLOSEOUT_FAILED Then
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

        If Not clsGlobal.RemoveNonResultFiles(m_mgrParams.GetParam("workdir"), m_DebugLevel) Then
            Dim Msg As String
            Msg = "Error removing non result files"
            m_message = AnalysisManagerBase.clsGlobal.AppendToComment(m_message, Msg)
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerDecon2lsBase.RunTool(); " & Msg)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        'If we get to here, everything worked so exit happily
        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    Protected MustOverride Sub StartDecon2LS(ByRef bw As System.ComponentModel.BackgroundWorker, _
                                             ByVal udtCurrentLoopParams As udtCurrentLoopParamsType)   'Uses overrides in subclasses to handle details of starting Decon2LS

    Protected Function RunDecon2Ls() As IJobParams.CloseOutType
        Const DEFAULT_LOOPING_CHUNK_SIZE As Integer = 25000
        Const PARAM_FILE_NAME_TEMP As String = "Decon2LSParamsCurrentLoop.xml"
        Const MAX_SCAN_STOP As Integer = 10000000       ' 10 million

        Dim ScanStart As Integer
        Dim ScanStop As Integer
        Dim LocScanStart As Integer
        Dim LocScanStop As Integer
        Dim blnLoopingEnabled As Boolean = False
        Dim intLoopChunkSize As Integer = DEFAULT_LOOPING_CHUNK_SIZE

        Dim RawDataType As String = m_jobParams.GetParam("RawDataType")
        'Dim TcpPort As Integer = CInt(m_mgrParams.GetParam("tcpport"))

        Dim strParamFile As String = System.IO.Path.Combine(m_WorkDir, m_jobParams.GetParam("parmFileName"))
        Dim strParamFileCurrentLoop As String = System.IO.Path.Combine(m_WorkDir, PARAM_FILE_NAME_TEMP)

        Dim strOutFileCurrentLoop As String
        Dim intLoopNum As Integer
        Dim blnDecon2LSError As Boolean

        clsGlobal.FilesToDelete.Add(PARAM_FILE_NAME_TEMP)

        ' See if Decon2LS Looping is enabled
        ' If True, then we will call Decon2LS repeatedly, processing a limited range of scans for each loop
        If clsGlobal.CBoolSafe(m_jobParams.GetParam("UseDecon2LSLooping"), False) Then
            blnLoopingEnabled = True

            intLoopChunkSize = clsGlobal.CIntSafe(m_jobParams.GetParam("Decon2LSLoopingChunkSize"), DEFAULT_LOOPING_CHUNK_SIZE)
            If intLoopChunkSize < 100 Then intLoopChunkSize = 100

            ' Read the ScanStart and ScanStop values from the parameter file
            If Not GetScanValues(strParamFile, ScanStart, ScanStop) Then
                ScanStart = 0
                ScanStop = MAX_SCAN_STOP
            End If

            If ScanStart < 1 Then ScanStart = 1
            If ScanStop > MAX_SCAN_STOP Then
                ScanStop = MAX_SCAN_STOP
            End If

            'Set up parameters to loop through scan ranges
            LocScanStart = ScanStart

            If ScanStop > (LocScanStart + intLoopChunkSize - 1) Then
                LocScanStop = LocScanStart + intLoopChunkSize - 1
            Else
                LocScanStop = ScanStop
            End If
        Else
            LocScanStart = 1
            LocScanStop = MAX_SCAN_STOP
        End If

        ' '' See if the PickPeaksOnly mode is enabled
        ' '' If enabled, then Decon2LS will run the peak-picking algorithm, but will not deisotope spectra
        ''If clsGlobal.CBoolSafe(m_jobParams.GetParam("Decon2LSPickPeaksOnly"), False) Then
        ''    mPickPeaksOnly = True
        ''Else
        ''    mPickPeaksOnly = False
        ''End If

        ' Get file type of the raw data file
        Dim filetype As DeconTools.Backend.Globals.MSFileType = GetInputFileType(RawDataType)
        If filetype = DeconTools.Backend.Globals.MSFileType.Undefined Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerDecon2lsBase.RunDecon2Ls(), Invalid data file type specifed while getting file type: " & RawDataType)
            m_message = "Invalid raw data type specified"
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        ' Specify output file name
        Dim OutFileName As String = System.IO.Path.Combine(m_WorkDir, m_jobParams.GetParam("datasetNum"))

        ' Specify Input file or folder
        mInputFileName = SpecifyInputFileName(RawDataType)
        If mInputFileName = "" Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerDecon2lsBase.RunDecon2Ls(), Invalid data file type specifed while input file name: " & RawDataType)
            m_message = "Invalid raw data type specified"
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        intLoopNum = 0
        Do
            ' Increment the loop counter
            intLoopNum += 1

            ' Instantiate the Background worker
            mDeconToolsBackgroundWorker = New System.ComponentModel.BackgroundWorker

            mDeconToolsBackgroundWorker.WorkerReportsProgress = True
            mDeconToolsBackgroundWorker.WorkerSupportsCancellation = True


            If blnLoopingEnabled Then
                ' Save as a new temporary parameter file, strParamFile  
                WriteTempParamFile(strParamFile, strParamFileCurrentLoop, LocScanStart, LocScanStop)
                strOutFileCurrentLoop = OutFileName & "_" & intLoopNum.ToString
            Else
                strParamFileCurrentLoop = strParamFile
                strOutFileCurrentLoop = OutFileName
            End If

            If m_DebugLevel >= 1 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerDecon2lsBase.RunDecon2Ls(), Processing scans " & LocScanStart.ToString & " to " & LocScanStop.ToString)
            End If

          
            ' Reset the log file tracking variables
            mDecon2LSThreadAbortedSinceFinished = False

            ' Initialize mCurrentLoopParams
            With mCurrentLoopParams
                .InputFilePath = mInputFileName
                .OutputFilePath = strOutFileCurrentLoop
                .ParamFilePath = strParamFileCurrentLoop
                .DeconFileType = filetype
            End With

            ' Reset the state variables
            mDeconToolsStatus.Clear()

            'Start Decon2LS via the backgroundworker
            mDeconToolsBackgroundWorker.RunWorkerAsync()

            'Wait for Decon2LS to finish
            WaitForDecon2LSFinish()

            ' Stop the analysis timer
            m_StopTime = System.DateTime.Now

            If m_DebugLevel > 3 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerDecon2lsBase.RunDecon2Ls(), Decon2LS finished")
            End If

            ' Determine reason for Decon2LS finish
            If mDecon2LSThreadAbortedSinceFinished Then
                ' The background worker is still running
                ' However, the log file says things completed successfully
                ' We'll trust the log file
                blnDecon2LSError = False
            Else
                Select Case mDeconToolsStatus.CurrentState
                    Case DeconToolsStateType.Complete
                        'This is normal, do nothing else
                        blnDecon2LSError = False

                    Case DeconToolsStateType.ErrorCaught
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerDecon2lsBase.RunDecon2Ls(), Decon2LS error: " & mDeconToolsStatus.ErrorMessage)
                        m_message = "Decon2LS error"
                        blnDecon2LSError = True

                    Case DeconToolsStateType.Idle
                        'Shouldn't ever get here
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerDecon2lsBase.RunDecon2Ls(), Decon2LS invalid state: IDLE")
                        m_message = "Decon2LS invalid state"
                        blnDecon2LSError = True
                End Select
            End If

            'Delay to allow Decon2LS a chance to close all files
            System.Threading.Thread.Sleep(5000)      '5 seconds

            ' Dispose of the background worker
            mDeconToolsBackgroundWorker.Dispose()

            If blnDecon2LSError Then Exit Do

            If blnLoopingEnabled AndAlso ScanStart < 10000 AndAlso LocScanStart >= 50000 Then
                ' The start scan was less than 10,000 and LocScanStart is now over 50,000
                ' We may have already processed all of the data in the input file
                '
                ' To check for this, see if strOutFileCurrentLoop was actually created
                ' If it wasn't, then we've most likely processed all of the scans in this file and thus should stop looping

                If Not System.IO.File.Exists(strOutFileCurrentLoop) Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "clsAnalysisToolRunnerDecon2lsBase.RunDecon2Ls(), Could not find file '" & System.IO.Path.GetFileName(strOutFileCurrentLoop) & "' for scan range" & LocScanStart.ToString & " to " & LocScanStop.ToString & "; assuming that the full scan range has been processed")
                    Exit Do
                End If
            End If

            If blnLoopingEnabled Then
                'Update loop parameters
                LocScanStart = LocScanStop + 1
                LocScanStop = LocScanStart + intLoopChunkSize - 1
                If LocScanStop > ScanStop Then
                    LocScanStop = ScanStop
                End If
            End If

        Loop While blnLoopingEnabled AndAlso (LocScanStart <= ScanStop)

        If blnDecon2LSError AndAlso intLoopNum > 1 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "clsAnalysisToolRunnerDecon2lsBase.RunDecon2Ls(), Decon2LS ended in error but we completed " & (intLoopNum - 1).ToString & " chunks; setting mDecon2LSFailedMidLooping to True")

            mDecon2LSFailedMidLooping = True
        End If

        If Not blnDecon2LSError OrElse mDecon2LSFailedMidLooping Then
            ' Either no error, or we had an error but at least one loop finished successfully
            Dim eResult As IJobParams.CloseOutType
            eResult = AssembleResults(blnLoopingEnabled, intLoopNum)

            If eResult <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerDecon2lsBase.RunDecon2Ls(), AssembleResults returned " & eResult.ToString)
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If
        End If

        If blnDecon2LSError Then
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        Else
            Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
        End If


    End Function

    Protected Function Decon2LSLogFileReportsFinished(ByRef dtFinishTime As DateTime) As Boolean

        Dim fiFileInfo As System.IO.FileInfo
        Dim srInFile As System.IO.StreamReader

        Dim strLogFilePath As String
        Dim strLineIn As String
        Dim blnFinished As Boolean
        Dim blnDateValid As Boolean

        Dim intCharIndex As Integer

        blnFinished = False

        Try
            strLogFilePath = System.IO.Path.Combine(m_WorkDir, mInputFileName & "_log.txt")

            If System.IO.File.Exists(strLogFilePath) Then
                srInFile = New System.IO.StreamReader(New System.IO.FileStream(strLogFilePath, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.ReadWrite))

                Do While srInFile.Peek >= 0
                    strLineIn = srInFile.ReadLine

                    If Not strLineIn Is Nothing AndAlso strLineIn.Length > 0 Then
                        intCharIndex = strLineIn.ToLower.IndexOf("finished file processing")
                        If intCharIndex >= 0 Then

                            blnDateValid = False
                            If intCharIndex > 1 Then
                                ' Parse out the date from strLineIn
                                If System.DateTime.TryParse(strLineIn.Substring(0, intCharIndex).Trim, dtFinishTime) Then
                                    blnDateValid = True
                                Else
                                    ' Unable to parse out the date
                                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Unable to parse date from string '" & strLineIn.Substring(0, intCharIndex).Trim & "'; will use file modification date as the processing finish time")
                                End If
                            End If

                            If Not blnDateValid Then
                                fiFileInfo = New System.IO.FileInfo(strLogFilePath)
                                dtFinishTime = fiFileInfo.LastWriteTime
                            End If

                            If m_DebugLevel >= 3 Then
                                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Decon2LS log file reports 'finished file processing' at " & dtFinishTime.ToString())
                            End If

                            blnFinished = True
                            Exit Do
                        End If
                    End If
                Loop
            End If

        Catch ex As System.Exception
            ' Ignore errors here
        Finally
            If Not srInFile Is Nothing Then
                srInFile.Close()
            End If
        End Try

        Return blnFinished

    End Function

    Protected Function GetInputFileType(ByVal RawDataType As String) As DeconTools.Backend.Globals.MSFileType

        'Gets the Decon2LS file type based on the input data type
        Select Case RawDataType.ToLower
            Case clsAnalysisResources.RAW_DATA_TYPE_DOT_RAW_FILES
                Return DeconTools.Backend.Globals.MSFileType.Finnigan
            Case clsAnalysisResources.RAW_DATA_TYPE_DOT_WIFF_FILES
                Return DeconTools.Backend.Globals.MSFileType.Agilent_WIFF
            Case clsAnalysisResources.RAW_DATA_TYPE_DOT_UIMF_FILES
                Return DeconTools.Backend.Globals.MSFileType.PNNL_UIMF
            Case clsAnalysisResources.RAW_DATA_TYPE_DOT_RAW_FOLDER
                Return DeconTools.Backend.Globals.MSFileType.Micromass_Rawdata
            Case clsAnalysisResources.RAW_DATA_TYPE_ZIPPED_S_FOLDERS
                If m_jobParams.GetParam("instClass").ToLower = "brukerftms" Then
                    'Data off of Bruker FTICR
                    Return DeconTools.Backend.Globals.MSFileType.Bruker
                ElseIf m_jobParams.GetParam("instClass").ToLower = "finnigan_fticr" Then
                    'Data from old Finnigan FTICR
                    Return DeconTools.Backend.Globals.MSFileType.SUNEXTREL
                Else
                    'Should never get here
                    Return DeconTools.Backend.Globals.MSFileType.Undefined
                End If
            Case "dot_mzxml_files"
                Return DeconTools.Backend.Globals.MSFileType.MZXML_Rawdata
            Case Else
                'Should never get this value
                Return DeconTools.Backend.Globals.MSFileType.Undefined
        End Select

    End Function

    Protected Function SpecifyInputFileName(ByVal RawDataType As String) As String

        'Based on the raw data type, assembles a string telling Decon2LS the name of the input file or folder
        Select Case RawDataType.ToLower
            Case clsAnalysisResources.RAW_DATA_TYPE_DOT_RAW_FILES
                Return System.IO.Path.Combine(m_WorkDir, m_jobParams.GetParam("datasetNum") & clsAnalysisResources.DOT_RAW_EXTENSION)
            Case clsAnalysisResources.RAW_DATA_TYPE_DOT_WIFF_FILES
                Return System.IO.Path.Combine(m_WorkDir, m_jobParams.GetParam("datasetNum") & clsAnalysisResources.DOT_WIFF_EXTENSION)
            Case clsAnalysisResources.RAW_DATA_TYPE_DOT_UIMF_FILES
                Return System.IO.Path.Combine(m_WorkDir, m_jobParams.GetParam("datasetNum") & clsAnalysisResources.DOT_UIMF_EXTENSION)
            Case clsAnalysisResources.RAW_DATA_TYPE_DOT_RAW_FOLDER
                Return System.IO.Path.Combine(m_WorkDir, m_jobParams.GetParam("datasetNum")) & ".raw/_FUNC001.DAT"
            Case clsAnalysisResources.RAW_DATA_TYPE_ZIPPED_S_FOLDERS
                Return System.IO.Path.Combine(m_WorkDir, m_jobParams.GetParam("datasetNum"))
            Case Else
                'Should never get this value
                Return ""
        End Select

    End Function

    Protected Sub WaitForDecon2LSFinish()
        Dim dtLastStatusUpdate As System.DateTime

        Dim dtFinishTime As DateTime
        Dim dtLastLogCheckTime As DateTime = System.DateTime.Now

        Dim blnCheckLogFile As Boolean

        'Loops while waiting for Decon2LS to finish running

        dtLastStatusUpdate = System.DateTime.Now

        While mDeconToolsBackgroundWorker.IsBusy

            ' Update the status every 5 seconds
            If System.DateTime.Now.Subtract(dtLastStatusUpdate).TotalSeconds >= 5 Then
                dtLastStatusUpdate = System.DateTime.Now

                ' Synchronize the stored Debug level with the value stored in the database
                Const MGR_SETTINGS_UPDATE_INTERVAL_SECONDS As Integer = 300
                MyBase.GetCurrentMgrSettingsFromDB(MGR_SETTINGS_UPDATE_INTERVAL_SECONDS)

                'Update the status file
                m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.RUNNING_TOOL, m_progress)

                If System.DateTime.Now.Subtract(dtLastLogCheckTime).TotalSeconds >= 30 Then
                    blnCheckLogFile = True
                Else
                    blnCheckLogFile = False
                End If

                Debug.WriteLine("Current Scan: " & mDeconToolsStatus.CurrentLCScan)
                If m_DebugLevel >= 5 OrElse (m_DebugLevel >= 2 And blnCheckLogFile) Then

                    Dim strProgressMessage As String

                    If mDeconToolsStatus.IsUIMF Then
                        strProgressMessage = "Frame=" & mDeconToolsStatus.CurrentLCScan & ", Scan=" & mDeconToolsStatus.CurrentIMSScan
                    Else
                        strProgressMessage = "Scan=" & mDeconToolsStatus.CurrentLCScan
                    End If

                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerDecon2lsBase.WaitForDecon2LSFinish(), State=" & mDeconToolsStatus.CurrentState & ", " & strProgressMessage & ", " & m_progress.ToString("0.0") & "% complete")

                End If

                ' Parse the Decon2LS _log.txt file every 30 seconds to see if it reports that things have finished
                If blnCheckLogFile Then
                    dtLastLogCheckTime = System.DateTime.Now

                    If Decon2LSLogFileReportsFinished(dtFinishTime) Then
                        ' The Decon2LS Log File reports that the task is complete
                        ' If it finished over 30 seconds ago, then forcibly kill the background worker and exit the while loop

                        If System.DateTime.Now.Subtract(dtFinishTime).TotalSeconds >= 30 Then
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Note: Forcibly closed the Decon2LS thread since the log file reports finished and over 30 seconds has elapsed, yet Decon2LS still reports its state as Running")

                            mDeconToolsBackgroundWorker.CancelAsync()

                            System.Threading.Thread.Sleep(3000)

                            mDecon2LSThreadAbortedSinceFinished = True
                            Exit While
                        End If
                    End If
                End If

            End If

        End While

    End Sub

    Protected Function DeleteRawDataFiles(ByVal RawDataType As String) As IJobParams.CloseOutType

        'Deletes the raw data files/folders from the working directory
        Dim IsFile As Boolean = True
        Dim FileOrFolderName As String

        Select Case RawDataType.ToLower
            Case clsAnalysisResources.RAW_DATA_TYPE_DOT_RAW_FILES
                FileOrFolderName = System.IO.Path.Combine(m_WorkDir, m_jobParams.GetParam("datasetNum") & clsAnalysisResources.DOT_RAW_EXTENSION)
                IsFile = True
            Case clsAnalysisResources.RAW_DATA_TYPE_DOT_WIFF_FILES
                FileOrFolderName = System.IO.Path.Combine(m_WorkDir, m_jobParams.GetParam("datasetNum") & clsAnalysisResources.DOT_WIFF_EXTENSION)
                IsFile = True
            Case clsAnalysisResources.RAW_DATA_TYPE_DOT_UIMF_FILES
                FileOrFolderName = System.IO.Path.Combine(m_WorkDir, m_jobParams.GetParam("datasetNum") & clsAnalysisResources.DOT_UIMF_EXTENSION)
                IsFile = True
            Case clsAnalysisResources.RAW_DATA_TYPE_DOT_RAW_FOLDER
                FileOrFolderName = System.IO.Path.Combine(m_WorkDir, m_jobParams.GetParam("datasetNum") & ".raw")
                IsFile = False
            Case clsAnalysisResources.RAW_DATA_TYPE_ZIPPED_S_FOLDERS
                FileOrFolderName = System.IO.Path.Combine(m_WorkDir, m_jobParams.GetParam("datasetNum"))
                IsFile = False
            Case Else
                'Should never get this value
                m_message = "DeleteRawDataFiles, Invalid RawDataType specified: " & RawDataType
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Select

        If IsFile Then
            'Data is a file, so use file deletion tools
            Try
                ' DeleteFileWithRetries will throw an exception if it cannot delete any raw data files (e.g. the .UIMF file)
                ' Thus, need to wrap it with an Exception handler

                If DeleteFileWithRetries(FileOrFolderName) Then
                    Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
                Else
                    m_message = "Error deleting raw data file " & FileOrFolderName
                    Return IJobParams.CloseOutType.CLOSEOUT_FAILED
                End If

            Catch ex As Exception
                m_message = "Exception deleting raw data file " & FileOrFolderName & ": " & _
                ex.Message & "; " & clsGlobal.GetExceptionStackTrace(ex)
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End Try
        Else
            'Use folder deletion tools
            Try
                System.IO.Directory.Delete(FileOrFolderName, True)
                Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
            Catch ex As System.Exception
                m_message = "Exception deleting raw data folder " & FileOrFolderName & ": " & _
                 ex.Message & "; " & clsGlobal.GetExceptionStackTrace(ex)
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End Try
        End If

    End Function

    Private Function GetScanValues(ByVal strParamFileCurrent As String, ByRef MinScanValueFromParamFile As Integer, ByRef MaxScanValueFromParamFile As Integer) As Boolean
        Dim objParamFile As System.Xml.XmlDocument
        Dim objNode As System.Xml.XmlNode

        Dim blnMinScanFound As Boolean
        Dim blnMaxScanFound As Boolean

        Try
            MinScanValueFromParamFile = 0
            MaxScanValueFromParamFile = 100000

            If Not System.IO.File.Exists(strParamFileCurrent) Then
                ' Parameter file not found
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Decon2LS param file not found: " & strParamFileCurrent)

                Return False
            Else
                ' Open the file and parse the XML
                objParamFile = New System.Xml.XmlDocument
                objParamFile.Load(New System.IO.FileStream(strParamFileCurrent, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.Read))

                ' Look for the XML: <MinScan></MinScan>
                objNode = objParamFile.SelectSingleNode("//parameters/Miscellaneous/MinScan")

                If Not objNode Is Nothing AndAlso objNode.HasChildNodes Then
                    ' Match found
                    ' Read the value of MinScan
                    If System.Int32.TryParse(objNode.ChildNodes(0).Value, MinScanValueFromParamFile) Then
                        blnMinScanFound = True
                    End If
                End If

                ' Look for the XML: <MinScan></MinScan>
                objNode = objParamFile.SelectSingleNode("//parameters/Miscellaneous/MaxScan")

                If Not objNode Is Nothing AndAlso objNode.HasChildNodes Then
                    ' Match found
                    ' Read the value of MinScan
                    If System.Int32.TryParse(objNode.ChildNodes(0).Value, MaxScanValueFromParamFile) Then
                        blnMaxScanFound = True
                    End If
                End If
            End If
        Catch ex As System.Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error in GetScanValues: " & ex.Message)
            Return False
        End Try

        If blnMinScanFound Or blnMaxScanFound Then
            Return True
        Else
            Return False
        End If

    End Function


    Private Function WriteTempParamFile(ByVal strParamFile As String, ByVal strParamFileTemp As String, ByVal NewMinScanValue As Integer, ByRef NewMaxScanValue As Integer) As Boolean
        Dim objParamFile As System.Xml.XmlDocument
        Dim swTempParamFile As System.IO.StreamWriter
        Dim objTempParamFile As System.Xml.XmlTextWriter

        Try
            If Not System.IO.File.Exists(strParamFile) Then
                ' Parameter file not found
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Decon2LS param file not found: " & strParamFile)

                Return False
            Else
                ' Open the file and parse the XML
                objParamFile = New System.Xml.XmlDocument
                objParamFile.Load(New System.IO.FileStream(strParamFile, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.Read))

                ' Look for the XML: <UseScanRange></UseScanRange> in the Miscellaneous section
                ' Set its value to "True" (this sub will add it if not present)
                WriteTempParamFileUpdateElementValue(objParamFile, "//parameters/Miscellaneous", "UseScanRange", "True")

                ' Now update the MinScan value
                WriteTempParamFileUpdateElementValue(objParamFile, "//parameters/Miscellaneous", "MinScan", NewMinScanValue.ToString)

                ' Now update the MaxScan value
                WriteTempParamFileUpdateElementValue(objParamFile, "//parameters/Miscellaneous", "MaxScan", NewMaxScanValue.ToString)

                Try
                    ' Now write out the XML to strParamFileTemp
                    swTempParamFile = New System.IO.StreamWriter(New System.IO.FileStream(strParamFileTemp, IO.FileMode.Create, IO.FileAccess.Write, IO.FileShare.Read))

                    objTempParamFile = New System.Xml.XmlTextWriter(swTempParamFile)
                    objTempParamFile.Indentation = 1
                    objTempParamFile.IndentChar = ControlChars.Tab
                    objTempParamFile.Formatting = Xml.Formatting.Indented

                    objParamFile.WriteContentTo(objTempParamFile)

                    swTempParamFile.Close()

                Catch ex As System.Exception
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error writing new param file in WriteTempParamFile: " & ex.Message)
                    Return False

                End Try

            End If
        Catch ex As System.Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error reading existing param file in WriteTempParamFile: " & ex.Message)
            Return False
        End Try

        Return True


    End Function

    ''' <summary>
    ''' Looks for the section specified by parameter XPathForSection.  If found, updates its value to NewElementValue.  If not found, tries to add a new node with name ElementName
    ''' </summary>
    ''' <param name="objXMLDocument">XML Document object</param>
    ''' <param name="XPathForSection">XPath specifying the section that contains the desired element.  For example: "//parameters/Miscellaneous"</param>
    ''' <param name="ElementName">Element name to find (or add)</param>
    ''' <param name="NewElementValue">New value for this element</param>
    ''' <remarks></remarks>
    Private Sub WriteTempParamFileUpdateElementValue(ByRef objXMLDocument As System.Xml.XmlDocument, ByVal XPathForSection As String, ByVal ElementName As String, ByVal NewElementValue As String)
        Dim objNode As System.Xml.XmlNode
        Dim objNewChild As System.Xml.XmlElement

        objNode = objXMLDocument.SelectSingleNode(XPathForSection & "/" & ElementName)

        If Not objNode Is Nothing Then
            If objNode.HasChildNodes Then
                ' Match found; update the value
                objNode.ChildNodes(0).Value = NewElementValue
            End If
        Else
            objNode = objXMLDocument.SelectSingleNode(XPathForSection)

            If Not objNode Is Nothing Then
                objNewChild = CType(objXMLDocument.CreateNode(Xml.XmlNodeType.Element, ElementName, ""), Xml.XmlElement)
                objNewChild.InnerXml = NewElementValue

                objNode.AppendChild(objNewChild)
            End If

        End If

    End Sub

#End Region

    Private Sub mDeconToolsBackgroundWorker_ProgressChanged(ByVal sender As Object, ByVal e As System.ComponentModel.ProgressChangedEventArgs) Handles mDeconToolsBackgroundWorker.ProgressChanged

        ' Get the progress complete (integer between 0 and 100)
        m_progress = e.ProgressPercentage

        Dim objState As DeconTools.Backend.Core.UserState
        objState = DirectCast(e.UserState, DeconTools.Backend.Core.UserState)

        'objState.CurrentRun.CurrentScanSet.NumIsotopicProfiles

        ' Get the progress complete (decimal value between 0 and 100)
        m_progress = objState.PercentDone

        If TypeOf objState.CurrentRun Is DeconTools.Backend.Runs.UIMFRun Then
            ' Processing an IMS UIMF file
            mDeconToolsStatus.CurrentLCScan = objState.CurrentFrameSet.PrimaryFrame
            mDeconToolsStatus.CurrentIMSScan = objState.CurrentScanSet.PrimaryScanNumber
            mDeconToolsStatus.IsUIMF = True
        Else
            mDeconToolsStatus.CurrentLCScan = objState.CurrentScanSet.PrimaryScanNumber
        End If

    End Sub

    Private Sub mDeconToolsBackgroundWorker_RunWorkerCompleted(ByVal sender As Object, ByVal e As System.ComponentModel.RunWorkerCompletedEventArgs) Handles mDeconToolsBackgroundWorker.RunWorkerCompleted

        If (e.Cancelled) Then
            mDeconToolsStatus.CurrentState = DeconToolsStateType.Cancelled
        ElseIf Not e.Error Is Nothing Then
            mDeconToolsStatus.CurrentState = DeconToolsStateType.ErrorCaught
            mDeconToolsStatus.ErrorMessage = e.Error.Message
        Else
            mDeconToolsStatus.CurrentState = DeconToolsStateType.Complete
            m_progress = 100
        End If

    End Sub
End Class
