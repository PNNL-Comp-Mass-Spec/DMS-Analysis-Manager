'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2006, Battelle Memorial Institute
' Created 09/14/2006
'
' Last modified 06/11/2009 JDS - Added logging using log4net
'*********************************************************************************************************

Imports AnalysisManagerBase.AnalysisTool
Imports AnalysisManagerBase.JobConfig
Imports AnalysisManagerBase.StatusReporting
Imports Decon2LS.Readers
Imports Decon2LSRemoter
Imports PRISM.Logging

Public MustInherit Class clsAnalysisToolRunnerDecon2lsBase
    Inherits AnalysisToolRunnerBase

    '*********************************************************************************************************
    'Base class for Decon2LS-specific tasks. Handles tasks common to using Decon2LS for deisotoping and TIC
    'generation
    '
    'This version uses .Net remoting to communicate with a separate process that runs Decon2LS. The separate
    'process is required due to Decon2LS' use of a Finnigan library that puts a lock on the raw data file, preventing
    'cleanup of the working directory after a job completes. Killing the Decon2LS process will
    'release the file lock.
    '*********************************************************************************************************

#Region "Constants"
    Private Const DECON2LS_FATAL_REMOTING_ERROR As String = "Fatal remoting error"
    Private Const DECON2LS_CORRUPTED_MEMORY_ERROR As String = "Corrupted memory error"

    Private Const DECON2LS_SCANS_FILE_SUFFIX As String = "_scans.csv"
    Private Const DECON2LS_ISOS_FILE_SUFFIX As String = "_isos.csv"
    Private Const DECON2LS_PEAKS_FILE_SUFFIX As String = "_peaks.dat"

#End Region

#Region "Module variables"
    Protected m_ToolObj As clsDecon2LSRemoter    'Remote class for execution of Decon2LS via .Net remoting
    Protected m_AnalysisType As String
    Protected m_RemotingTools As clsRemotingTools
    Protected m_ServerRunning As Boolean = False

    ' The following variable is set to True if Decon2LS fails, but at least one loop of analysis succeeded
    Protected mDecon2LSFailedMidLooping As Boolean = False
#End Region

#Region "Enums"
    'Used for result file type
    Enum Decon2LSResultFileType
        DECON2LS_ISOS = 0
        DECON2LS_SCANS = 1
    End Enum

#End Region

#Region "Methods"
    Public Sub New()

    End Sub

    Private Function AssembleFiles(ByVal strCombinedFileName As String, _
                                   ByVal resFileType As Decon2LSResultFileType, _
                                   ByVal intNumResultFiles As Integer) As CloseOutType

        Dim tr As System.IO.StreamReader = Nothing
        Dim tw As System.IO.StreamWriter
        Dim s As String
        Dim fileNameCounter As Integer
        Dim ResultsFile As String = ""
        Dim intLinesRead As Integer

        Dim blnFilesContainHeaderLine As Boolean
        Dim blnHeaderLineWritten As Boolean
        Dim blnAddSegmentNumberToEachLine As Boolean = False

        Try

            tw = CreateNewExportFile(System.IO.Path.Combine(mWorkDir, strCombinedFileName))
            If tw Is Nothing Then
                Return CloseOutType.CLOSEOUT_FAILED
            End If

            For fileNameCounter = 1 To intNumResultFiles
                Select Case resFileType
                    Case Decon2LSResultFileType.DECON2LS_ISOS
                        ResultsFile = mDatasetName & "_" & fileNameCounter & DECON2LS_ISOS_FILE_SUFFIX
                        mJobParams.AddResultFileToSkip(ResultsFile)
                        blnFilesContainHeaderLine = True

                    Case Decon2LSResultFileType.DECON2LS_SCANS
                        ResultsFile = mDatasetName & "_" & fileNameCounter & DECON2LS_SCANS_FILE_SUFFIX
                        mJobParams.AddResultFileToSkip(ResultsFile)
                        blnFilesContainHeaderLine = True

                    Case Else
                        ' Unknown Decon2LSResultFileType
                        LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, "clsAnalysisToolRunnerDecon2lsBase->AssembleFiles: Unknown Decon2ls Result File Type: " & resFileType.ToString)
                        Exit For
                End Select

                If Not System.IO.File.Exists(System.IO.Path.Combine(mWorkDir, ResultsFile)) Then
                    ' Isos or Scans file is not found
                    If resFileType = Decon2LSResultFileType.DECON2LS_SCANS Then
                        ' Be sure to delete the _#_peaks.dat file (which Decon2LS likely created, but it doesn't contain any useful information)
                        ResultsFile = mDatasetName & "_" & fileNameCounter & DECON2LS_PEAKS_FILE_SUFFIX
                        mJobParams.AddResultFileToSkip(ResultsFile)
                    End If
                Else
                    intLinesRead = 0

                    tr = New System.IO.StreamReader(System.IO.Path.Combine(mWorkDir, ResultsFile))

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
            LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.WARN, "clsAnalysisToolRunnerDecon2lsBase.AssembleFiles, job " & mJob & ", step " & mJobParams.GetParam("Step") & ": " & ex.Message)
            Return CloseOutType.CLOSEOUT_FAILED
        End Try

        Return CloseOutType.CLOSEOUT_SUCCESS

    End Function

    Private Function AssembleResults(ByVal blnLoopingEnabled As Boolean, ByVal intNumResultFiles As Integer) As CloseOutType
        Dim result As CloseOutType

        Dim ScansFilePath As String
        Dim IsosFilePath As String
        Dim PeaksFilePath As String

        Try

            ScansFilePath = System.IO.Path.Combine(mWorkDir, mDatasetName & DECON2LS_SCANS_FILE_SUFFIX)
            IsosFilePath = System.IO.Path.Combine(mWorkDir, mDatasetName & DECON2LS_ISOS_FILE_SUFFIX)
            PeaksFilePath = System.IO.Path.Combine(mWorkDir, mDatasetName & DECON2LS_PEAKS_FILE_SUFFIX)

            mJobParams.AddResultFileToKeep(ScansFilePath)
            mJobParams.AddResultFileToKeep(IsosFilePath)
            mJobParams.AddResultFileToKeep(PeaksFilePath)

            If blnLoopingEnabled Then
                If mDebugLevel >= 3 Then
                    LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG, "Assembling Decon2LS " & DECON2LS_SCANS_FILE_SUFFIX & " files")
                End If

                result = AssembleFiles(ScansFilePath, Decon2LSResultFileType.DECON2LS_SCANS, intNumResultFiles)
                If result <> CloseOutType.CLOSEOUT_SUCCESS Then
                    Return result
                End If

                If mDebugLevel >= 3 Then
                    LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG, "Assembling Decon2LS " & DECON2LS_ISOS_FILE_SUFFIX & " files")
                End If

                result = AssembleFiles(IsosFilePath, Decon2LSResultFileType.DECON2LS_ISOS, intNumResultFiles)
                If result <> CloseOutType.CLOSEOUT_SUCCESS Then
                    Return result
                End If
            End If

        Catch ex As System.Exception
            LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, "clsAnalysisToolRunnerDecon2lsBase.AssembleResults, job " & mJob & ", step " & mJobParams.GetParam("Step") & ": " & ex.Message)
            Return CloseOutType.CLOSEOUT_FAILED
        End Try

        Return CloseOutType.CLOSEOUT_SUCCESS

    End Function

    Private Function CreateNewExportFile(ByVal exportFileName As String) As System.IO.StreamWriter
        Dim ef As System.IO.StreamWriter

        If System.IO.File.Exists(exportFileName) Then
            'post error to log
            LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, "clsAnalysisToolRunnerDecon2lsBase->createNewExportFile: Export file already exists (" & exportFileName & "); this is unexpected")
            Return Nothing
        End If

        ef = New System.IO.StreamWriter(exportFileName, False)
        Return ef

    End Function

    Public Overrides Function RunTool() As CloseOutType

        'Runs the Decon2LS analysis tool. The actual tool version details (deconvolute or TIC) will be handled by a subclass

        Dim result As CloseOutType
        Dim RawDataType As String = mJobParams.GetParam("RawDataType")
        Dim TcpPort As Integer = CInt(mMgrParams.GetParam("tcpport"))

        If mDebugLevel > 3 Then
            LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG, "clsAnalysisToolRunnerDecon2LSBase.RunTool()")
        End If

        'Call base class for initial setup
        If Not MyBase.RunTool = CloseOutType.CLOSEOUT_SUCCESS Then
            'Error message is generated in base class, so just exit with error
            Return CloseOutType.CLOSEOUT_FAILED
        End If

        ' Store the Decon2LS version info in the database
        If Not StoreToolVersionInfo() Then
            LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, "Aborting since StoreToolVersionInfo returned false")
            mMessage = "Error determining Decon2LS version"
            Return CloseOutType.CLOSEOUT_FAILED
        End If

        mDecon2LSFailedMidLooping = False

        result = RunDecon2Ls()
        If result <> CloseOutType.CLOSEOUT_SUCCESS Then
            'Run Decon2LS
            mMessage = "Error running Decon2LS"

            ' Only return Closeout_Failed if mDecon2LSFailedMidLooping is False
            If Not mDecon2LSFailedMidLooping Then
                Return CloseOutType.CLOSEOUT_FAILED
            End If
        End If

        'Delete the raw data files
        If mDebugLevel > 3 Then
            LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG, "clsAnalysisToolRunnerDecon2lsBase.RunTool(), Deleting raw data file")
        End If

        If Not DeleteRawDataFiles(mJobParams.GetParam("RawDataType")) Then
            LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, "clsAnalysisToolRunnerDecon2lsBase.RunTool(), Problem deleting raw data files: " & mMessage)
            mMessage = "Error deleting raw data files"
            Return CloseOutType.CLOSEOUT_FAILED
        End If

        'Update the job summary file
        If mDebugLevel > 3 Then
            LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG, "clsAnalysisToolRunnerDecon2lsBase.RunTool(), Updating summary file")
        End If
        UpdateSummaryFile()

        'Make the results folder
        If mDebugLevel > 3 Then
            LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG, "clsAnalysisToolRunnerDecon2lsBase.RunTool(), Making results folder")
        End If

        Dim success As Boolean = MakeResultsDirectory()

        If Not success Then
            'MakeResultsDirectory handles posting to local log, so set database error message and exit
            mMessage = "Error making results folder"
            Return CloseOutType.CLOSEOUT_FAILED
        End If

        success = MoveResultFiles()
        If Not success Then
            'MoveResultFiles moves the result files to the result folder
            mMessage = "Error making results folder"
            Return CloseOutType.CLOSEOUT_FAILED
        End If

        If mDecon2LSFailedMidLooping Then
            ' Try to save whatever files were moved into the results folder
            Dim objAnalysisResults As AnalysisResults = New AnalysisResults(mMgrParams, mJobParams)
            objAnalysisResults.CopyFailedResultsToArchiveDirectory(System.IO.Path.Combine(mWorkDir, mResultsDirectoryName))

            Return CloseOutType.CLOSEOUT_FAILED
        End If

        success = CopyResultsFolderToServer()
        If Not success Then
            'TODO: What do we do here?
            Return result
        End If

        Return CloseOutType.CLOSEOUT_SUCCESS

    End Function

    Protected MustOverride Sub StartDecon2LS()   'Uses overrides in subclasses to handle details of starting Decon2LS

    Protected Overridable Sub CalculateNewStatus()

        'Get the percent complete status from Decon2LS
        mProgress = CSng(m_ToolObj.PercentDone)

    End Sub

    Protected Function RunDecon2Ls() As CloseOutType
        Const DEFAULT_LOOPING_CHUNK_SIZE As Integer = 25000
        Const PARAM_FILE_NAME_TEMP As String = "Decon2LSParamsCurrentLoop.xml"
        Const MAX_SCAN_STOP As Integer = 10000000       ' 10 million

        Dim ScanStart As Integer
        Dim ScanStop As Integer
        Dim LocScanStart As Integer
        Dim LocScanStop As Integer
        Dim blnLoopingEnabled As Boolean = False
        Dim intLoopChunkSize As Integer = DEFAULT_LOOPING_CHUNK_SIZE

        Dim RawDataType As String = mJobParams.GetParam("RawDataType")
        Dim TcpPort As Integer = CInt(mMgrParams.GetParam("tcpport"))

        Dim strParamFile As String = System.IO.Path.Combine(mWorkDir, mJobParams.GetParam("parmFileName"))
        Dim strParamFileCurrentLoop As String = System.IO.Path.Combine(mWorkDir, PARAM_FILE_NAME_TEMP)

        Dim strOutFileCurrentLoop As String
        Dim intLoopNum As Integer
        Dim blnDecon2LSError As Boolean

        mJobParams.AddResultFileToSkip(PARAM_FILE_NAME_TEMP)

        If mJobParams.GetJobParameter("UseDecon2LSLooping", False) Then
            blnLoopingEnabled = True

            intLoopChunkSize = AnalysisManagerBase.Global.CIntSafe(mJobParams.GetParam("Decon2LSLoopingChunkSize"), DEFAULT_LOOPING_CHUNK_SIZE)
            If intLoopChunkSize < 100 Then intLoopChunkSize = 100

            ' Read the ScanStart and ScanStop values from the parameter file
            If Not GetScanValues(strParamFile, ScanStart, ScanStop) Then
                ScanStart = 0
                ScanStop = MAX_SCAN_STOP
            End If

            If ScanStart < 0 Then ScanStart = 0
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

            If mDebugLevel >= 1 Then
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG, "clsAnalysisToolRunnerDecon2lsBase.RunDecon2Ls(), Decon2LSLooping is enabled with chunk size " & intLoopChunkSize.ToString)
            End If

        Else
            LocScanStart = 0
            LocScanStop = MAX_SCAN_STOP
        End If

        ' Get file type of the raw data file
        Dim filetype As Decon2LS.Readers.FileType = GetInputFileType(RawDataType) 'Decon2LS.Readers.FileType.BRUKER
        If filetype = filetype.UNDEFINED Then
            LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, "clsAnalysisToolRunnerDecon2lsBase.RunDecon2Ls(), Invalid data file type specifed while getting file type: " & RawDataType)
            mMessage = "Invalid raw data type specified"
            Return CloseOutType.CLOSEOUT_FAILED
        End If

        ' Specify output file name
        Dim OutFileName As String = System.IO.Path.Combine(mWorkDir, mDatasetName)

        ' Specify Input file or folder
        Dim InpFileName As String = SpecifyInputFileName(RawDataType)
        If InpFileName = "" Then
            LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, "clsAnalysisToolRunnerDecon2lsBase.RunDecon2Ls(), Invalid data file type specifed while input file name: " & RawDataType)
            mMessage = "Invalid raw data type specified"
            Return CloseOutType.CLOSEOUT_FAILED
        End If

        intLoopNum = 0
        Do
            ' Increment the loop counter
            intLoopNum += 1

            'Create an object to handle the .Net Remoting tasks
            m_RemotingTools = New clsRemotingTools(mDebugLevel, TcpPort)

            If blnLoopingEnabled Then
                ' Save as a new temporary parameter file, strParamFile
                WriteTempParamFile(strParamFile, strParamFileCurrentLoop, LocScanStart, LocScanStop)
                strOutFileCurrentLoop = OutFileName & "_" & intLoopNum.ToString
            Else
                strParamFileCurrentLoop = strParamFile
                strOutFileCurrentLoop = OutFileName
            End If

            If mDebugLevel >= 1 Then
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG, "clsAnalysisToolRunnerDecon2lsBase.RunDecon2Ls(), Processing scans " & LocScanStart.ToString & " to " & LocScanStop.ToString)
            End If

            'Start the remoting server
            If Not m_RemotingTools.StartSvr Then
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, "clsAnalysisToolRunnerDecon2lsBase.RunDecon2Ls(), Remoting server startup failed")
                mMessage = "Remoting server startup problem"
                blnDecon2LSError = True
                Exit Do
            End If

            'Delay 5 seconds to allow server to start up
            System.Threading.Thread.Sleep(5000)

            'Init the Decon2LS wrapper
            Try
                'Instantiate the remote object
                m_ToolObj = New clsDecon2LSRemoter
                With m_ToolObj
                    .ResetState()
                    .DataFile = InpFileName
                    .DeconFileType = filetype
                    .OutFile = strOutFileCurrentLoop
                    .ParamFile = strParamFileCurrentLoop
                End With
            Catch ex As System.Exception
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, "clsAnalysisToolRunnerDecon2lsBase.RunDecon2Ls(), Error initializing Decon2LS: " & _
                 ex.Message & "; " & AnalysisManagerBase.Global.GetExceptionStackTrace(ex))
                mMessage = "Error initializing Decon2LS"
                If ex.Message.Contains("No connection could be made because the target machine actively refused it") Then
                    mMessage &= " (" & DECON2LS_FATAL_REMOTING_ERROR & ")"
                ElseIf ex.Message.Contains("Requested Service not found") Then
                    mMessage &= " (" & DECON2LS_FATAL_REMOTING_ERROR & ")"
                ElseIf ex.Message.Contains("memory is corrupt") Then
                    mMessage &= " (" & DECON2LS_CORRUPTED_MEMORY_ERROR & ")"
                End If

                blnDecon2LSError = True
                Exit Do
            End Try

            'Start Decon2LS via the subclass in a separate thread
            Dim Decon2LSThread As New System.Threading.Thread(AddressOf StartDecon2LS)
            Decon2LSThread.Start()

            'Wait for Decon2LS to finish
            System.Threading.Thread.Sleep(3000)       'Pause to ensure Decon2LS has adequate time to start
            WaitForDecon2LSFinish()

            ' Stop the analysis timer
            mStopTime = System.DateTime.UtcNow

            If mDebugLevel >= 3 Then
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG, "clsAnalysisToolRunnerDecon2lsBase.RunDecon2Ls(), Decon2LS finished")
            End If

            ' Determine reason for Decon2LS finish
            Select Case m_ToolObj.DeconState
                Case DMSDecon2LS.DeconState.DONE
                    'This is normal, do nothing else
                Case DMSDecon2LS.DeconState.ERROR
                    LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, "clsAnalysisToolRunnerDecon2lsBase.RunDecon2Ls(), Decon2LS error: " & m_ToolObj.ErrMsg)
                    mMessage = "Decon2LS error"
                    blnDecon2LSError = True

                Case DMSDecon2LS.DeconState.IDLE
                    'Shouldn't ever get here
                    LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, "clsAnalysisToolRunnerDecon2lsBase.RunDecon2Ls(), Decon2LS invalid state: IDLE")
                    mMessage = "Decon2LS error"
                    blnDecon2LSError = True
            End Select

            'Delay to allow Decon2LS a chance to close all files
            System.Threading.Thread.Sleep(5000)      '5 seconds

            'Kill the Decon2LS object and stop the remoting server
            KillDecon2LSObject()
            If blnDecon2LSError Then Exit Do

            If blnLoopingEnabled AndAlso ScanStart < 10000 AndAlso LocScanStart >= 50000 Then
                ' The start scan was less than 10,000 and LocScanStart is now over 50,000
                ' We may have already processed all of the data in the input file
                '
                ' To check for this, see if strOutFileCurrentLoop was actually created
                ' If it wasn't, then we've most likely processed all of the scans in this file and thus should stop looping

                If Not System.IO.File.Exists(strOutFileCurrentLoop) Then
                    LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.WARN, "clsAnalysisToolRunnerDecon2lsBase.RunDecon2Ls(), Could not find file '" & System.IO.Path.GetFileName(strOutFileCurrentLoop) & "' for scan range" & LocScanStart.ToString & " to " & LocScanStop.ToString & "; assuming that the full scan range has been processed")
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
            LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.WARN, "clsAnalysisToolRunnerDecon2lsBase.RunDecon2Ls(), Decon2LS ended in error but we completed " & (intLoopNum - 1).ToString & " chunks; setting mDecon2LSFailedMidLooping to True")

            mDecon2LSFailedMidLooping = True
        End If

        If Not blnDecon2LSError OrElse mDecon2LSFailedMidLooping Then
            ' Either no error, or we had an error but at least one loop finished successfully
            Dim eResult As CloseOutType
            eResult = AssembleResults(blnLoopingEnabled, intLoopNum)

            If eResult <> CloseOutType.CLOSEOUT_SUCCESS Then
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, "clsAnalysisToolRunnerDecon2lsBase.RunDecon2Ls(), AssembleResults returned " & eResult.ToString)
                Return CloseOutType.CLOSEOUT_FAILED
            End If
        End If

        If blnDecon2LSError Then
            Return CloseOutType.CLOSEOUT_FAILED
        Else
            Return CloseOutType.CLOSEOUT_SUCCESS
        End If


    End Function

    Protected Function GetInputFileType(ByVal RawDataType As String) As Decon2LS.Readers.FileType

        'Gets the Decon2LS file type based on the input data type
        Select Case RawDataType.ToLower
            Case "dot_raw_files"
                Return FileType.FINNIGAN
            Case "dot_wiff_files"
                Return FileType.AGILENT_TOF
            Case "dot_raw_folder"
                Return FileType.MICROMASSRAWDATA
            Case "zipped_s_folders"
                If mJobParams.GetParam("instClass").ToLower = "brukerftms" Then
                    Dim NewSourceFolder As String = AnalysisResources.ResolveSerStoragePath(mWorkDir)
                    'Check for "0.ser" folder
                    If Not String.IsNullOrEmpty(NewSourceFolder) Then
                        ' _StoragePathInfo.txt file is present
                        'Data off of Bruker FTICR, in ser file format
                        Return FileType.BRUKER
                    Else
                        ' _StoragePathInfo.txt file is not present
                        'Data off of Bruker FTICR, in zipped s-folder format
                        Return FileType.ICR2LSRAWDATA
                    End If
                ElseIf mJobParams.GetParam("instClass").ToLower = "finnigan_fticr" Then
                'Data from old Finnigan FTICR
                Return FileType.SUNEXTREL
                Else
                'Should never get here
                Return FileType.UNDEFINED
                End If
            Case Else
                'Should never get this value
                Return FileType.UNDEFINED
        End Select

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
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, "Decon2LS param file not found: " & strParamFileCurrent)

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
            LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, "Error in GetScanValues: " & ex.Message)
            Return False
        End Try

        If blnMinScanFound Or blnMaxScanFound Then
            Return True
        Else
            Return False
        End If

    End Function

    Protected Sub KillDecon2LSObject()

        'Removes the Decon2LS object
        If Not IsNothing(m_ToolObj) Then
            m_ToolObj.Dispose()
            m_ToolObj = Nothing
        End If

        'Stop remoting server
        m_RemotingTools.StopSvr()        'At present, no action other than logging is being taken if there is a problem stopping the server.

    End Sub

    Protected Function SpecifyInputFileName(ByVal RawDataType As String) As String

        'Based on the raw data type, assembles a string telling Decon2LS the name of the input file or folder
        Select Case RawDataType.ToLower
            Case "dot_raw_files"
                Return System.IO.Path.Combine(mWorkDir, mDatasetName & ".raw")
            Case "dot_wiff_files"
                Return System.IO.Path.Combine(mWorkDir, mDatasetName & ".wiff")
            Case "dot_raw_folder"
                Return System.IO.Path.Combine(mWorkDir, mDatasetName) & ".raw/_FUNC001.DAT"
            Case "zipped_s_folders"
                Dim NewSourceFolder As String = AnalysisResources.ResolveSerStoragePath(mWorkDir)
                'Check for "0.ser" folder
                If Not String.IsNullOrEmpty(NewSourceFolder) Then
                    Return NewSourceFolder
                Else
                    Return System.IO.Path.Combine(mWorkDir, mDatasetName)
                End If

            Case Else
                'Should never get this value
                Return ""
        End Select

    End Function

    ''' <summary>
    ''' Stores the tool version info in the database
    ''' </summary>
    ''' <remarks></remarks>
    Protected Function StoreToolVersionInfo() As Boolean

        Dim strToolVersionInfo As String = String.Empty
        Dim strAppFolderPath As String = AnalysisManagerBase.Global.GetAppDirectoryPath()

        If mDebugLevel >= 2 Then
            LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG, "Determining tool version info")
        End If

        ' Lookup the version of DMSDecon2LS
        If Not StoreToolVersionInfoForLoadedAssembly(strToolVersionInfo, "DMSDecon2LS") Then
            Return False
        End If

        ' Lookup the version of DeconEngine
        If Not StoreToolVersionInfoForLoadedAssembly(strToolVersionInfo, "DeconEngine") Then
            Return False
        End If

        ' Store paths to key DLLs in ioToolFiles
        Dim ioToolFiles As New System.Collections.Generic.List(Of System.IO.FileInfo)
        ioToolFiles.Add(New System.IO.FileInfo(System.IO.Path.Combine(strAppFolderPath, "DMSDecon2LS.dll")))
        ioToolFiles.Add(New System.IO.FileInfo(System.IO.Path.Combine(strAppFolderPath, "DeconEngine.dll")))

        Try
            Return MyBase.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles)
        Catch ex As System.Exception
            LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion: " & ex.Message)
            Return False
        End Try

    End Function

    Protected Sub WaitForDecon2LSFinish()

        'Loops while waiting for Decon2LS to finish running

        Dim CurState As DMSDecon2LS.DeconState = m_ToolObj.DeconState
        While (CurState = DMSDecon2LS.DeconState.RUNNING_DECON) Or (CurState = DMSDecon2LS.DeconState.RUNNING_TIC)

            'Update the % completion
            CalculateNewStatus()

            LogProgress("Decon2LS")

            'Update the status file
            mStatusTools.UpdateAndWrite(MgrStatusCodes.RUNNING,TaskStatusCodes.RUNNING, TaskStatusDetailCodes.RUNNING_TOOL, mProgress)

            'Wait 5 seconds, then get a new Decon2LS state
            System.Threading.Thread.Sleep(5000)
            CurState = m_ToolObj.DeconState
            Debug.WriteLine("Current Scan: " & m_ToolObj.CurrentScan)
            If mDebugLevel >= 5 Then
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG, "clsAnalysisToolRunnerDecon2lsBase.WaitForDecon2LSFinish(), Scan " & m_ToolObj.CurrentScan)
            End If

        End While

    End Sub

    Private Function WriteTempParamFile(ByVal strParamFile As String, ByVal strParamFileTemp As String, ByVal NewMinScanValue As Integer, ByRef NewMaxScanValue As Integer) As Boolean
        Dim objParamFile As System.Xml.XmlDocument
        Dim swTempParamFile As System.IO.StreamWriter
        Dim objTempParamFile As System.Xml.XmlTextWriter

        Try
            If Not System.IO.File.Exists(strParamFile) Then
                ' Parameter file not found
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, "Decon2LS param file not found: " & strParamFile)

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
                    LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, "Error writing new param file in WriteTempParamFile: " & ex.Message)
                    Return False

                End Try

            End If
        Catch ex As System.Exception
            LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, "Error reading existing param file in WriteTempParamFile: " & ex.Message)
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

End Class
