'*********************************************************************************************************
' Written by John Sandoval for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2008, Battelle Memorial Institute
' Created 07/25/2008
'
' Last modified 11/18/2008
' Last modified 06/15/2009 JDS - Added logging using log4net
'*********************************************************************************************************

imports AnalysisManagerBase
Imports PRISM.Files
Imports PRISM.Files.clsFileTools
Imports AnalysisManagerBase.clsGlobal
Imports System.io
Imports System.Text.RegularExpressions

Public Class clsAnalysisToolRunnerIN
    Inherits clsAnalysisToolRunnerBase

    '*********************************************************************************************************
    'Class for running InSpecT analysis
    '*********************************************************************************************************

#Region "Structures"
    Protected Structure udtModInfoType
        Public ModName As String
        Public ModMass As String           ' Storing as a string since reading from a text file and writing to a text file
        Public Residues As String
    End Structure

    Protected Structure udtCachedSpectraCountInfoType
        Public MostRecentSpectrumInfo As String
        Public MostRecentLineNumber As Integer
        Public CachedCount As Integer
    End Structure
#End Region

#Region "Module Variables"
    Public Const INSPECT_INPUT_PARAMS_FILENAME As String = "inspect_input.txt"

    Protected WithEvents CmdRunner As clsRunDosProgram

    Protected mInspectCustomParamFileName As String

    Protected mInspectConcatenatedDtaFilePath As String = ""
    Protected mInspectResultsFilePath As String = ""
    Protected mInspectErrorFilePath As String = ""

    Protected m_isParallelInspect As Boolean

    Protected mInspectSearchLogFilePath As String = "InspectSearchLog.txt"      ' This value gets updated in function RunTool
    Protected mInspectSearchLogMostRecentEntry As String = String.Empty

    Protected mInspectConsoleOutputFilePath As String

    Protected WithEvents mSearchLogFileWatcher As System.IO.FileSystemWatcher
    Protected m_CloneStepRenum As String
    Protected m_StepNum As String

#End Region

#Region "Methods"
    ''' <summary>
    ''' Constructor
    ''' </summary>
    ''' <remarks>Presently not used</remarks>
    Public Sub New()

    End Sub

    ''' <summary>
    ''' Initializes class
    ''' </summary>
    ''' <param name="mgrParams">Object containing manager parameters</param>
    ''' <param name="jobParams">Object containing job parameters</param>
    ''' <param name="StatusTools">Object for updating status file as job progresses</param>
    ''' <remarks></remarks>
    Public Overrides Sub Setup(ByVal mgrParams As IMgrParams, ByVal jobParams As IJobParams, _
      ByVal StatusTools As IStatusFile)

        MyBase.Setup(mgrParams, jobParams, StatusTools)

        If m_DebugLevel > 3 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerSeqBase.Setup()")
        End If
    End Sub
    ''' <summary>
    ''' Runs InSpecT tool
    ''' </summary>
    ''' <returns>CloseOutType enum indicating success or failure</returns>
    ''' <remarks></remarks>
    Public Overrides Function RunTool() As IJobParams.CloseOutType
        Dim result As IJobParams.CloseOutType
        Dim OrgDbName As String
        Dim strBaseFilePath As String
        Dim objIndexedDBCreator As New clsCreateInspectIndexedDB

        Dim strFileNameAdder As String
        Dim strParallelizedText As String

        Try
            If m_DebugLevel > 4 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerIN.RunTool(): Enter")
            End If

            OrgDbName = m_jobParams.GetParam("organismDBName")

            'Start the job timer
            m_StartTime = System.DateTime.Now

            result = objIndexedDBCreator.CreateIndexedDbFiles(m_mgrParams, m_jobParams, m_DebugLevel, m_JobNum)
            If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                Return result
            End If

            'Determine if this is a parallelized job
            m_CloneStepRenum = m_jobParams.GetParam("CloneStepRenumberStart")
            m_StepNum = m_jobParams.GetParam("Step")
            strBaseFilePath = Path.Combine(m_WorkDir, m_jobParams.GetParam("datasetNum"))

            'Determine if this is parallelized inspect job
            If System.String.IsNullOrEmpty(m_CloneStepRenum) Then
                strFileNameAdder = ""
                strParallelizedText = "non-parallelized"
                m_isParallelInspect = False
            Else
                strFileNameAdder = "_" & (CInt(m_StepNum) - CInt(m_CloneStepRenum) + 1).ToString()
                strParallelizedText = "parallelized"
                m_isParallelInspect = True
            End If

            mInspectConcatenatedDtaFilePath = strBaseFilePath & strFileNameAdder & "_dta.txt"
            mInspectResultsFilePath = strBaseFilePath & strFileNameAdder & "_inspect.txt"
            mInspectErrorFilePath = strBaseFilePath & strFileNameAdder & "_error.txt"
            mInspectSearchLogFilePath = System.IO.Path.Combine(m_WorkDir, "InspectSearchLog" & strFileNameAdder & ".txt")
            mInspectConsoleOutputFilePath = System.IO.Path.Combine(m_WorkDir, "InspectConsoleOutput" & strFileNameAdder & ".txt")

            If m_DebugLevel >= 3 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Running " & strParallelizedText & " inspect on " & System.IO.Path.GetFileName(mInspectConcatenatedDtaFilePath))
            End If

            result = RunInSpecT()
            If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                Return result
            End If

            'If not a parallelized job, run as normal
            If Not m_isParallelInspect Then
                'Zip the output file
                Dim ZipResult As IJobParams.CloseOutType = ZipMainOutputFile()
            End If

            'Stop the job timer
            m_StopTime = System.DateTime.Now

            'Add the current job data to the summary file
            If Not UpdateSummaryFile() Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN, "Error creating summary file, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step"))
            End If

            'Make sure objects are released
            System.Threading.Thread.Sleep(2000)        '2 second delay
            GC.Collect()
            GC.WaitForPendingFinalizers()

            result = MakeResultsFolder()
            If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                'TODO: What do we do here?
                Return result
            End If

            result = MoveResultFiles()
            If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                'TODO: What do we do here?
                Return result
            End If

            result = CopyResultsFolderToServer()
            If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                'TODO: What do we do here?
                Return result
            End If

            If Not clsGlobal.RemoveNonResultFiles(m_WorkDir, m_DebugLevel) Then
                m_message = AppendToComment(m_message, "Error deleting non-result files")
                'TODO: Figure out what to do here
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

        Catch ex As Exception
            m_message = "Error in InspectPlugin->RunTool: " & ex.Message
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS 'No failures so everything must have succeeded

    End Function

    ''' <summary>
    ''' Build inspect input file from base parameter file
    ''' </summary>
    ''' <returns>CloseOutType enum indicating success or failure</returns>
    ''' <remarks></remarks>
    Private Function BuildInspectInputFile() As String
        Dim result As String = String.Empty

        ' set up input to reference spectra file, and parameter file
        Dim ParamFilename As String
        Dim orgDbDir As String
        Dim fastaFilename As String
        Dim dbFilename As String
        Dim mzXMLFilename As String
        Dim inputFilename As String
        Dim strInputSpectra As String

        Try
            ParamFilename = Path.Combine(m_WorkDir, m_jobParams.GetParam("parmFileName"))
            orgDbDir = m_mgrParams.GetParam("orgdbdir")
            fastaFilename = Path.Combine(orgDbDir, m_jobParams.GetParam("generatedFastaName"))
            dbFilename = fastaFilename.Replace("fasta", "trie")
            mzXMLFilename = Path.Combine(m_WorkDir, m_jobParams.GetParam("datasetNum") & ".mzXML")
            inputFilename = Path.Combine(m_WorkDir, INSPECT_INPUT_PARAMS_FILENAME)
            strInputSpectra = String.Empty

            'add extra lines to the parameter files
            'the parameter file will become the input file for inspect
            Dim swInspectInputFile As StreamWriter = New StreamWriter((New System.IO.FileStream(inputFilename, FileMode.Create, FileAccess.Write, FileShare.Read)))

            ' Create an instance of StreamReader to read from a file.
            Dim srInputBase As StreamReader = New StreamReader((New System.IO.FileStream(ParamFilename, FileMode.Open, FileAccess.Read, FileShare.Read)))
            Dim strParamLine As String

            swInspectInputFile.WriteLine("#Use the following to define the name of the log file created by Inspect (default is InspectSearchLog.txt if not defined)")
            swInspectInputFile.WriteLine("SearchLogFileName," & mInspectSearchLogFilePath)
            swInspectInputFile.WriteLine()

            swInspectInputFile.WriteLine("#Spectrum file to search; preferred formats are .mzXML and .mgf")

            'The code below was commented out since we are only supporting dta files.
            'If clsGlobal.GetJobParameter(m_jobParams, "UseMzXML", False) Then
            '    strInputSpectra = String.Copy(mzXMLFilename)
            'Else
            If clsAnalysisResourcesIN.DECONCATENATE_DTA_TXT_FILE Then
                strInputSpectra = String.Copy(m_WorkDir)
            Else
                strInputSpectra = String.Copy(mInspectConcatenatedDtaFilePath)
            End If
            'End If

            If m_DebugLevel >= 3 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Inspect input spectra: " & strInputSpectra)
            End If

            swInspectInputFile.WriteLine("spectra," & strInputSpectra)
            swInspectInputFile.WriteLine()

            swInspectInputFile.WriteLine("#Note: The fully qualified database (.trie file) filename")
            swInspectInputFile.WriteLine("DB," & dbFilename)

            ' Read and display the lines from the file until the end 
            ' of the file is reached.
            Do
                strParamLine = srInputBase.ReadLine()
                If strParamLine Is Nothing Then
                    Exit Do
                End If
                swInspectInputFile.WriteLine(strParamLine)
            Loop Until strParamLine Is Nothing
            srInputBase.Close()
            swInspectInputFile.Close()

            If m_DebugLevel >= 2 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Created Inspect input file '" & inputFilename & "' using '" & ParamFilename & "'")
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Using DB '" & dbFilename & "' and input spectra '" & strInputSpectra & "'")
            End If

        Catch ex As Exception
            ' Let the user know what went wrong.
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerIN.BuildInspectInputFile-> error while writing file: " & ex.Message)
            Return String.Empty
        End Try

        Return inputFilename

    End Function

    Private Function ExtractScanCountValueFromMzXML(ByVal strMZXMLFilename As String) As Integer

        Dim intScanCount As Integer

        Dim objMZXmlFile As MSDataFileReader.clsMzXMLFileReader
        Dim objSpectrumInfo As MSDataFileReader.clsSpectrumInfo

        Try
            objMZXmlFile = New MSDataFileReader.clsMzXMLFileReader()

            ' Open the file
            objMZXmlFile.OpenFile(strMZXMLFilename)

            ' Read the first spectrum (required to determine the ScanCount)
            If objMZXmlFile.ReadNextSpectrum(objSpectrumInfo) Then
                intScanCount = objMZXmlFile.ScanCount
            End If

            If Not objMZXmlFile Is Nothing Then
                objMZXmlFile.CloseFile()
            End If

        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerIN.ExtractScanCountValueFromMzXML, Error determining the scan count in the .mzXML file: " & ex.Message)
            Return 0
        End Try

        Return intScanCount

    End Function

    ''' <summary>
    ''' Run -p threshold value
    ''' </summary>
    ''' <returns>Value as a string or empty string means failure</returns>
    ''' <remarks></remarks>
    Private Function getPthresh() As String
        Dim defPvalThresh As String = "0.1"
        Dim tmpPvalThresh As String = ""
        tmpPvalThresh = m_mgrParams.GetParam("InspectPvalueThreshold")
        If tmpPvalThresh <> "" Then
            Return tmpPvalThresh 'return pValueThreshold value in settings file
        Else
            Return defPvalThresh 'if not found, return default of 0.1
        End If

    End Function

    Private Sub InitializeInspectSearchLogFileWatcher(ByVal strWorkDir As String)

        mSearchLogFileWatcher = New System.IO.FileSystemWatcher()
        With mSearchLogFileWatcher
            .BeginInit()
            .Path = strWorkDir
            .IncludeSubdirectories = False
            .Filter = System.IO.Path.GetFileName(mInspectSearchLogFilePath)
            .NotifyFilter = NotifyFilters.LastWrite Or NotifyFilters.Size
            .EndInit()
            .EnableRaisingEvents = True
        End With

    End Sub

    ''' <summary>
    ''' Looks for the inspect _errors.txt file in the working folder.  If present, reads and parses it
    ''' </summary>
    ''' <param name="m_workdir"></param>
    ''' <param name="errorFilename"></param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Protected Function ParseInspectErrorsFile(ByVal m_workdir As String, ByVal errorFilename As String) As Boolean

        Dim srInFile As System.IO.StreamReader

        Dim strInputFilePath As String
        Dim strLineIn As String = String.Empty

        Dim htMessages As System.Collections.Hashtable

        Try

            If m_DebugLevel > 4 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerIN.ParseInspectErrorsFile(): Reading " & errorFilename)
            End If

            strInputFilePath = System.IO.Path.Combine(m_WorkDir, errorFilename)

            If Not System.IO.File.Exists(strInputFilePath) Then
                ' File not found; that means no errors occurred
                Return True
            Else
                Dim fi As System.IO.FileInfo
                fi = New System.IO.FileInfo(errorFilename)

                If fi.Length = 0 Then
                    ' Error file is 0 bytes, which means no errors occurred 
                    ' Delete the file
                    File.Delete(errorFilename)
                    Return True
                End If
            End If

            ' Initialize htMessages
            htMessages = New System.Collections.Hashtable

            ' Read the contents of strInputFilePath
            srInFile = New System.IO.StreamReader(New System.IO.FileStream(strInputFilePath, FileMode.Open, FileAccess.Read, FileShare.Read))

            Do While srInFile.Peek <> -1
                strLineIn = srInFile.ReadLine

                If Not strLineIn Is Nothing Then
                    strLineIn = strLineIn.Trim

                    If strLineIn.Length > 0 Then
                        If Not htMessages.Contains(strLineIn) Then
                            htMessages.Add(strLineIn, 1)
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Inspect warning/error: " & strLineIn)
                        End If
                    End If

                End If
            Loop

            Console.WriteLine()

            If Not srInFile Is Nothing Then
                srInFile.Close()
            End If

        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerIN.ParseInspectErrorsFile, Error reading the Inspect _errors.txt file (" & errorFilename & ")")
            Return False
        End Try

        Return True

    End Function

    ''' <summary>
    ''' Run InSpecT
    ''' </summary>
    ''' <returns>CloseOutType enum indicating success or failure</returns>
    ''' <remarks></remarks>
    Private Function RunInSpecT() As IJobParams.CloseOutType
        Dim CmdStr As String
        Dim InspectDir As String = m_mgrParams.GetParam("inspectdir")
        Dim ParamFilePath As String = Path.Combine(m_WorkDir, m_jobParams.GetParam("parmFileName"))
        Dim blnSuccess As Boolean = False

        ' Build the Inspect Input Parameters file
        mInspectCustomParamFileName = BuildInspectInputFile()
        If mInspectCustomParamFileName.Length = 0 Then
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        CmdRunner = New clsRunDosProgram(InspectDir)

        If m_DebugLevel > 4 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerIN.RunInSpecT(): Enter")
        End If

        ' verify that program file exists
        Dim progLoc As String = System.IO.Path.Combine(InspectDir, "inspect.exe")
        If Not File.Exists(progLoc) Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Cannot find inspect.exe program file")
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        ' Create a file watcher to monitor Search Log created by Inspect
        ' This file is updated after each chunk of 100 spectra are processed
        ' The 4th column of this file displays the PercentComplete value for the overall search
        InitializeInspectSearchLogFileWatcher(m_WorkDir)

        ' Let the user know what went wrong.
        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Starting Inspect")

        ' Set up and execute a program runner to run Inspect.exe
        CmdStr = " -i " & mInspectCustomParamFileName & " -o " & mInspectResultsFilePath & " -e " & mInspectErrorFilePath
        If m_DebugLevel >= 1 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, progLoc & CmdStr)
        End If

        With CmdRunner
            .CreateNoWindow = True
            .CacheStandardOutput = True
            .EchoOutputToConsole = True

            .WriteConsoleOutputToFile = True
            .ConsoleOutputFilePath = mInspectConsoleOutputFilePath
        End With

        If Not CmdRunner.RunProgram(progLoc, CmdStr, "Inspect", True) Then

            If CmdRunner.ExitCode <> 0 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Inspect.exe returned a non-zero exit code: " & CmdRunner.ExitCode.ToString)
            Else
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Call to Inspect.exe failed (but exit code is 0)")
            End If

            Select Case CmdRunner.ExitCode
                Case -1073741819
                    ' Corresponds to message "{W0010} .\PValue.c:453:Only 182 top-scoring matches for charge state; not recalibrating the p-value curve."
                    ' This is a warning, and not an error
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Exit code indicates message from PValue.c concerning not enough top-scoring matches for a given charge state; we ignore this error since it only affects the p-values")
                    blnSuccess = True
                Case -1073741510
                    ' Corresponds to the user pressing Ctrl+Break to stop Inspect
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exit code indicates user pressed Ctrl+Break; job failed")
                Case Else
                    ' Any other code
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Unknown exit code; job failed")
                    blnSuccess = False
            End Select

            If CmdRunner.ExitCode <> 0 Then
                If mInspectSearchLogMostRecentEntry.Length > 0 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Most recent Inspect search log entry: " & mInspectSearchLogMostRecentEntry)
                Else
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Most recent Inspect search log entry: n/a")
                End If
            End If

        Else
            blnSuccess = True
        End If

        If Not blnSuccess Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error running Inspect.exe : " & m_JobNum)
        Else
            m_progress = 100
            m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.RUNNING_TOOL, m_progress, 0, "", "", "", False)
        End If

        If Not mSearchLogFileWatcher Is Nothing Then
            mSearchLogFileWatcher.EnableRaisingEvents = False
            mSearchLogFileWatcher = Nothing
        End If

        ' Parse the _errors.txt file (if it exists) and copy any errors to the analysis manager log
        ParseInspectErrorsFile(m_WorkDir, mInspectErrorFilePath)

        'even though success is returned, check for the result file
        If File.Exists(mInspectResultsFilePath) Then
            blnSuccess = True
        Else
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Inspect results file not found; job failed: " & System.IO.Path.GetFileName(mInspectResultsFilePath))
            blnSuccess = False
        End If

        If blnSuccess Then
            Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
        Else
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If


    End Function

    ''' <summary>
    ''' Zips Inspect search result file
    ''' </summary>
    ''' <returns>CloseOutType enum indicating success or failure</returns>
    ''' <remarks></remarks>
    Private Function ZipMainOutputFile() As IJobParams.CloseOutType
        Dim TmpFile As String
        Dim FileList() As String
        Dim ZipFileName As String

        Try
            Dim Zipper As New ZipTools(m_WorkDir, m_mgrParams.GetParam("zipprogram"))
            FileList = Directory.GetFiles(m_WorkDir, "*_inspect.txt")
            For Each TmpFile In FileList
                ZipFileName = Path.Combine(m_WorkDir, Path.GetFileNameWithoutExtension(TmpFile)) & ".zip"
                If Not Zipper.MakeZipFile("-fast", ZipFileName, Path.GetFileName(TmpFile)) Then
                    Dim Msg As String = "Error zipping output files, job " & m_JobNum
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, Msg)
                    m_message = AppendToComment(m_message, "Error zipping output files")
                    Return IJobParams.CloseOutType.CLOSEOUT_FAILED
                End If
            Next
        Catch ex As Exception
            Dim Msg As String = "clsAnalysisToolRunnerIN.ZipMainOutputFile, Exception zipping output files, job " & m_JobNum & ": " & ex.Message
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, Msg)
            m_message = AppendToComment(m_message, "Error zipping output files")
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        'Delete the Inspect search result file
        Try
            FileList = Directory.GetFiles(m_WorkDir, "*_inspect.txt")
            For Each TmpFile In FileList
                File.SetAttributes(TmpFile, File.GetAttributes(TmpFile) And (Not FileAttributes.ReadOnly))
                File.Delete(TmpFile)
            Next
        Catch Err As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerIN.ZipMainOutputFile, Error deleting _inspect.txt file, job " & m_JobNum & Err.Message)
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

        ' Synchronize the stored Debug level with the value stored in the database
        Const MGR_SETTINGS_UPDATE_INTERVAL_SECONDS As Integer = 300
        MyBase.GetCurrentMgrSettingsFromDB(MGR_SETTINGS_UPDATE_INTERVAL_SECONDS)

        'Update the status file (limit the updates to every 5 seconds)
        If System.DateTime.Now.Subtract(dtLastStatusUpdate).TotalSeconds >= 5 Then
            dtLastStatusUpdate = System.DateTime.Now
            m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.RUNNING_TOOL, m_progress, m_DtaCount, "", "", "", False)
        End If

    End Sub

    Private Sub ParseInspectSearchLogFile(ByVal strSearchLogFilePath As String)
        Dim ioFile As System.IO.FileInfo
        Dim srLogFile As System.IO.StreamReader

        Dim strLineIn As String = String.Empty
        Dim strLastEntry As String = String.Empty
        Dim strSplitline() As String
        Dim strProgress As String

        Try
            ioFile = New System.IO.FileInfo(strSearchLogFilePath)
            If ioFile.Exists AndAlso ioFile.Length > 0 Then
                ' Search log file has been updated
                ' Open the file and read the contents

                srLogFile = New System.IO.StreamReader(New System.IO.FileStream(strSearchLogFilePath, FileMode.Open, FileAccess.Read, FileShare.Write))

                ' Read to the end of the file
                Do While srLogFile.Peek >= 0
                    strLineIn = srLogFile.ReadLine()

                    If Not strLineIn Is Nothing AndAlso strLineIn.Length > 0 Then
                        strLastEntry = String.Copy(strLineIn)
                    End If
                Loop
                srLogFile.Close()

                If Not strLastEntry Is Nothing AndAlso strLastEntry.Length > 0 Then

                    If m_DebugLevel >= 4 Then
                        ' Store the new search log entry in the log
                        If mInspectSearchLogMostRecentEntry.Length = 0 OrElse mInspectSearchLogMostRecentEntry <> strLastEntry Then
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Inspect search log entry: " & strLastEntry)
                        End If
                    End If

                    ' Cache the log entry
                    mInspectSearchLogMostRecentEntry = String.Copy(strLastEntry)

                    strSplitline = strLastEntry.Split(ControlChars.Tab)

                    If strSplitline.Length >= 4 Then
                        ' Parse out the number of spectra from the 3rd column
                        Integer.TryParse(strSplitline(2), m_DtaCount)

                        ' Parse out the % complete from the 4th column
                        ' Use .TrimEnd to remove the trailing % sign
                        strProgress = strSplitline(3).TrimEnd(New Char() {"%"c})
                        Single.TryParse(strProgress, m_progress)
                    End If
                End If

            End If

        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerIN.ParseInspectSearchLogFile, error reading Inspect search log" & ex.Message)
        End Try

    End Sub

    ''' <summary>
    ''' Event handler for mSearchLogFileWatcher.Changed event
    ''' </summary>
    ''' <param name="sender"></param>
    ''' <param name="e"></param>
    ''' <remarks></remarks>
    Private Sub mSearchLogFileWatcher_Changed(ByVal sender As Object, ByVal e As System.IO.FileSystemEventArgs) Handles mSearchLogFileWatcher.Changed
        ParseInspectSearchLogFile(mInspectSearchLogFilePath)
    End Sub

#End Region

End Class
