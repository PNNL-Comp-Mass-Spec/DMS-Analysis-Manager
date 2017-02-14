'*********************************************************************************************************
' Written by John Sandoval for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2008, Battelle Memorial Institute
' Created 07/25/2008
'
'*********************************************************************************************************

Option Strict On

Imports System.Collections.Generic
Imports System.IO
Imports System.Threading
Imports AnalysisManagerBase
Imports PRISM.Processes

''' <summary>
''' Class for running InSpecT analysis
''' </summary>
''' <remarks></remarks>
Public Class clsAnalysisToolRunnerIN
    Inherits clsAnalysisToolRunnerBase

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
    Protected Const INSPECT_EXE_NAME As String = "inspect.exe"

    Protected mCmdRunner As clsRunDosProgram

    Protected mInspectCustomParamFileName As String

    Protected mInspectConcatenatedDtaFilePath As String = ""
    Protected mInspectResultsFilePath As String = ""
    Protected mInspectErrorFilePath As String = ""

    Protected m_isParallelInspect As Boolean

    Protected mInspectSearchLogFilePath As String = "InspectSearchLog.txt"      ' This value gets updated in function RunTool
    Protected mInspectSearchLogMostRecentEntry As String = String.Empty

    Protected mInspectConsoleOutputFilePath As String

    Protected WithEvents mSearchLogFileWatcher As FileSystemWatcher
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
    ''' Runs InSpecT tool
    ''' </summary>
    ''' <returns>CloseOutType enum indicating success or failure</returns>
    ''' <remarks></remarks>
    Public Overrides Function RunTool() As CloseOutType
        Dim result As CloseOutType

        Dim InspectDir As String
        Dim OrgDbDir As String

        Dim strBaseFilePath As String
        Dim objIndexedDBCreator As New clsCreateInspectIndexedDB

        Dim strFileNameAdder As String
        Dim strParallelizedText As String

        Try
            'Call base class for initial setup
            If Not MyBase.RunTool = CloseOutType.CLOSEOUT_SUCCESS Then
                Return CloseOutType.CLOSEOUT_FAILED
            End If

            If m_DebugLevel > 4 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerIN.RunTool(): Enter")
            End If

            InspectDir = m_mgrParams.GetParam("inspectdir")
            OrgDbDir = m_mgrParams.GetParam("orgdbdir")

            ' Store the Inspect version info in the database
            If Not StoreToolVersionInfo(InspectDir) Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Aborting since StoreToolVersionInfo returned false")
                m_message = "Error determining Inspect version"
                Return CloseOutType.CLOSEOUT_FAILED
            End If

            If m_DebugLevel >= 3 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Indexing Fasta file to create .trie file")
            End If

            ' Index the fasta file to create the .trie file
            result = objIndexedDBCreator.CreateIndexedDbFiles(m_mgrParams, m_jobParams, m_DebugLevel, m_JobNum, InspectDir, OrgDbDir)
            If result <> CloseOutType.CLOSEOUT_SUCCESS Then
                Return result
            End If

            'Determine if this is a parallelized job
            m_CloneStepRenum = m_jobParams.GetParam("CloneStepRenumberStart")
            m_StepNum = m_jobParams.GetParam("Step")
            strBaseFilePath = Path.Combine(m_WorkDir, m_Dataset)

            'Determine if this is parallelized inspect job
            If String.IsNullOrEmpty(m_CloneStepRenum) Then
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
            mInspectSearchLogFilePath = Path.Combine(m_WorkDir, "InspectSearchLog" & strFileNameAdder & ".txt")
            mInspectConsoleOutputFilePath = Path.Combine(m_WorkDir, "InspectConsoleOutput" & strFileNameAdder & ".txt")

            ' Make sure the _DTA.txt file is valid
            If Not ValidateCDTAFile(mInspectConcatenatedDtaFilePath) Then
                Return CloseOutType.CLOSEOUT_NO_DTA_FILES
            End If

            If m_DebugLevel >= 3 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Running " & strParallelizedText & " inspect on " & Path.GetFileName(mInspectConcatenatedDtaFilePath))
            End If

            result = RunInSpecT(InspectDir)
            If result <> CloseOutType.CLOSEOUT_SUCCESS Then
                Return result
            End If

            'If not a parallelized job, then zip the _Inspect.txt file
            If Not m_isParallelInspect Then
                'Zip the output file
                Dim blnSuccess As Boolean
                blnSuccess = MyBase.ZipFile(mInspectResultsFilePath, True)
                If Not blnSuccess Then
                    Return CloseOutType.CLOSEOUT_FAILED
                End If
            End If

            'Stop the job timer
            m_StopTime = DateTime.UtcNow

            'Add the current job data to the summary file
            If Not UpdateSummaryFile() Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN, "Error creating summary file, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step"))
            End If

            'Make sure objects are released
            Thread.Sleep(500)        ' 500 msec delay
            clsProgRunner.GarbageCollectNow()

            result = MakeResultsFolder()
            If result <> CloseOutType.CLOSEOUT_SUCCESS Then
                'MakeResultsFolder handles posting to local log, so set database error message and exit
                m_message = "Error making results folder"
                Return CloseOutType.CLOSEOUT_FAILED
            End If

            result = MoveResultFiles()
            If result <> CloseOutType.CLOSEOUT_SUCCESS Then
                'MoveResultFiles moves the result files to the result folder
                m_message = "Error moving files into results folder"
                Return CloseOutType.CLOSEOUT_FAILED
            End If

            result = CopyResultsFolderToServer()
            If result <> CloseOutType.CLOSEOUT_SUCCESS Then
                'TODO: What do we do here?
                ' Note that CopyResultsFolderToServer should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
                Return result
            End If

        Catch ex As Exception
            m_message = "Error in InspectPlugin->RunTool: " & ex.Message
            Return CloseOutType.CLOSEOUT_FAILED
        End Try

        Return CloseOutType.CLOSEOUT_SUCCESS 'No failures so everything must have succeeded

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
        Dim dbFilePath As String
        Dim inputFilename As String
        Dim strInputSpectra As String

        Dim blnUseShuffledDB As Boolean

        Try
            ParamFilename = Path.Combine(m_WorkDir, m_jobParams.GetParam("parmFileName"))
            orgDbDir = m_mgrParams.GetParam("orgdbdir")
            fastaFilename = m_jobParams.GetParam("PeptideSearch", "generatedFastaName")
            inputFilename = Path.Combine(m_WorkDir, INSPECT_INPUT_PARAMS_FILENAME)
            strInputSpectra = String.Empty

            blnUseShuffledDB = m_jobParams.GetJobParameter("InspectUsesShuffledDB", False)

            If blnUseShuffledDB Then
                ' Using shuffled version of the .trie file
                ' The Pvalue.py script does much better at computing p-values if a decoy search is performed (i.e. shuffleDB.py is used)
                ' Note that shuffleDB will add a prefix of XXX to the shuffled protein names
                dbFilePath = Path.GetFileNameWithoutExtension(fastaFilename) & "_shuffle.trie"
            Else
                dbFilePath = Path.GetFileNameWithoutExtension(fastaFilename) & ".trie"
            End If

            dbFilePath = Path.Combine(orgDbDir, dbFilePath)

            'add extra lines to the parameter files
            'the parameter file will become the input file for inspect
            Dim swInspectInputFile As StreamWriter
            swInspectInputFile = New StreamWriter((New FileStream(inputFilename, FileMode.Create, FileAccess.Write, FileShare.Read)))

            ' Create an instance of StreamReader to read from a file.
            Dim srInputBase As StreamReader
            srInputBase = New StreamReader((New FileStream(ParamFilename, FileMode.Open, FileAccess.Read, FileShare.Read)))

            Dim strParamLine As String

            swInspectInputFile.WriteLine("#Use the following to define the name of the log file created by Inspect (default is InspectSearchLog.txt if not defined)")
            swInspectInputFile.WriteLine("SearchLogFileName," & mInspectSearchLogFilePath)
            swInspectInputFile.WriteLine()

            swInspectInputFile.WriteLine("#Spectrum file to search; preferred formats are .mzXML and .mgf")

            'The code below was commented out since we are only supporting dta files.
            ''Dim mzXMLFilename As String
            ''mzXMLFilename = System.IO.Path.Combine(m_WorkDir, m_Dataset & ".mzXML")
            ''If m_jobParams.GetJobParameter("UseMzXML", False) Then
            '         '    strInputSpectra = String.Copy(mzXMLFilename)
            ''Else

            strInputSpectra = String.Copy(mInspectConcatenatedDtaFilePath)
            'End If

            If m_DebugLevel >= 3 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Inspect input spectra: " & strInputSpectra)
            End If

            swInspectInputFile.WriteLine("spectra," & strInputSpectra)
            swInspectInputFile.WriteLine()

            swInspectInputFile.WriteLine("#Note: The fully qualified database (.trie file) filename")
            swInspectInputFile.WriteLine("DB," & dbFilePath)

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
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Using DB '" & dbFilePath & "' and input spectra '" & strInputSpectra & "'")
            End If

        Catch ex As Exception
            ' Let the user know what went wrong.
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerIN.BuildInspectInputFile-> error while writing file: " & ex.Message)
            Return String.Empty
        End Try

        Return inputFilename

    End Function

    ' Unused function
    ''Private Function ExtractScanCountValueFromMzXML(ByVal strMZXMLFilename As String) As Integer

    ''    Dim intScanCount As Integer

    ''    Dim objMZXmlFile As MSDataFileReader.clsMzXMLFileReader
    ''    Dim objSpectrumInfo As MSDataFileReader.clsSpectrumInfo

    ''    Try
    ''        objMZXmlFile = New MSDataFileReader.clsMzXMLFileReader()

    ''        ' Open the file
    ''        objMZXmlFile.OpenFile(strMZXMLFilename)

    ''        ' Read the first spectrum (required to determine the ScanCount)
    ''        If objMZXmlFile.ReadNextSpectrum(objSpectrumInfo) Then
    ''            intScanCount = objMZXmlFile.ScanCount
    ''        End If

    ''        If Not objMZXmlFile Is Nothing Then
    ''            objMZXmlFile.CloseFile()
    ''        End If

    ''    Catch ex As Exception
    ''        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerIN.ExtractScanCountValueFromMzXML, Error determining the scan count in the .mzXML file: " & ex.Message)
    ''        Return 0
    ''    End Try

    ''    Return intScanCount

    ''End Function

    ''' <summary>
    ''' Run -p threshold value
    ''' </summary>
    ''' <returns>Value as a string or empty string means failure</returns>
    ''' <remarks></remarks>
    Private Function getPthresh() As String
        Dim defPvalThresh = "0.1"
        Dim tmpPvalThresh = m_mgrParams.GetParam("InspectPvalueThreshold")
        If tmpPvalThresh <> "" Then
            Return tmpPvalThresh 'return pValueThreshold value in settings file
        Else
            Return defPvalThresh 'if not found, return default of 0.1
        End If

    End Function

    Private Sub InitializeInspectSearchLogFileWatcher(strWorkDir As String)

        mSearchLogFileWatcher = New FileSystemWatcher()
        With mSearchLogFileWatcher
            .BeginInit()
            .Path = strWorkDir
            .IncludeSubdirectories = False
            .Filter = Path.GetFileName(mInspectSearchLogFilePath)
            .NotifyFilter = NotifyFilters.LastWrite Or NotifyFilters.Size
            .EndInit()
            .EnableRaisingEvents = True
        End With

    End Sub

    ''' <summary>
    ''' Looks for the inspect _errors.txt file in the working folder.  If present, reads and parses it
    ''' </summary>
    ''' <param name="errorFilename"></param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Protected Function ParseInspectErrorsFile(errorFilename As String) As Boolean

        Dim srInFile As StreamReader

        Dim strInputFilePath As String
        Dim strLineIn As String = String.Empty

        Dim htMessages As Hashtable

        Try

            If m_DebugLevel > 4 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerIN.ParseInspectErrorsFile(): Reading " & errorFilename)
            End If

            strInputFilePath = Path.Combine(m_WorkDir, errorFilename)

            If Not File.Exists(strInputFilePath) Then
                ' File not found; that means no errors occurred
                Return True
            Else
                Dim fi As FileInfo
                fi = New FileInfo(errorFilename)

                If fi.Length = 0 Then
                    ' Error file is 0 bytes, which means no errors occurred 
                    ' Delete the file
                    File.Delete(errorFilename)
                    Return True
                End If
            End If

            ' Initialize htMessages
            htMessages = New Hashtable

            ' Read the contents of strInputFilePath
            srInFile = New StreamReader(New FileStream(strInputFilePath, FileMode.Open, FileAccess.Read, FileShare.Read))

            Do While Not srInFile.EndOfStream
                strLineIn = srInFile.ReadLine

                If strLineIn Is Nothing Then Continue Do

                strLineIn = strLineIn.Trim

                If strLineIn.Length > 0 Then
                    If Not htMessages.Contains(strLineIn) Then
                        htMessages.Add(strLineIn, 1)
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Inspect warning/error: " & strLineIn)
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
    Private Function RunInSpecT(InspectDir As String) As CloseOutType
        Dim CmdStr As String
        Dim ParamFilePath As String = Path.Combine(m_WorkDir, m_jobParams.GetParam("parmFileName"))
        Dim blnSuccess As Boolean = False

        ' Build the Inspect Input Parameters file
        mInspectCustomParamFileName = BuildInspectInputFile()
        If mInspectCustomParamFileName.Length = 0 Then
            Return CloseOutType.CLOSEOUT_FAILED
        End If

        mCmdRunner = New clsRunDosProgram(InspectDir)
        RegisterEvents(mCmdRunner)
        AddHandler mCmdRunner.LoopWaiting, AddressOf CmdRunner_LoopWaiting

        If m_DebugLevel > 4 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerIN.RunInSpecT(): Enter")
        End If

        ' verify that program file exists
        Dim progLoc As String = Path.Combine(InspectDir, INSPECT_EXE_NAME)
        If Not File.Exists(progLoc) Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Cannot find Inspect program file: " & progLoc)
            Return CloseOutType.CLOSEOUT_FAILED
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
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, progLoc & " " & CmdStr)
        End If

        With mCmdRunner
            .CreateNoWindow = True
            .CacheStandardOutput = True
            .EchoOutputToConsole = True

            .WriteConsoleOutputToFile = True
            .ConsoleOutputFilePath = mInspectConsoleOutputFilePath
        End With

        If Not mCmdRunner.RunProgram(progLoc, CmdStr, "Inspect", True) Then

            If mCmdRunner.ExitCode <> 0 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Inspect returned a non-zero exit code: " & mCmdRunner.ExitCode.ToString)
            Else
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Call to Inspect failed (but exit code is 0)")
            End If

            Select Case mCmdRunner.ExitCode
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

            If mCmdRunner.ExitCode <> 0 Then
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
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error running Inspect : " & m_JobNum)
        Else
            m_progress = 100
            UpdateStatusRunning()
        End If

        If Not mSearchLogFileWatcher Is Nothing Then
            mSearchLogFileWatcher.EnableRaisingEvents = False
            mSearchLogFileWatcher = Nothing
        End If

        ' Parse the _errors.txt file (if it exists) and copy any errors to the analysis manager log
        ParseInspectErrorsFile(mInspectErrorFilePath)

        'even though success is returned, check for the result file
        If File.Exists(mInspectResultsFilePath) Then
            blnSuccess = True
        Else
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Inspect results file not found; job failed: " & Path.GetFileName(mInspectResultsFilePath))
            blnSuccess = False
        End If

        If blnSuccess Then
            Return CloseOutType.CLOSEOUT_SUCCESS
        Else
            Return CloseOutType.CLOSEOUT_FAILED
        End If


    End Function

    ''' <summary>
    ''' Event handler for CmdRunner.LoopWaiting event
    ''' </summary>
    ''' <remarks></remarks>
    Private Sub CmdRunner_LoopWaiting()
        Static dtLastStatusUpdate As DateTime = DateTime.UtcNow

        ' Update the status file (limit the updates to every 5 seconds)
        If DateTime.UtcNow.Subtract(dtLastStatusUpdate).TotalSeconds >= 5 Then
            dtLastStatusUpdate = DateTime.UtcNow
            UpdateStatusRunning(m_progress, m_DtaCount)
        End If

        LogProgress("Inspect")

    End Sub

    Private Sub ParseInspectSearchLogFile(strSearchLogFilePath As String)
        Dim strLineIn As String = String.Empty
        Dim strLastEntry As String = String.Empty
        Dim strSplitline() As String
        Dim strProgress As String

        Try
            Dim ioFile = New FileInfo(strSearchLogFilePath)
            If ioFile.Exists AndAlso ioFile.Length > 0 Then
                ' Search log file has been updated
                ' Open the file and read the contents

                Using srLogFile = New StreamReader(New FileStream(strSearchLogFilePath, FileMode.Open, FileAccess.Read, FileShare.Write))

                    ' Read to the end of the file
                    Do While Not srLogFile.EndOfStream
                        strLineIn = srLogFile.ReadLine()

                        If Not String.IsNullOrEmpty(strLineIn) Then
                            strLastEntry = String.Copy(strLineIn)
                        End If
                    Loop

                End Using

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
    ''' Stores the tool version info in the database
    ''' </summary>
    ''' <remarks></remarks>
    Protected Function StoreToolVersionInfo(strInspectFolder As String) As Boolean

        Dim strToolVersionInfo As String = String.Empty

        If m_DebugLevel >= 2 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info")
        End If

        ' Store paths to key files in ioToolFiles
        Dim ioToolFiles As New List(Of FileInfo)
        ioToolFiles.Add(New FileInfo(Path.Combine(strInspectFolder, INSPECT_EXE_NAME)))

        Try
            Return MyBase.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles)
        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion: " & ex.Message)
            Return False
        End Try

    End Function

    ''' <summary>
    ''' Event handler for mSearchLogFileWatcher.Changed event
    ''' </summary>
    ''' <param name="sender"></param>
    ''' <param name="e"></param>
    ''' <remarks></remarks>
    Private Sub mSearchLogFileWatcher_Changed(sender As Object, e As FileSystemEventArgs) Handles mSearchLogFileWatcher.Changed
        ParseInspectSearchLogFile(mInspectSearchLogFilePath)
    End Sub

#End Region

End Class
