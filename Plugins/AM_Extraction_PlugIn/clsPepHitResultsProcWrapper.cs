'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2007, Battelle Memorial Institute
' Created 07/11/2007
'
' Program converted from original version written by J.D. Sandoval, PNNL.
' Conversion performed as part of upgrade to VB.Net 2005, modification for use with manager and broker databases
'
'*********************************************************************************************************

Imports AnalysisManagerBase
Imports System.IO
Imports System.Text.RegularExpressions

''' <summary>
''' Calls PeptideHitResultsProcRunner.exe
''' </summary>
''' <remarks></remarks>
Public Class clsPepHitResultsProcWrapper

#Region "Constants"

    Public Const PHRP_LOG_FILE_NAME As String = "PHRP_LogFile.txt"

#End Region

#Region "Module Variables"
    Private ReadOnly m_DebugLevel As Integer = 0
    Private ReadOnly m_MgrParams As IMgrParams
    Private ReadOnly m_JobParams As IJobParams

    Private m_Progress As Integer = 0
    Private m_ErrMsg As String = String.Empty
    Private m_PHRPConsoleOutputFilePath As String

    ' This list tracks the error messages reported by CmdRunner
    Protected mCmdRunnerErrors As Concurrent.ConcurrentBag(Of String)

#End Region

#Region "Properties"
    Public ReadOnly Property ErrMsg() As String
        Get
            If m_ErrMsg Is Nothing Then
                Return String.Empty
            Else
                Return m_ErrMsg
            End If
        End Get
    End Property
#End Region

#Region "Events"
    Public Event ProgressChanged(taskDescription As String, percentComplete As Single)
#End Region

#Region "Methods"
    ''' <summary>
    ''' Constructor
    ''' </summary>
    ''' <param name="MgrParams">IMgrParams object containing manager settings</param>
    ''' <param name="JobParams">IJobParams object containing job parameters</param>
    ''' <remarks></remarks>
    Public Sub New(MgrParams As IMgrParams, JobParams As IJobParams)

        m_MgrParams = MgrParams
        m_JobParams = JobParams
        m_DebugLevel = m_MgrParams.GetParam("debuglevel", 1)

        mCmdRunnerErrors = New Concurrent.ConcurrentBag(Of String)
    End Sub

    ''' <summary>
    ''' Converts Sequest, X!Tandem, Inspect, MSGDB, or MSAlign output file to a flat file
    ''' </summary>
    ''' <returns>CloseOutType enum indicating success or failure</returns>
    ''' <remarks></remarks>
    Public Function ExtractDataFromResults(peptideSearchResultsFileName As String, fastaFilePath As String, resultType As String) As CloseOutType
        '  Let the DLL auto-determines the input filename, based on the dataset name
        Return ExtractDataFromResults(peptideSearchResultsFileName, True, True, fastaFilePath, resultType)
    End Function

    ''' <summary>
    ''' Converts Sequest, X!Tandem, Inspect, MSGF+, MSAlign, MODa, or MODPlus output file to a flat file
    ''' </summary>
    ''' <returns>CloseOutType enum indicating success or failure</returns>
    ''' <remarks></remarks>
    Public Function ExtractDataFromResults(
      peptideSearchResultsFileName As String,
      createFirstHitsFile As Boolean,
      createSynopsisFile As Boolean,
      fastaFilePath As String,
      resultType As String) As CloseOutType

        Dim ModDefsFileName As String
        Dim ParamFileName As String = m_JobParams.GetParam("ParmFileName")

        Dim ioInputFile As FileInfo

        Dim cmdStr As String
        Dim blnSuccess As Boolean

        Try
            m_Progress = 0
            m_ErrMsg = String.Empty

            If String.IsNullOrWhiteSpace(peptideSearchResultsFileName) Then
                m_ErrMsg = "PeptideSearchResultsFileName is empty; unable to continue"
                Return CloseOutType.CLOSEOUT_FILE_NOT_FOUND
            End If

            ' Define the modification definitions file name
            ModDefsFileName = Path.GetFileNameWithoutExtension(ParamFileName) & clsAnalysisResourcesExtraction.MOD_DEFS_FILE_SUFFIX

            ioInputFile = New FileInfo(peptideSearchResultsFileName)
            m_PHRPConsoleOutputFilePath = Path.Combine(ioInputFile.DirectoryName, "PHRPOutput.txt")

            Dim progLoc As String = m_MgrParams.GetParam("PHRPProgLoc")
            progLoc = Path.Combine(progLoc, "PeptideHitResultsProcRunner.exe")

            ' Verify that program file exists
            If Not File.Exists(progLoc) Then
                m_ErrMsg = "PHRP not found at " & progLoc
                Return CloseOutType.CLOSEOUT_FAILED
            End If

            ' Set up and execute a program runner to run the PHRP
            ' Note: 
            '   /SynPvalue is only used when processing Inspect files
            '   /SynProb is only used for MODa and MODPlus results
            cmdStr = ioInputFile.FullName &
            " /O:" & ioInputFile.DirectoryName &
            " /M:" & ModDefsFileName &
            " /T:" & clsAnalysisResourcesExtraction.MASS_CORRECTION_TAGS_FILENAME &
            " /N:" & ParamFileName &
            " /SynPvalue:0.2 " &
            " /SynProb:0.05 "

            cmdStr &= " /L:" & Path.Combine(ioInputFile.DirectoryName, PHRP_LOG_FILE_NAME)

            Dim blnSkipProteinMods = m_JobParams.GetJobParameter("SkipProteinMods", False)
            If Not blnSkipProteinMods Then
                cmdStr &= " /ProteinMods"
            End If

            If Not String.IsNullOrEmpty(fastaFilePath) Then
                ' Note that FastaFilePath will likely be empty if job parameter SkipProteinMods is true
                cmdStr &= " /F:" & clsAnalysisToolRunnerBase.PossiblyQuotePath(fastaFilePath)
            End If

            ' Note that PHRP assumes /InsFHT=True and /InsSyn=True by default
            ' Thus, we only need to use these switches if either of these should be false
            If Not createFirstHitsFile Or Not createSynopsisFile Then
                cmdStr &= " /InsFHT:" & createFirstHitsFile.ToString()
                cmdStr &= " /InsSyn:" & createSynopsisFile.ToString()
            End If

            ' PHRP defaults to use /MSGFPlusSpecEValue:5E-7  and  /MSGFPlusEValue:0.75
            ' Adjust these if defined in the job parameters
            Dim msgfPlusSpecEValue = m_JobParams.GetJobParameter("MSGFPlusSpecEValue", "")
            Dim msgfPlusEValue = m_JobParams.GetJobParameter("MSGFPlusEValue", "")

            If Not String.IsNullOrEmpty(msgfPlusSpecEValue) Then
                cmdStr &= " /MSGFPlusSpecEValue:" & msgfPlusSpecEValue
            End If

            If Not String.IsNullOrEmpty(msgfPlusEValue) Then
                cmdStr &= " /MSGFPlusEValue:" & msgfPlusEValue
            End If

            If m_DebugLevel >= 1 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, progLoc & " " & CmdStr)
            End If

            Dim cmdRunner = New clsRunDosProgram(ioInputFile.DirectoryName) With {
                .CreateNoWindow = True,
                .CacheStandardOutput = True,
                .EchoOutputToConsole = True,
                .WriteConsoleOutputToFile = True,
                .ConsoleOutputFilePath = m_PHRPConsoleOutputFilePath
            }
            AddHandler cmdRunner.LoopWaiting, AddressOf CmdRunner_LoopWaiting
            AddHandler cmdRunner.ErrorEvent, AddressOf CmdRunner_ErrorEvent

            ' Abort PHRP if it runs for over 720 minutes (this generally indicates that it's stuck)
            Const intMaxRuntimeSeconds As Integer = 720 * 60
            blnSuccess = cmdRunner.RunProgram(progLoc, cmdStr, "PHRP", True, intMaxRuntimeSeconds)

            If mCmdRunnerErrors.Count > 0 Then
                ' Append the error messages to the log
                ' Note that clsProgRunner will have already included them in the ConsoleOutput.txt file
                For Each strError As String In mCmdRunnerErrors
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "... " & strError)
                Next
            End If

            If Not blnSuccess Then
                m_ErrMsg = "Error running PHRP"
                Return CloseOutType.CLOSEOUT_FAILED
            End If

            If cmdRunner.ExitCode <> 0 Then
                m_ErrMsg = "PHRP runner returned a non-zero error code: " & cmdRunner.ExitCode.ToString

                ' Parse the console output file for any lines that contain "Error"
                ' Append them to m_ErrMsg

                Dim ioConsoleOutputFile = New FileInfo(m_PHRPConsoleOutputFilePath)
                Dim blnErrorMessageFound = False

                If ioConsoleOutputFile.Exists Then
                    Dim srInFile As StreamReader
                    srInFile = New StreamReader(New FileStream(ioConsoleOutputFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))

                    Do While Not srInFile.EndOfStream
                        Dim strLineIn As String
                        strLineIn = srInFile.ReadLine()
                        If Not String.IsNullOrWhiteSpace(strLineIn) Then
                            If strLineIn.ToLower.Contains("error") Then
                                m_ErrMsg &= "; " & m_ErrMsg
                                blnErrorMessageFound = True
                            End If
                        End If
                    Loop
                    srInFile.Close()
                End If

                If Not blnErrorMessageFound Then
                    m_ErrMsg &= "; Unknown error message"
                End If

                Return CloseOutType.CLOSEOUT_FAILED
            Else
                ' Make sure the key PHRP result files were created
                Dim lstFilesToCheck As List(Of String)
                lstFilesToCheck = New List(Of String)

                If createFirstHitsFile And Not createSynopsisFile Then
                    ' We're processing Inspect data, and PHRP simply created the _fht.txt file
                    ' Thus, only look for the first-hits file
                    lstFilesToCheck.Add("_fht.txt")
                Else
                    lstFilesToCheck.Add("_ResultToSeqMap.txt")
                    lstFilesToCheck.Add("_SeqInfo.txt")
                    lstFilesToCheck.Add("_SeqToProteinMap.txt")
                    lstFilesToCheck.Add("_ModSummary.txt")
                    lstFilesToCheck.Add("_ModDetails.txt")

                    If Not blnSkipProteinMods Then
                        If Not String.IsNullOrEmpty(fastaFilePath) Then
                            Dim strWarningMessage As String = String.Empty

                            If PeptideHitResultsProcessor.clsPHRPBaseClass.ValidateProteinFastaFile(fastaFilePath, strWarningMessage) Then
                                lstFilesToCheck.Add("_ProteinMods.txt")
                            End If
                        ElseIf resultType = clsAnalysisResources.RESULT_TYPE_MSGFPLUS Then
                            lstFilesToCheck.Add("_ProteinMods.txt")
                        End If
                    End If

                End If

                For Each strFileName As String In lstFilesToCheck
                    If ioInputFile.Directory.GetFiles("*" & strFileName).Length = 0 Then
                        m_ErrMsg = "PHRP results file not found: " & strFileName
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_ErrMsg)
                        Return CloseOutType.CLOSEOUT_FAILED
                    End If
                Next
            End If

            ' Delete strPHRPConsoleOutputFilePath, since we didn't encounter any errors and the file is typically not useful
            Try
                File.Delete(m_PHRPConsoleOutputFilePath)
            Catch ex As Exception
                ' Ignore errors here
            End Try

        Catch ex As Exception
            Dim logMessage = "Exception while running the peptide hit results processor: " & ex.Message & "; " & clsGlobal.GetExceptionStackTrace(ex)
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, logMessage)
            Return CloseOutType.CLOSEOUT_FAILED
        End Try

        If m_DebugLevel >= 3 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Peptide hit results processor complete")
        End If

        Return CloseOutType.CLOSEOUT_SUCCESS

    End Function

    Private Sub ParsePHRPConsoleOutputFile()

        Const CREATING_FHT = 0
        Const CREATING_SYN = 10
        Const CREATING_PHRP_FILES = 20
        Const PHRP_COMPLETE = 100

        Static reProcessing As Regex = New Regex("Processing: (\d+)")
        Static reProcessingPHRP As Regex = New Regex("^([0-9.]+)\% complete")

        Try
            Dim reMatch As Text.RegularExpressions.Match

            Dim currentTaskProgressAtStart = CREATING_FHT
            Dim currentTaskProgressAtEnd = CREATING_SYN

            Dim progressSubtask As Single = 0

            If File.Exists(m_PHRPConsoleOutputFilePath) Then
                Using srInFile = New StreamReader(New FileStream(m_PHRPConsoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))

                    Do While Not srInFile.EndOfStream
                        Dim strLineIn = srInFile.ReadLine()
                        If String.IsNullOrWhiteSpace(strLineIn) Then
                            Continue Do
                        End If

                        If strLineIn.StartsWith("Creating the FHT file") Then
                            currentTaskProgressAtStart = CREATING_FHT
                            currentTaskProgressAtEnd = CREATING_SYN
                            progressSubtask = 0
                        ElseIf strLineIn.StartsWith("Creating the SYN file") Then
                            currentTaskProgressAtStart = CREATING_SYN
                            currentTaskProgressAtEnd = CREATING_PHRP_FILES
                            progressSubtask = 0
                        ElseIf strLineIn.StartsWith("Creating the PHRP files") Then
                            currentTaskProgressAtStart = CREATING_PHRP_FILES
                            currentTaskProgressAtEnd = PHRP_COMPLETE
                            progressSubtask = 0
                        End If

                        If currentTaskProgressAtStart < CREATING_PHRP_FILES Then
                            reMatch = reProcessing.Match(strLineIn)
                            If reMatch.Success Then
                                Single.TryParse(reMatch.Groups.Item(1).Value, progressSubtask)
                            End If
                        Else
                            reMatch = reProcessingPHRP.Match(strLineIn)
                            If reMatch.Success Then
                                Single.TryParse(reMatch.Groups.Item(1).Value, progressSubtask)
                            End If
                        End If

                    Loop

                End Using

                Dim progressOverall = clsAnalysisToolRunnerBase.ComputeIncrementalProgress(currentTaskProgressAtStart, currentTaskProgressAtEnd, progressSubtask)

                If progressOverall > m_Progress Then
                    m_Progress = CInt(progressOverall)
                    RaiseEvent ProgressChanged("Running PHRP", m_Progress)
                End If
            End If

        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error parsing PHRP Console Output File", ex)
        End Try

    End Sub

#End Region

#Region "Event Handlers"

    Private Sub CmdRunner_ErrorEvent(NewText As String, ex As Exception)

        If Not mCmdRunnerErrors Is Nothing Then
            ' Split NewText on newline characters
            Dim strSplitLine() As String
            Dim chNewLineChars = New Char() {ControlChars.Cr, ControlChars.Lf}

            strSplitLine = NewText.Split(chNewLineChars, StringSplitOptions.RemoveEmptyEntries)

            If Not strSplitLine Is Nothing Then
                For Each strItem As String In strSplitLine
                    strItem = strItem.Trim(chNewLineChars)
                    If Not String.IsNullOrEmpty(strItem) Then

                        mCmdRunnerErrors.Add(strItem)

                    End If
                Next
            End If

        End If

    End Sub

    ''' <summary>
    ''' Event handler for CmdRunner.LoopWaiting event
    ''' </summary>
    ''' <remarks></remarks>
    Private Sub CmdRunner_LoopWaiting()
        Static dtLastStatusUpdate As DateTime = Date.UtcNow

        'Update the status by parsing the PHRP Console Output file every 20 seconds
        If Date.UtcNow.Subtract(dtLastStatusUpdate).TotalSeconds >= 20 Then
            dtLastStatusUpdate = Date.UtcNow
            ParsePHRPConsoleOutputFile()
        End If

    End Sub

#End Region
End Class
