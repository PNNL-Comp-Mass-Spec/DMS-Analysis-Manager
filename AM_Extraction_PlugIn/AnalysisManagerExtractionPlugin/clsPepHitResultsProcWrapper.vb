'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2007, Battelle Memorial Institute
' Created 07/11/2007
'
' Program converted from original version written by J.D. Sandoval, PNNL.
' Conversion performed as part of upgrade to VB.Net 2005, modification for use with manager and broker databases
'
' Last modified 01/07/2009
' Last modified 06/15/2009 JDS - Added logging using log4net
'*********************************************************************************************************

Imports System.IO
Imports AnalysisManagerBase

Public Class clsPepHitResultsProcWrapper

	'*********************************************************************************************************
    ' Calls PeptideHitResultsProcRunner.exe
	'*********************************************************************************************************

#Region "Module Variables"
    Private m_DebugLevel As Integer = 0
    Private m_MgrParams As IMgrParams
    Private m_JobParams As IJobParams

    Private m_Progress As Integer = 0
    Private m_ErrMsg As String = String.Empty
    Private m_PHRPConsoleOutputFilePath As String

    '' Old/unused
    '' Private WithEvents mPeptideHitResultsProcessor As PeptideHitResultsProcessor.IPeptideHitResultsProcessor

    Protected WithEvents CmdRunner As clsRunDosProgram

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
    Public Event ProgressChanged(ByVal taskDescription As String, ByVal percentComplete As Single)
#End Region

#Region "Methods"
    ''' <summary>
    ''' Constructor
    ''' </summary>
    ''' <param name="MgrParams">IMgrParams object containing manager settings</param>
    ''' <param name="JobParams">IJobParams object containing job parameters</param>
    ''' <remarks></remarks>
    Public Sub New(ByVal MgrParams As IMgrParams, ByVal JobParams As IJobParams)

        m_MgrParams = MgrParams
        m_JobParams = JobParams
        m_DebugLevel = CInt(m_MgrParams.GetParam("debuglevel"))

    End Sub

    ''' <summary>
    ''' Converts Sequest, X!Tandem, or Inspect output file to a flat file
    ''' </summary>
    ''' <returns>IJobParams.CloseOutType enum indicating success or failure</returns>
    ''' <remarks></remarks>
    Public Function ExtractDataFromResults(ByVal PeptideSearchResultsFileName As String) As IJobParams.CloseOutType
        '  Let the DLL auto-determines the input filename, based on the dataset name
        Return ExtractDataFromResults(PeptideSearchResultsFileName, True, True)
    End Function

    ''' <summary>
    ''' Converts Sequest, X!Tandem, or Inspect output file to a flat file
    ''' </summary>
    ''' <returns>IJobParams.CloseOutType enum indicating success or failure</returns>
    ''' <remarks></remarks>
    Public Function ExtractDataFromResults(ByVal PeptideSearchResultsFileName As String, _
                                           ByVal CreateInspectFirstHitsFile As Boolean, _
                                           ByVal CreateInspectSynopsisFile As Boolean) As IJobParams.CloseOutType

        Dim result As IJobParams.CloseOutType

        result = MakeTextOutputFiles(PeptideSearchResultsFileName, CreateInspectFirstHitsFile, CreateInspectSynopsisFile)
        If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            Return result
        End If

        Return result

    End Function


    ''' <summary>
    ''' Makes flat text file from PeptideSearchResultsFileName
    ''' </summary>
    ''' <returns>IJobParams.CloseOutType enum indicating success or failure</returns>
    ''' <remarks></remarks>
    Private Function MakeTextOutputFiles(ByVal PeptideSearchResultsFileName As String, _
                                         ByVal CreateInspectFirstHitsFile As Boolean, _
                                         ByVal CreateInspectSynopsisFile As Boolean) As IJobParams.CloseOutType

        Dim ModDefsFileName As String
        Dim ParamFileName As String = m_JobParams.GetParam("ParmFileName")

        Dim ioInputFile As System.IO.FileInfo

        Dim CmdStr As String

        Try
            m_Progress = 0
            m_ErrMsg = String.Empty

            If String.IsNullOrWhiteSpace(PeptideSearchResultsFileName) Then
                m_ErrMsg = "PeptideSearchResultsFileName is empty; unable to continue"
                Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
            End If

            ' Define the modification definitions file name
            ModDefsFileName = System.IO.Path.GetFileNameWithoutExtension(ParamFileName) & clsAnalysisResourcesExtraction.MOD_DEFS_FILE_SUFFIX

            ioInputFile = New System.IO.FileInfo(PeptideSearchResultsFileName)
            m_PHRPConsoleOutputFilePath = System.IO.Path.Combine(ioInputFile.DirectoryName, "PHRPOutput.txt")

            CmdRunner = New clsRunDosProgram(ioInputFile.DirectoryName)

            Dim progLoc As String = m_MgrParams.GetParam("PHRPProgLoc")
            progLoc = System.IO.Path.Combine(progLoc, "PeptideHitResultsProcRunner.exe")

            ' verify that program file exists
            If Not System.IO.File.Exists(progLoc) Then
                m_ErrMsg = "PHRP not found at " & progLoc
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            ' Set up and execute a program runner to run the PHRP
            ' Note that /SynPvalue is only used when processing Inspect files
            CmdStr = ioInputFile.FullName & _
                     " /O:" & ioInputFile.DirectoryName & _
                     " /M:" & ModDefsFileName & _
                     " /T:" & clsAnalysisResourcesExtraction.MASS_CORRECTION_TAGS_FILENAME & _
                     " /N:" & ParamFileName & _
                     " /SynPvalue:0.2"

            ' Note that PHRP assumes /InsFHT=True and /InsSyn=True by default
            ' Thus, we only need to use these switches if either or these should be false
            If Not CreateInspectFirstHitsFile Or Not CreateInspectSynopsisFile Then
                CmdStr &= " /InsFHT:" & CreateInspectFirstHitsFile.ToString()
                CmdStr &= " /InsSyn:" & CreateInspectSynopsisFile.ToString()
            End If

            If m_DebugLevel >= 2 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, progLoc & " " & CmdStr)
            End If

            With CmdRunner
                .CreateNoWindow = True
                .CacheStandardOutput = True
                .EchoOutputToConsole = True

                .WriteConsoleOutputToFile = True
                .ConsoleOutputFilePath = m_PHRPConsoleOutputFilePath
            End With

            If Not CmdRunner.RunProgram(progLoc, CmdStr, "PHRP", True) Then
                m_ErrMsg = "Error running PHRP"
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If


            If CmdRunner.ExitCode <> 0 Then
                m_ErrMsg = "PHRP runner returned a non-zero error code: " & CmdRunner.ExitCode.ToString

                ' Parse the console output file for any lines that contain "Error"
                ' Append them to m_ErrMsg

                Dim ioConsoleOutputFile As System.IO.FileInfo = New System.IO.FileInfo(m_PHRPConsoleOutputFilePath)
                Dim blnErrorMessageFound As Boolean = False

                If ioConsoleOutputFile.Exists Then
                    Dim srInFile As System.IO.StreamReader
                    srInFile = New System.IO.StreamReader(New System.IO.FileStream(ioConsoleOutputFile.FullName, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.ReadWrite))

                    Do While srInFile.Peek() >= 0
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

                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            Else
                ' Make sure the key PHRP result files were created
                Dim lstFilesToCheck As System.Collections.Generic.List(Of String)
                lstFilesToCheck = New System.Collections.Generic.List(Of String)

                If CreateInspectFirstHitsFile And Not CreateInspectSynopsisFile Then
                    ' We're processing Inspect data, and PHRP simply created the _fht.txt file
                    ' Thus, only look for the first-hits file
                    lstFilesToCheck.Add("_fht.txt")
                Else
                    lstFilesToCheck.Add("_ResultToSeqMap.txt")
                    lstFilesToCheck.Add("_SeqInfo.txt")
                    lstFilesToCheck.Add("_SeqToProteinMap.txt")
                    lstFilesToCheck.Add("_ModSummary.txt")
                    lstFilesToCheck.Add("_ModDetails.txt")
                End If

                For Each strFileName As String In lstFilesToCheck
                    If ioInputFile.Directory.GetFiles("*" & strFileName).Length = 0 Then
                        m_ErrMsg = "PHRP results file not found: " & strFileName
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_ErrMsg)
                        Return IJobParams.CloseOutType.CLOSEOUT_FAILED
                    End If
                Next
            End If

            ' Delete strPHRPConsoleOutputFilePath, since we didn't encounter any errors and the file is typically not useful
            Try
                System.IO.File.Delete(m_PHRPConsoleOutputFilePath)
            Catch ex As Exception
                ' Ignore errors here
            End Try

        Catch ex As System.Exception
            Dim Msg As String
            Msg = "Exception while running the peptide hit results processor: " & _
             ex.Message & "; " & clsGlobal.GetExceptionStackTrace(ex)
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        If m_DebugLevel >= 3 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Peptide hit results processor complete")
        End If

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    Private Sub ParsePHRPConsoleOutputFile()
        Static reProcessing As System.Text.RegularExpressions.Regex = New System.Text.RegularExpressions.Regex("Processing: (\d+)")

        Try
            Dim srInFile As System.IO.StreamReader
            Dim strLineIn As String
            Dim reMatch As System.Text.RegularExpressions.Match
            Dim intProgress As Integer = 0

            If System.IO.File.Exists(m_PHRPConsoleOutputFilePath) Then
                srInFile = New System.IO.StreamReader(New System.IO.FileStream(m_PHRPConsoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))

                Do While srInFile.Peek >= 0
                    strLineIn = srInFile.ReadLine()
                    reMatch = reProcessing.Match(strLineIn)

                    If reMatch.Success Then
                        If Integer.TryParse(reMatch.Groups.Item(1).Value, intProgress) Then
                            ' Success parsing out the progress
                        End If
                    End If
                Loop

                srInFile.Close()

                If intProgress > m_Progress Then
                    m_Progress = intProgress
                    RaiseEvent ProgressChanged("Running PHRP", m_Progress)
                End If
            End If

        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error parsing PHRP Console Output File", ex)
        End Try

    End Sub

#End Region

#Region "Event Handlers"



    ''' <summary>
    ''' Event handler for CmdRunner.LoopWaiting event
    ''' </summary>
    ''' <remarks></remarks>
    Private Sub CmdRunner_LoopWaiting() Handles CmdRunner.LoopWaiting
        Static dtLastStatusUpdate As System.DateTime = System.DateTime.UtcNow

        'Update the status by parsing the PHRP Console Output file every 20 seconds
        If System.DateTime.UtcNow.Subtract(dtLastStatusUpdate).TotalSeconds >= 20 Then
            dtLastStatusUpdate = System.DateTime.UtcNow
            ParsePHRPConsoleOutputFile()
        End If

    End Sub

#End Region
End Class
