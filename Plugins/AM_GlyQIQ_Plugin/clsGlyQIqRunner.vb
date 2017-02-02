Imports AnalysisManagerBase
Imports System.IO
Imports PRISM.Processes
Imports System.Text.RegularExpressions

Public Class clsGlyQIqRunner

    Public Const GLYQ_IQ_CONSOLE_OUTPUT_PREFIX As String = "GlyQ-IQ_ConsoleOutput_Core"

#Region "Enums"

    Public Enum GlyQIqRunnerStatusCodes
        NotStarted = 0
        Running = 1
        Success = 2
        Failure = 3
    End Enum

#End Region

#Region "Events"
    Public Event CmdRunnerWaiting()
#End Region

#Region "Properties"
    
    Public ReadOnly Property BatchFilePath As String
        Get
            Return mBatchFilePath
        End Get
    End Property

    Public ReadOnly Property ConsoleOutputFilePath As String
        Get
            Return mConsoleOutputFilePath
        End Get
    End Property

    Public ReadOnly Property Core As Integer
        Get
            Return mCore
        End Get
    End Property

    ''' <summary>
    ''' Value between 0 and 100
    ''' </summary>
    ''' <remarks></remarks>
    Public ReadOnly Property Progress As Double
        Get
            Return mProgress
        End Get
    End Property


    Public ReadOnly Property ProgRunner As clsRunDosProgram
        Get
            Return mCmdRunner
        End Get
    End Property

    Public ReadOnly Property Status As GlyQIqRunnerStatusCodes
        Get
            Return mStatus
        End Get
    End Property

    Public ReadOnly Property ProgRunnerStatus As clsProgRunner.States
        Get
            If mCmdRunner Is Nothing Then
                Return clsProgRunner.States.NotMonitoring
            End If
            Return mCmdRunner.State
        End Get
    End Property

#End Region

#Region "Member Variables"

    Protected mBatchFilePath As String
    Protected mConsoleOutputFilePath As String
    Protected mCore As Integer
    Protected mProgress As Double

    ''' <summary>
    ''' Dictionary tracking target names, and True/False for whether the target has been reported as being searched in the GlyQ-IQ Console Output window
    ''' </summary>
    ''' <remarks></remarks>
    Protected mTargets As Dictionary(Of String, Boolean)

    Protected mStatus As GlyQIqRunnerStatusCodes

    Protected ReadOnly mWorkingDirectory As String

    Protected mCmdRunner As clsRunDosProgram

#End Region

    Public Sub New(ByVal workingDirectory As String, ByVal processingCore As Integer, batchFilePathToUse As String)
        mWorkingDirectory = workingDirectory
        mCore = processingCore
        mBatchFilePath = batchFilePathToUse
        mStatus = GlyQIqRunnerStatusCodes.NotStarted

        mTargets = New Dictionary(Of String, Boolean)

        CacheTargets()

    End Sub

    ''' <summary>
    ''' Forcibly ends GlyQ-IQ
    ''' </summary>
    ''' <remarks></remarks>
    Public Sub AbortProcessingNow()
        If Not mCmdRunner Is Nothing Then
            mCmdRunner.AbortProgramNow()
        End If
    End Sub

    Protected Sub CacheTargets()

        Dim fiBatchFile = New FileInfo(mBatchFilePath)
        If Not fiBatchFile.Exists Then
            Throw New FileNotFoundException("Batch file not found", mBatchFilePath)
        End If

        Try
            Dim fileContents = String.Empty

            Using srBatchFile = New StreamReader(New FileStream(fiBatchFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                If Not srBatchFile.EndOfStream Then
                    fileContents = srBatchFile.ReadLine
                End If
            End Using

            If String.IsNullOrWhiteSpace(fileContents) Then
                Throw New Exception("Batch file is empty, " + fiBatchFile.Name)
            End If

            ' Replace instances of " " with tab characters
            fileContents = fileContents.Replace("""" & " " & """", ControlChars.Tab)

            ' Replace any remaining double quotes with a tab character
            fileContents = fileContents.Replace("""", ControlChars.Tab)

            Dim parameterList = fileContents.Split(ControlChars.Tab)

            ' Remove any empty items
            Dim parameterListFiltered = (From item In parameterList Where Not String.IsNullOrWhiteSpace(item) Select item).ToList()

            If parameterListFiltered.Count < 6 Then
                Throw New Exception("Batch file arguments are not in the correct format")
            End If

            Dim targetsFileName = parameterListFiltered.Item(4)
            Dim workingParametersFolderPath = parameterListFiltered.Item(6)

            Dim diWorkingParameters = New DirectoryInfo(workingParametersFolderPath)
            If Not diWorkingParameters.Exists Then
                Throw New DirectoryNotFoundException("Folder not found, " & diWorkingParameters.FullName)
            End If

            Dim fiTargetsFile = New FileInfo(Path.Combine(diWorkingParameters.FullName, targetsFileName))
            If Not fiTargetsFile.Exists Then
                Throw New FileNotFoundException("Targets file not found, " & fiTargetsFile.FullName)
            End If

            Dim columnDelimiters() As Char = {ControlChars.Tab}
            Const CODE_COLUMN_INDEX As Integer = 2

            Using srTargetsFile = New StreamReader(New FileStream(fiTargetsFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))

                ' Read the header line
                If Not srTargetsFile.EndOfStream Then
                    Dim headerLine = srTargetsFile.ReadLine

                    Dim headers = headerLine.Split(ControlChars.Tab)
                    If headers.Count < 3 Then
                        Throw New DirectoryNotFoundException("Header line in the targets file does not have enough columns, " & fiTargetsFile.Name)
                    End If

                    If String.Compare(headers(CODE_COLUMN_INDEX), "Code", True) <> 0 Then
                        Throw New DirectoryNotFoundException("The 3rd column in the header line of the targets file is not 'Code', it is '" & headers(2) & "' in " & fiTargetsFile.Name)
                    End If
                End If

                While Not srTargetsFile.EndOfStream
                    Dim dataLine = srTargetsFile.ReadLine

                    Dim targetInfoColumns = dataLine.Split(columnDelimiters, 4)

                    If targetInfoColumns.Length > CODE_COLUMN_INDEX + 1 Then
                        Dim targetName = targetInfoColumns(CODE_COLUMN_INDEX)
                        If Not mTargets.ContainsKey(targetName) Then
                            mTargets.Add(targetName, False)
                        End If
                    End If

                End While
            End Using


        Catch ex As Exception
            Throw New Exception("Error caching the targets file: " & ex.Message, ex)
        End Try

    End Sub

    Public Sub StartAnalysis()

        mCmdRunner = New clsRunDosProgram(mWorkingDirectory)
        AddHandler mCmdRunner.ErrorEvent, AddressOf CmdRunner_ErrorEvent
        AddHandler mCmdRunner.LoopWaiting, AddressOf CmdRunner_LoopWaiting

        mProgress = 0

        mConsoleOutputFilePath = Path.Combine(mWorkingDirectory, GLYQ_IQ_CONSOLE_OUTPUT_PREFIX & mCore & ".txt")

        With mCmdRunner
            .CreateNoWindow = True
            .CacheStandardOutput = False
            .EchoOutputToConsole = False

            .WriteConsoleOutputToFile = True
            .ConsoleOutputFilePath = mConsoleOutputFilePath
        End With

        mStatus = GlyQIqRunnerStatusCodes.Running

        Dim cmdStr As String = String.Empty
        Dim blnSuccess = mCmdRunner.RunProgram(BatchFilePath, cmdStr, "GlyQ-IQ", True)

        If blnSuccess Then
            mStatus = GlyQIqRunnerStatusCodes.Success
            mProgress = 100
        Else
            mStatus = GlyQIqRunnerStatusCodes.Failure
        End If

    End Sub

    ''' <summary>
    ''' Parse the GlyQ-IQ console output file to track the search progress
    ''' </summary>
    ''' <param name="strConsoleOutputFilePath"></param>
    ''' <remarks></remarks>
    Private Sub ParseConsoleOutputFile(ByVal strConsoleOutputFilePath As String)

        ' In the Console output, we look for lines like this:
        ' Start Workflows... (FragmentedTargetedIQWorkflow) on 3-6-1-0-0
        '
        ' The Target Code is listed at the end of those lines, there 3-6-1-0-0
        ' That code corresponds to the third column in the Targets file

        Static reStartWorkflows As Regex = New Regex("^Start Workflows... .+ on (.+)$", RegexOptions.Compiled Or RegexOptions.IgnoreCase)

        Try
            If Not File.Exists(strConsoleOutputFilePath) Then
                Exit Sub
            End If

            Dim strLineIn As String
            Dim analysisFinished = False

            Using srInFile = New StreamReader(New FileStream(strConsoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))

                Do While Not srInFile.EndOfStream
                    strLineIn = srInFile.ReadLine()

                    If Not String.IsNullOrWhiteSpace(strLineIn) Then

                        Dim reMatch = reStartWorkflows.Match(strLineIn)

                        If reMatch.Success Then
                            Dim targetName As String = reMatch.Groups(1).Value

                            If mTargets.ContainsKey(targetName) Then
                                mTargets(targetName) = True
                            End If
                        ElseIf strLineIn.StartsWith("Target Analysis Finished") Then
                            analysisFinished = True
                        End If

                    End If
                Loop

            End Using

            Dim targetsProcessed = (From item In mTargets Where item.Value = True).Count - 1
            If targetsProcessed < 0 Then targetsProcessed = 0

            Dim glyqIqProgress = Math.Round(targetsProcessed / CDbl(mTargets.Count) * 100)

            If analysisFinished Then glyqIqProgress = 100

            If glyqIqProgress > mProgress Then
                mProgress = glyqIqProgress
            End If

        Catch ex As Exception
            ' Ignore errors here
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error parsing console output file (" & strConsoleOutputFilePath & "): " & ex.Message)
        End Try

    End Sub

    ''' <summary>
    ''' Event handler for event CmdRunner.ErrorEvent
    ''' </summary>
    ''' <param name="strMessage"></param>
    ''' <param name="ex"></param>
    Private Sub CmdRunner_ErrorEvent(strMessage As String, ex As Exception)
        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, strMessage, ex)
    End Sub

    Private Sub CmdRunner_LoopWaiting()

        Static dtLastConsoleOutputParse As DateTime = DateTime.UtcNow

        If DateTime.UtcNow.Subtract(dtLastConsoleOutputParse).TotalSeconds >= 15 Then
            dtLastConsoleOutputParse = DateTime.UtcNow

            ParseConsoleOutputFile(mConsoleOutputFilePath)

        End If

        RaiseEvent CmdRunnerWaiting()
    End Sub

End Class
