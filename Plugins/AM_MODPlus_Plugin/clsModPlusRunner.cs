Imports AnalysisManagerBase
Imports System.IO
Imports PRISM.Processes
Imports System.Text.RegularExpressions

Public Class clsMODPlusRunner

    Public Const MOD_PLUS_CONSOLE_OUTPUT_PREFIX As String = "MODPlus_ConsoleOutput_Part"

    Public Const RESULTS_FILE_SUFFIX As String = "_modp.txt"

#Region "Enums"

    Public Enum MODPlusRunnerStatusCodes
        NotStarted = 0
        Running = 1
        Success = 2
        Failure = 3
    End Enum

#End Region

#Region "Events"
    Public Event CmdRunnerWaiting(processIDs As List(Of Integer), coreUsageCurrent As Single, secondsBetweenUpdates As Integer)
#End Region

#Region "Properties"

    Public Property CommandLineArgsLogged As Boolean

    Public ReadOnly Property CommandLineArgs As String
        Get
            Return mCommandLineArgs
        End Get
    End Property

    Public ReadOnly Property ConsoleOutputFilePath As String
        Get
            Return mConsoleOutputFilePath
        End Get
    End Property
    
    Public Property JavaMemorySizeMB As Integer
        Get
            Return mJavaMemorySizeMB
        End Get
        Set(value As Integer)
            If value < 500 Then value = 500
            mJavaMemorySizeMB = value
        End Set
    End Property
    
    Public ReadOnly Property OutputFilePath As String
        Get
            Return mOutputFilePath
        End Get
    End Property

    Public ReadOnly Property ParameterFilePath As String
        Get
            Return mParameterFilePath
        End Get
    End Property

    Public ReadOnly Property ProcessID As Integer
        Get
            Return mProcessID
        End Get
    End Property

    Public ReadOnly Property CoreUsage() As Single
        Get
            Return mCoreUsageCurrent
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

    Public ReadOnly Property ProgRunnerStatus As clsProgRunner.States
        Get
            If mCmdRunner Is Nothing Then
                Return clsProgRunner.States.NotMonitoring
            End If
            Return mCmdRunner.State
        End Get
    End Property

    ''' <summary>
    ''' Program release date, as reported at the console
    ''' </summary>
    ''' <value></value>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public ReadOnly Property ReleaseDate As String
        Get
            Return mReleaseDate
        End Get
    End Property

    Public ReadOnly Property Status As MODPlusRunnerStatusCodes
        Get
            Return mStatus
        End Get
    End Property
    
    Public ReadOnly Property Thread As Integer
        Get
            Return mThread
        End Get
    End Property

#End Region

#Region "Member Variables"

    Private mConsoleOutputFilePath As String
    Private mOutputFilePath As String
    Private mCommandLineArgs As String

    Private mProgress As Double
    Private mJavaMemorySizeMB As Integer

    Private mStatus As MODPlusRunnerStatusCodes
    Private mReleaseDate As String

    Private ReadOnly mDataset As String
    Private ReadOnly mThread As Integer
    Private ReadOnly mWorkingDirectory As String
    Private ReadOnly mParameterFilePath As String
    Private ReadOnly mJavaProgLog As String
    Private ReadOnly mModPlusJarFilePath As String

    Private mProcessID As Integer
    Private mCoreUsageCurrent As Single

    Private mCmdRunner As clsRunDosProgram

#End Region

    ''' <summary>
    ''' Constructor
    ''' </summary>
    ''' <param name="dataset"></param>
    ''' <param name="processingThread"></param>
    ''' <param name="workingDirectory"></param>
    ''' <param name="paramFilePath"></param>
    ''' <param name="javaProgLoc"></param>
    ''' <param name="modPlusJarFilePath"></param>
    ''' <remarks></remarks>
    Public Sub New(
      dataset As String,
      processingThread As Integer,
      workingDirectory As String,
      paramFilePath As String,
      javaProgLoc As String,
      modPlusJarFilePath As String)

        mDataset = dataset
        mThread = processingThread
        mWorkingDirectory = workingDirectory
        mParameterFilePath = paramFilePath
        mJavaProgLog = javaProgLoc
        mModPlusJarFilePath = modPlusJarFilePath

        mStatus = MODPlusRunnerStatusCodes.NotStarted
        mReleaseDate = String.Empty

        mConsoleOutputFilePath = String.Empty
        mOutputFilePath = String.Empty
        mCommandLineArgs = String.Empty

        CommandLineArgsLogged = False
        JavaMemorySizeMB = 3000

    End Sub

    ''' <summary>
    ''' Forcibly ends MODPlus
    ''' </summary>
    ''' <remarks></remarks>
    Public Sub AbortProcessingNow()
        If Not mCmdRunner Is Nothing Then
            mCmdRunner.AbortProgramNow()
        End If
    End Sub

    Public Sub StartAnalysis()

        mCmdRunner = New clsRunDosProgram(mWorkingDirectory)
        AddHandler mCmdRunner.ErrorEvent, AddressOf CmdRunner_ErrorEvent
        AddHandler mCmdRunner.LoopWaiting, AddressOf CmdRunner_LoopWaiting

        mProgress = 0

        mConsoleOutputFilePath = Path.Combine(mWorkingDirectory, MOD_PLUS_CONSOLE_OUTPUT_PREFIX & mThread & ".txt")

        mOutputFilePath = Path.Combine(mWorkingDirectory, mDataset & "_Part" & mThread & RESULTS_FILE_SUFFIX)

        With mCmdRunner
            .CreateNoWindow = True
            .CacheStandardOutput = False
            .EchoOutputToConsole = False

            .WriteConsoleOutputToFile = True
            .ConsoleOutputFilePath = mConsoleOutputFilePath
        End With

        mStatus = MODPlusRunnerStatusCodes.Running

        Dim cmdStr As String = String.Empty
        
        cmdStr &= " -Xmx" & JavaMemorySizeMB & "M"
        cmdStr &= " -jar " & clsGlobal.PossiblyQuotePath(mModPlusJarFilePath)
        cmdStr &= " -i " & clsGlobal.PossiblyQuotePath(mParameterFilePath)
        cmdStr &= " -o " & clsGlobal.PossiblyQuotePath(mOutputFilePath)

        mCommandLineArgs = cmdStr

        mProcessID = 0
        mCoreUsageCurrent = 1

        ' Start the program and wait for it to finish
        ' However, while it's running, LoopWaiting will get called via events
        Dim blnSuccess = mCmdRunner.RunProgram(mJavaProgLog, cmdStr, "MODPlus", True)

        If blnSuccess Then
            mStatus = MODPlusRunnerStatusCodes.Success
            mProgress = 100
        Else
            mStatus = MODPlusRunnerStatusCodes.Failure
        End If

    End Sub

    ''' <summary>
    ''' Parse the MODPlus console output file to track the search progress
    ''' </summary>
    ''' <param name="strConsoleOutputFilePath"></param>
    ''' <remarks></remarks>
    Private Sub ParseConsoleOutputFile(strConsoleOutputFilePath As String)

        ' Example Console output
        '
        ' ************************************************************************************
        ' Modplus (version pnnl) - Identification of post-translational modifications
        ' Release Date: Apr 28, 2015
        ' ************************************************************************************
        ' 
        ' Reading parameters.....
        ' - Input datasest : E:\DMS_WorkDir2\SBEP_STM_rip_LB6_12Aug13_Frodo_13-04-15.mzXML (MZXML type)
        ' - Input database : C:\DMS_Temp_Org\ID_004313_B6EC8119_Excerpt.fasta
        ' - Instrument Resolution: High MS / High MS2 (TRAP)
        ' - Enzyme : Trypsin [KR/*], [*/] (Miss Cleavages: 2, #Enzymatic Termini: 1)
        ' - Variable modifications : 398 specified (Multiple modifications per peptide)
        ' - Precursor ion mass tolerance : 20.0 ppm (C13 error of -1 ~ 2)
        ' - Fragment ion mass tolerance : 0.05 Dalton
        ' 
        ' Start searching!
        ' Reading MS/MS spectra.....  50396 scans
        ' Reading protein database.....  615 proteins / 232536 residues (1)
        ' 
        ' MODPlus | 1/50396
        ' MODPlus | 2/50396
        ' MODPlus | 3/50396
        ' ...
        ' MODPlus | 50394/50396
        ' MODPlus | 50395/50396
        ' MODPlus | 50396/50396
        ' [MOD-Plus] Elapsed Time : 6461 Sec
        ' 

        Static reCheckProgress As New Regex("^MODPlus[^0-9]+(\d+)/(\d+)", RegexOptions.Compiled Or RegexOptions.IgnoreCase)

        Try
            If Not File.Exists(strConsoleOutputFilePath) Then
                Exit Sub
            End If

            Dim spectraSearched = 0
            Dim totalSpectra = 1

            Using srInFile = New StreamReader(New FileStream(strConsoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))

                Do While Not srInFile.EndOfStream
                    Dim strLineIn = srInFile.ReadLine()

                    If String.IsNullOrWhiteSpace(strLineIn) Then
                        Continue Do
                    End If

                    If strLineIn.ToLower().StartsWith("release date:") Then
                        mReleaseDate = strLineIn.Substring(13).TrimStart()
                        Continue Do
                    End If


                    Dim reMatch As Match = reCheckProgress.Match(strLineIn)
                    If reMatch.Success Then
                        Integer.TryParse(reMatch.Groups(1).ToString(), spectraSearched)
                        Integer.TryParse(reMatch.Groups(2).ToString(), totalSpectra)
                        Continue Do
                    End If

                Loop

            End Using

            If totalSpectra < 1 Then totalSpectra = 1

            ' Value between 0 and 100
            Dim progressComplete = Math.Round(spectraSearched / CDbl(totalSpectra) * 100)

            If progressComplete > mProgress Then
                mProgress = progressComplete
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

        Const SECONDS_BETWEEN_UPDATE = 30
        Static dtLastConsoleOutputParse As DateTime = DateTime.UtcNow

        If DateTime.UtcNow.Subtract(dtLastConsoleOutputParse).TotalSeconds >= SECONDS_BETWEEN_UPDATE Then
            dtLastConsoleOutputParse = DateTime.UtcNow

            ParseConsoleOutputFile(mConsoleOutputFilePath)

            ' Note that the call to GetCoreUsage() will take at least 1 second
            mCoreUsageCurrent = ProgRunner.GetCoreUsage()
            mProcessID = ProgRunner.ProcessID
        End If

        Dim processIDs = New List(Of Integer)
        processIDs.Add(mProcessID)

        RaiseEvent CmdRunnerWaiting(processIDs, mCoreUsageCurrent, SECONDS_BETWEEN_UPDATE)
    End Sub

End Class
