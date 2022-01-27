'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy
' Pacific Northwest National Laboratory, Richland, WA
' Created 10/12/2011
'
'*********************************************************************************************************

Option Strict On

Imports System.IO
Imports AnalysisManagerBase.AnalysisTool
Imports AnalysisManagerBase.JobConfig
Imports PRISM.Logging

Public Class clsAnalysisToolRunnerMSGFDB_IMS
    Inherits AnalysisToolRunnerBase

    '*********************************************************************************************************
    'Class for running MSGFDB_IMS analysis
    '*********************************************************************************************************


#Region "Constants"
    Protected Const ION_MOBILITY_CONSOLE_OUTPUT As String = "IonMobilityMsMs_ConsoleOutput.txt"
    Protected Const ION_MOBILITY_MSMS_CONSOLE_NAME As String = "IonMobilityMsMsConsole.exe"
    Protected Const ION_MOBILITY_MSMS_RESULTS_FILE_NAME As String = "Results_MSGFDB_Appended.txt"

    Protected Const MSGFDB_JAR_NAME As String = "MSGFDB.jar"

    Protected Const PROGRESS_PCT_STARTING As Single = 1
    Protected Const PROGRESS_PCT_RUNNING_ION_MOBILITY_MSMS_CONSOLE As Single = 2
    Protected Const PROGRESS_PCT_LOADING_MS_PEAKS As Single = 3
    Protected Const PROGRESS_PCT_LOADING_FEATURES As Single = 10
    Protected Const PROGRESS_PCT_RUNNING_MSGFDB As Single = 30
    Protected Const PROGRESS_PCT_ION_MOBILITY_MSMS_COMPLETE As Single = 96
    Protected Const PROGRESS_PCT_MSGFDB_MAPPING_PEPTIDES_TO_PROTEINS As Single = 97
    Protected Const PROGRESS_PCT_COMPLETE As Single = 99
#End Region

#Region "Module Variables"

    Protected mToolVersionWritten As Boolean
    Protected mIonMobilityMsMsProgLoc As String
    Protected mMSGFDbProgLoc As String
    Protected mMSGFPlus As Boolean

    Protected mResultsIncludeAutoAddedDecoyPeptides As Boolean = False
    Protected mIonMobilityMsMsConsoleOutputErrorMsg As String = String.Empty

    Protected mMSGFDBUtils As AnalysisManagerMSGFDBPlugIn.MSGFPlusUtils

    Protected WithEvents CmdRunner As RunDosProgram

#End Region

#Region "Methods"
    ''' <summary>
    ''' Runs MSGFDB_IMS tool
    ''' </summary>
    ''' <returns>CloseOutType enum indicating success or failure</returns>
    ''' <remarks></remarks>
    Public Overrides Function RunTool() As CloseOutType
        Dim CmdStr As String
        Dim intJavaMemorySize As Integer
        Dim strMSGFDbCmdLineOptions As String = String.Empty

        Dim blnProcessingError As Boolean = False

        Dim blnSuccess As Boolean

        Dim FastaFilePath As String = String.Empty
        Dim FastaFileSizeKB As Single
        Dim FastaFileIsDecoy As Boolean

        Dim strAssumedScanType As String = String.Empty
        Dim strScanTypeFilePath As String

        Dim ResultsFileName As String

        Try
            'Call base class for initial setup
            If Not MyBase.RunTool = CloseOutType.CLOSEOUT_SUCCESS Then
                Return CloseOutType.CLOSEOUT_FAILED
            End If

            If mDebugLevel > 4 Then
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG, "clsAnalysisToolRunnerMSGFDB_IMS.RunTool(): Enter")
            End If

            ' Verify that program files exist

            ' JavaProgLoc will typically be "C:\Program Files\Java\jre7\bin\Java.exe"
            ' Note that we need to run MSGFDB with a 64-bit version of Java since it prefers to use 2 or more GB of ram
            Dim JavaProgLoc = GetJavaProgLoc()
            If String.IsNullOrEmpty(JavaProgLoc) Then
                Return CloseOutType.CLOSEOUT_FAILED
            End If

            Dim blnUseLegacyMSGFDB As Boolean
            Dim strMSGFJarfile As String
            Dim strSearchEngineName As String

            ' Always set blnUseLegacyMSGFDB=True since the IonMobilityMSMS software is hard-coded to use MSGFDB.jar
            blnUseLegacyMSGFDB = True

            If blnUseLegacyMSGFDB Then
                mMSGFPlus = False
                strMSGFJarfile = MSGFDB_JAR_NAME
                strSearchEngineName = "MS-GFDB"
            Else
                mMSGFPlus = True
                strMSGFJarfile = AnalysisManagerMSGFDBPlugIn.MSGFPlusUtils.MSGFPLUS_JAR_NAME
                strSearchEngineName = "MSGF+"
            End If

            ' Determine the path to the IonMobilityMsMs program
            mIonMobilityMsMsProgLoc = DetermineProgramLocation("IonMobilityMsMs", "IonMobilityMsMsProgLoc", ION_MOBILITY_MSMS_CONSOLE_NAME)

            If String.IsNullOrWhiteSpace(mIonMobilityMsMsProgLoc) Then
                Return CloseOutType.CLOSEOUT_FAILED
            End If

            ' Determine the path to the MSGFDB program
            ' Note that we're using a copy of MSGFDB.jar that resides in the same folder as the IonMobilityMsMsConsole application
            mMSGFDbProgLoc = System.IO.Path.Combine(System.IO.Path.GetDirectoryName(mIonMobilityMsMsProgLoc), strMSGFJarfile)
            If Not System.IO.File.Exists(mMSGFDbProgLoc) Then
                mMessage = strMSGFJarfile & " not found in the IonMobilityMsMs folder"
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, mMessage & ": " & mMSGFDbProgLoc)
                Return CloseOutType.CLOSEOUT_FAILED
            End If

            ' Note: we will store the IonMobilityMSMS and MSGFDB version info in the database after the first line is written to file MSGFDB_ConsoleOutput.txt
            mToolVersionWritten = False

            strAssumedScanType = "HCD"
            strScanTypeFilePath = String.Empty

            ' Initialize mMSGFDBUtils
            mMSGFDBUtils = New AnalysisManagerMSGFDBPlugIn.MSGFPlusUtils(mMgrParams, mJobParams, mWorkDir, mDebugLevel)
            RegisterEvents(mMSGFDBUtils)

            AddHandler mMSGFDBUtils.IgnorePreviousErrorEvent, AddressOf mMSGFDBUtils_IgnorePreviousErrorEvent

            ' Get the FASTA file and index it if necessary
            ' Passing in the path to the parameter file so we can look for TDA=0 when using large FASTA files
            ' Note that InitializeFastaFile() calls objIndexedDBCreator.CreateSuffixArrayFiles() to index the fasta file
            ' Legacy MSGFDB and MSGF+ have different index file formats, but .CreateSuffixArrayFiles() knows how to handle this

            Dim parameterFilePath = IO.Path.Combine(mWorkDir, mJobParams.GetParam(AnalysisResources.JOB_PARAM_PARAMETER_FILE))

            Dim result = mMSGFDBUtils.InitializeFastaFile(JavaProgLoc, mMSGFDbProgLoc, FastaFileSizeKB, FastaFileIsDecoy, FastaFilePath, parameterFilePath)
            If result <> CloseOutType.CLOSEOUT_SUCCESS Then
                Return result
            End If

            Dim strInstrumentGroup As String = mJobParams.GetJobParameter("JobParameters", "InstrumentGroup", String.Empty)

            ' Read the MSGFDB Parameter File

            Dim sourceParamFile As IO.FileInfo = Nothing
            Dim finalParameterFile as IO.FileInfo = Nothing

            result = mMSGFDBUtils.ParseMSGFPlusParameterFile(FastaFileIsDecoy, strAssumedScanType, strScanTypeFilePath, strInstrumentGroup, parameterFilePath, sourceParamFile, finalParameterFile)
            If result <> CloseOutType.CLOSEOUT_SUCCESS Then
                Return result
            ElseIf String.IsNullOrEmpty(strMSGFDbCmdLineOptions) Then
                If String.IsNullOrEmpty(mMessage) Then
                    mMessage = "Problem parsing " & strSearchEngineName & " parameter file"
                End If
                Return CloseOutType.CLOSEOUT_FAILED
            End If

            ' This will be set to True if the parameter file contains both TDA=1 and showDecoy=1
            ' Alternatively, if running MSGF+, this is set to true if TDA=1
            mResultsIncludeAutoAddedDecoyPeptides = mMSGFDBUtils.ResultsIncludeAutoAddedDecoyPeptides

            ' Note that the IonMobilityMsMs program creates a file named Results_MSGFDB
            ' After IonMobilityMsMs finishes, we will rename that file to be ResultsFileName
            ResultsFileName = mDatasetName & "_msgfdb.txt"

            LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.INFO, "Running IonMobilityMsMs")

            ' If an MSGFDB analysis crashes with an "out-of-memory" error, then we need to reserve more memory for Java
            ' Customize this on a per-job basis using the MSGFDBJavaMemorySize setting in the settings file
            ' (job 611216 succeeded with a value of 5000)
            intJavaMemorySize = mJobParams.GetJobParameter("MSGFDBJavaMemorySize", 2000)
            If intJavaMemorySize < 512 Then intJavaMemorySize = 512

            ' Set up and execute a program runner to run IonMobilityMsMsConsole.exe
            ' Note that when the program calls MSGFDB it uses these switches:

            ' "C:\Program Files\Java\jre7\bin\java.exe"
            '  -Xmx2000M
            '  -jar C:\DMS_Programs\IonMobilityMsMs\MSGFDB.jar
            '  -s E:\DMS_WorkDir2\Results_dta.txt
            '  -o Results_MSGFDB.txt
            '  -d c:\DMS_Temp_Org\ID_003649_C4CE0EAB.fasta

            ' This plugin provides the remaining switches to the program via strMSGFDBArgsAddon
            ' For example,
            '  -t 20ppm -m 3 -inst 1 -e 1 -c13 0 -nnet 2 -tda 1 -minLength 6 -maxLength 50 -n 1 -uniformAAProb 0 -thread 4 -mod C:\DMS_WorkDir1\MSGFDB_Mods.txt

            Dim intPrecursorMassTolPPM As Integer = mJobParams.GetJobParameter("PrecursorMassTolPPM", 20)
            Dim intFragmentMassTolPPM As Integer = mJobParams.GetJobParameter("FragmentMassTolPPM", 20)
            Dim intNumMsMsBetweenEachMS1 As Integer = mJobParams.GetJobParameter("NumMsMsBetweenEachMS1", 3)
            Dim intParentToFragmentIMSScanDiffMax As Integer = mJobParams.GetJobParameter("ParentToFragmentIMSScanDiffMax", 3)
            Dim intParentToFragmentLCScanDiffMax As Integer = mJobParams.GetJobParameter("ParentToFragmentLCScanDiffMax", 10)

            CmdStr = mIonMobilityMsMsProgLoc
            CmdStr &= " -isos:" & PossiblyQuotePath(System.IO.Path.Combine(mWorkDir, mDatasetName & "_isos.csv"))
            CmdStr &= " -fasta:" & PossiblyQuotePath(FastaFilePath)
            CmdStr &= " -db false"
            CmdStr &= " -ppmp " & intPrecursorMassTolPPM
            CmdStr &= " -ppmf " & intFragmentMassTolPPM
            CmdStr &= " -n " & intNumMsMsBetweenEachMS1
            CmdStr &= " -ims " & intParentToFragmentIMSScanDiffMax
            CmdStr &= " -lc " & intParentToFragmentLCScanDiffMax
            CmdStr &= " -javaExe:" & PossiblyQuotePath(JavaProgLoc)
            CmdStr &= " -msgfDbArgs:" & PossiblyQuotePath(strMSGFDbCmdLineOptions)


            ' Make sure the machine has enough free memory to run MSGFDB
            Dim blnLogFreeMemoryOnSuccess As Boolean = True
            If mDebugLevel < 1 Then blnLogFreeMemoryOnSuccess = False

            If Not AnalysisResources.ValidateFreeMemorySize(intJavaMemorySize, strSearchEngineName, blnLogFreeMemoryOnSuccess) Then
                mMessage = "Not enough free memory to run " & strSearchEngineName
                Return CloseOutType.CLOSEOUT_FAILED
            End If

            LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG, mIonMobilityMsMsProgLoc & " " & CmdStr)

            CmdRunner = New RunDosProgram(mWorkDir)

            With CmdRunner
                .CreateNoWindow = True
                .CacheStandardOutput = True
                .EchoOutputToConsole = True

                .WriteConsoleOutputToFile = True
                .ConsoleOutputFilePath = System.IO.Path.Combine(mWorkDir, ION_MOBILITY_CONSOLE_OUTPUT)
            End With

            mProgress = PROGRESS_PCT_STARTING

            blnSuccess = CmdRunner.RunProgram(mIonMobilityMsMsProgLoc, CmdStr, "IonMobilityMsMs", True)

            If Not blnSuccess And String.IsNullOrEmpty(mMSGFDBUtils.ConsoleOutputErrorMsg) And String.IsNullOrEmpty(mIonMobilityMsMsConsoleOutputErrorMsg) Then
                ' Parse the console output file one more time in hopes of finding an error message
                ParseConsoleOutputFile()
            End If

            If Not mToolVersionWritten Then
                If String.IsNullOrWhiteSpace(mMSGFDBUtils.MSGFPlusVersion) Then
                    mMSGFDBUtils.ParseMSGFPlusConsoleOutputFile(mWorkDir)
                End If
                mToolVersionWritten = StoreToolVersionInfo()
            End If

            If Not String.IsNullOrEmpty(mMSGFDBUtils.ConsoleOutputErrorMsg) Then
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, mMSGFDBUtils.ConsoleOutputErrorMsg)
            End If


            If Not blnSuccess Then
                Dim Msg As String
                Msg = "Error running IonMobilityMsMs"
                mMessage = AnalysisManagerBase.Global.AppendToComment(mMessage, Msg)

                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, Msg & ", job " & mJob)

                If CmdRunner.ExitCode <> 0 Then
                    LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.WARN, "IonMobilityMsMs returned a non-zero exit code: " & CmdRunner.ExitCode.ToString)
                Else
                    LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.WARN, "Call to IonMobilityMsMs failed (but exit code is 0)")
                End If

                blnProcessingError = True

            Else
                mProgress = PROGRESS_PCT_ION_MOBILITY_MSMS_COMPLETE
                mStatusTools.UpdateAndWrite(mProgress)
                If mDebugLevel >= 3 Then
                    LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG, "IonMobilityMsMs Search Complete")
                End If
            End If

            If Not blnProcessingError Then
                result = PostProcessMSGFDBResults(ResultsFileName)
                If result <> CloseOutType.CLOSEOUT_SUCCESS Then
                    If String.IsNullOrEmpty(mMessage) Then
                        mMessage = "Unknown error post-processing the MSGFDB results"
                    End If
                    Return result
                End If
            End If

            mProgress = PROGRESS_PCT_COMPLETE

            'Stop the job timer
            mStopTime = System.DateTime.UtcNow

            'Add the current job data to the summary file
            If Not UpdateSummaryFile() Then
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.WARN, "Error creating summary file, job " & mJob & ", step " & mJobParams.GetParam("Step"))
            End If

            'Make sure objects are released
            System.Threading.Thread.Sleep(500)         ' 1 second delay
            PRISM.ProgRunner.GarbageCollectNow()

            If blnProcessingError Or result <> CloseOutType.CLOSEOUT_SUCCESS Then
                ' Something went wrong
                ' In order to help diagnose things, we will move whatever files were created into the result folder,
                '  archive it using CopyFailedResultsToArchiveDirectory, then return CloseOutType.CLOSEOUT_FAILED
                CopyFailedResultsToArchiveDirectory()
                Return CloseOutType.CLOSEOUT_FAILED
            End If

            Dim success = MakeResultsDirectory()
            If Not success Then
                'MakeResultsDirectory handles posting to local log, so set database error message and exit
                mMessage = "Error making results folder"
                Return CloseOutType.CLOSEOUT_FAILED
            End If

            success = MoveResultFiles()
            If Not success Then
                ' Note that MoveResultFiles should have already called clsAnalysisResults.CopyFailedResultsToArchiveDirectory
                mMessage = "Error moving files into results folder"
                Return CloseOutType.CLOSEOUT_FAILED
            End If

            success = CopyResultsFolderToServer()
            If Not success Then
                ' Note that CopyResultsFolderToServer should have already called clsAnalysisResults.CopyFailedResultsToArchiveDirectory
                Return CloseOutType.CLOSEOUT_FAILED
            End If

        Catch ex As Exception
            mMessage = "Error in MSGFDbPlugin->RunTool: " & ex.Message
            Return CloseOutType.CLOSEOUT_FAILED
        End Try

        Return CloseOutType.CLOSEOUT_SUCCESS    'No failures so everything must have succeeded

    End Function

    Protected Sub CopyFailedResultsToArchiveDirectory()

        Dim strFailedResultsFolderPath As String = mMgrParams.GetParam("FailedResultsFolderPath")
        If String.IsNullOrWhiteSpace(strFailedResultsFolderPath) Then strFailedResultsFolderPath = "??Not Defined??"

        LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.WARN, "Processing interrupted; copying results to archive folder: " & strFailedResultsFolderPath)

        ' Bump up the debug level if less than 2
        If mDebugLevel < 2 Then mDebugLevel = 2

        ' Try to save whatever files are in the work directory (however, delete the several files first)
        Dim strFolderPathToArchive As String
        strFolderPathToArchive = String.Copy(mWorkDir)

        DeleteFileInWorkDir(mDatasetName & "_dta.txt")
        DeleteFileInWorkDir(mDatasetName & "_dta.zip")
        DeleteFileInWorkDir(mDatasetName & "_peaks.txt")
        DeleteFileInWorkDir(mDatasetName & "_peaks.zip")
        DeleteFileInWorkDir(mDatasetName & "_isos.csv")

        ' Make the results folder
        Dim success = MakeResultsDirectory()
        If success Then
            ' Move the result files into the result folder
            success = MoveResultFiles()
            If success Then
                ' Move was a success; update strFolderPathToArchive
                strFolderPathToArchive = System.IO.Path.Combine(mWorkDir, mResultsDirectoryName)
            End If
        End If

        ' Copy the results folder to the Archive folder
        Dim objAnalysisResults As AnalysisResults = New AnalysisResults(mMgrParams, mJobParams)
        objAnalysisResults.CopyFailedResultsToArchiveDirectory(strFolderPathToArchive)

    End Sub

    Sub DeleteFileInWorkDir(strFilename As String)

        Dim fiFile As FileInfo

        Try
            fiFile = New FileInfo(Path.Combine(mWorkDir, strFilename))

            If fiFile.Exists Then
                fiFile.Delete()
            End If

        Catch ex As Exception
            ' Ignore errors here
        End Try

    End Sub

    ''' <summary>
    ''' Parse the IonMobilityMsMsConsole console output file to track the search progress
    ''' </summary>
    ''' <remarks></remarks>
    Private Sub ParseConsoleOutputFile()

        ' Example Console output:
        '
        ' 8/23/2012 8:14:15 PM:   Multiplexed Ion Mobility MS/MS Version: 0.2.12226.34622
        ' 8/23/2012 8:14:15 PM:   Loading MS Peaks
        ' 8/23/2012 8:15:45 PM:   # Peaks = 36054872
        ' 8/23/2012 8:15:45 PM:   Loading MS Features
        ' 8/23/2012 8:15:50 PM:   # MS Features = 440425
        ' 8/23/2012 8:15:50 PM:   Begin Group ID = MS1
        ' 08/23/2012 20:15:51     UIMF file has been opened.
        ' 8/23/2012 8:16:13 PM:   Begin Group ID = MS2
        ' 8/23/2012 8:16:14 PM:   # parents = 3355
        ' 8/23/2012 8:16:14 PM:   # fragments = 13585
        ' 8/23/2012 8:16:14 PM:   Writing parents and fragments to file.
        ' 8/23/2012 8:16:14 PM:   Searching for Parent -> Fragment matches.
        ' 8/23/2012 8:16:14 PM:   Creating dta file.
        ' 8/23/2012 8:16:20 PM:   Running MSGF-DB
        ' 8/23/2012 8:16:20 PM:   "C:\Program Files\Java\jre7\bin\java.exe" -Xmx2000M -jar C:\DMS_Programs\IonMobilityMsMs\MSGFDB.jar -s E:\DMS_WorkDir2\Results_dta.txt -o Results_MSGFDB.txt -d c:\DMS_Temp_Org\ID_003649_C4CE0EAB.fasta -t 20ppm -m 3 -inst 1 -e1 -c13 0 -nnet 2 -tda 1 -minLength 6 -maxLength 50 -n 1 -uniformAAProb 0 -thread 4 -mod C:\DMS_Programs\IonMobilityMsMs\MSGFDB_Mods.txt
        ' 8/23/2012 8:16:38 PM:   MSGF-DB Finished.
        ' 8/23/2012 8:16:38 PM:   Creating new MSGF-DB output file

        Dim strConsoleOutputFilePath As String = "??"
        Dim sngEffectiveProgress As Single
        Dim sngMSGFBProgress As Single

        Try

            strConsoleOutputFilePath = System.IO.Path.Combine(mWorkDir, ION_MOBILITY_CONSOLE_OUTPUT)
            If Not System.IO.File.Exists(strConsoleOutputFilePath) Then
                If mDebugLevel >= 4 Then
                    LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG, "Console output file not found: " & strConsoleOutputFilePath)
                End If

                Exit Sub
            End If

            If mDebugLevel >= 4 Then
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG, "Parsing file " & strConsoleOutputFilePath)
            End If

            Dim strLineIn As String
            Dim intLinesRead As Integer
            Dim intTabIndex As Integer

            sngEffectiveProgress = PROGRESS_PCT_RUNNING_ION_MOBILITY_MSMS_CONSOLE

            Using srInFile As System.IO.StreamReader = New System.IO.StreamReader(New System.IO.FileStream(strConsoleOutputFilePath, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.ReadWrite))

                intLinesRead = 0
                Do While Not srInFile.EndOfStream
                    strLineIn = srInFile.ReadLine()
                    intLinesRead += 1

                    If String.IsNullOrWhiteSpace(strLineIn) Then Continue Do

                    intTabIndex = strLineIn.IndexOf(ControlChars.Tab)
                    If intTabIndex > -1 And intTabIndex < strLineIn.Length - 1 Then
                        strLineIn = strLineIn.Substring(intTabIndex + 1)
                    End If

                    ' Update progress if the line starts with one of the expected phrases
                    If strLineIn.StartsWith("Loading MS Peaks") Then
                        If sngEffectiveProgress < PROGRESS_PCT_LOADING_MS_PEAKS Then
                            sngEffectiveProgress = PROGRESS_PCT_LOADING_MS_PEAKS
                        End If

                    ElseIf strLineIn.StartsWith("Loading MS Features") Then
                        If sngEffectiveProgress < PROGRESS_PCT_LOADING_FEATURES Then
                            sngEffectiveProgress = PROGRESS_PCT_LOADING_FEATURES
                        End If

                    ElseIf strLineIn.StartsWith("Running MSGF-DB") Then
                        If sngEffectiveProgress < PROGRESS_PCT_RUNNING_MSGFDB Then
                            sngEffectiveProgress = PROGRESS_PCT_RUNNING_MSGFDB
                        End If

                    ElseIf strLineIn.ToLower().StartsWith("error") Then
                        mIonMobilityMsMsConsoleOutputErrorMsg = String.Copy(strLineIn)
                    End If

                Loop

            End Using

            If Not mMSGFDBUtils Is Nothing Then
                sngMSGFBProgress = mMSGFDBUtils.ParseMSGFPlusConsoleOutputFile(mWorkDir)

                If sngMSGFBProgress > 0 Then
                    sngEffectiveProgress = PROGRESS_PCT_RUNNING_MSGFDB + CSng((PROGRESS_PCT_ION_MOBILITY_MSMS_COMPLETE - PROGRESS_PCT_RUNNING_MSGFDB) * sngMSGFBProgress / 100.0)
                End If

            End If

            If mProgress < sngEffectiveProgress Then
                mProgress = sngEffectiveProgress
            End If

        Catch ex As Exception
            ' Ignore errors here
            If mDebugLevel >= 2 Then
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, "Error parsing console output file (" & strConsoleOutputFilePath & "): " & ex.Message)
            End If
        End Try

    End Sub

    Protected Function PostProcessMSGFDBResults(ByVal ResultsFileName As String) As CloseOutType

        Dim result As CloseOutType

        ' Rename the Results_dta.txt file that IonMobilityMsMs created
        Dim ioDtaFile As System.IO.FileInfo = New System.IO.FileInfo(System.IO.Path.Combine(mWorkDir, "Results_dta.txt"))
        Dim strDtaFilenameFinal As String = mDatasetName & "_dta.txt"

        If Not ioDtaFile.Exists Then
            LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.WARN, "Results_dta.txt file not found; this is unexpected: " & ioDtaFile.FullName)
        Else
            Try
                ioDtaFile.MoveTo(System.IO.Path.Combine(mWorkDir, strDtaFilenameFinal))
            Catch ex As Exception
                mMessage = "Error renaming the Results_dta.txt file"
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, mMessage & ": " & ex.Message)
                Return CloseOutType.CLOSEOUT_FAILED
            End Try
        End If

        'Zip the DTA.txt file
        result = mMSGFDBUtils.ZipOutputFile(Me, strDtaFilenameFinal)
        If result <> CloseOutType.CLOSEOUT_SUCCESS Then
            Return result
        End If

        ' Rename the output file that IonMobilityMsMs created
        Dim ioIonMobilityMsMsResults As System.IO.FileInfo = New System.IO.FileInfo(System.IO.Path.Combine(mWorkDir, ION_MOBILITY_MSMS_RESULTS_FILE_NAME))
        If Not ioIonMobilityMsMsResults.Exists Then
            mMessage = "IonMobilityMsMsResults file not found: " & ION_MOBILITY_MSMS_RESULTS_FILE_NAME
            LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, mMessage & ", job " & mJob)
            Return CloseOutType.CLOSEOUT_NO_OUT_FILES
        Else
            Try
                ioIonMobilityMsMsResults.MoveTo(System.IO.Path.Combine(mWorkDir, ResultsFileName))
            Catch ex As Exception
                mMessage = "Error renaming the IonMobilityMsMsResults file"
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, mMessage & ": " & ex.Message)
                Return CloseOutType.CLOSEOUT_FAILED
            End Try
        End If

        'Zip the output file
        result = mMSGFDBUtils.ZipOutputFile(Me, ResultsFileName)
        If result <> CloseOutType.CLOSEOUT_SUCCESS Then
            Return result
        End If

        mJobParams.AddResultFileToSkip(ResultsFileName & ".temp.tsv")
        mJobParams.AddResultFileToSkip("Results_MSGFDB.txt")
        mJobParams.AddResultFileToSkip("Results_MSGFDB.txt.temp.tsv")

        mJobParams.AddResultFileToSkip(mDatasetName & "_FeatureFinder_Log.txt")
        mJobParams.AddResultFileToSkip("Run_MSGFDB.bat")

        ' Delete the PeptideToProteinMapEngine_log file
        For Each fiFile As System.IO.FileInfo In New System.IO.DirectoryInfo(mWorkDir).GetFiles("PeptideToProteinMapEngine_log*.txt")
            DeleteFileInWorkDir(fiFile.Name)
        Next

        ' Create the Peptide to Protein map file
        UpdateStatusRunning(PROGRESS_PCT_MSGFDB_MAPPING_PEPTIDES_TO_PROTEINS)

        Dim localOrgDbFolder = mMgrParams.GetParam(AnalysisResources.MGR_PARAM_ORG_DB_DIR)

        result = mMSGFDBUtils.CreatePeptideToProteinMapping(ResultsFileName, mResultsIncludeAutoAddedDecoyPeptides, localOrgDbFolder)
        If result <> CloseOutType.CLOSEOUT_SUCCESS And result <> CloseOutType.CLOSEOUT_NO_DATA Then
            Return result
        End If

        Return CloseOutType.CLOSEOUT_SUCCESS

    End Function

    ''' <summary>
    ''' Stores the tool version info in the database
    ''' </summary>
    ''' <remarks></remarks>
    Protected Function StoreToolVersionInfo() As Boolean

        Dim strToolVersionInfo As String = String.Empty
        Dim ioIonMobilityMsMs As System.IO.FileInfo
        Dim blnSuccess As Boolean

        If mDebugLevel >= 2 Then
            LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG, "Determining tool version info")
        End If

        ioIonMobilityMsMs = New System.IO.FileInfo(mIonMobilityMsMsProgLoc)
        If Not ioIonMobilityMsMs.Exists Then
            Try
                strToolVersionInfo = "Unknown"
                MyBase.SetStepTaskToolVersion(strToolVersionInfo, New List(Of System.IO.FileInfo), saveToolVersionTextFile:=False)
            Catch ex As Exception
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion: " & ex.Message)
                Return False
            End Try

            Return False
        End If

        ' Store the MSGFDb Version
        strToolVersionInfo = mMSGFDBUtils.MSGFPlusVersion
        If String.IsNullOrEmpty(strToolVersionInfo) Then
            strToolVersionInfo = "MSGFDB v???? (??/??/????)"
        End If

        ' Append the version of the IonMobilityMsMs program
        blnSuccess = mToolVersionUtilities.StoreToolVersionInfoOneFile64Bit(strToolVersionInfo, ioIonMobilityMsMs.FullName)
        If Not blnSuccess Then Return False

        ' Store paths to key files in ioToolFiles
        Dim ioToolFiles As New List(Of System.IO.FileInfo)
        ioToolFiles.Add(New System.IO.FileInfo(System.IO.Path.Combine(ioIonMobilityMsMs.DirectoryName, MSGFDB_JAR_NAME)))
        ioToolFiles.Add(ioIonMobilityMsMs)

        Try
            Return MyBase.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles, saveToolVersionTextFile:=False)
        Catch ex As Exception
            LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion: " & ex.Message)
            Return False
        End Try

    End Function

#End Region

#Region "Event Handlers"

    ''' <summary>
    ''' Event handler for CmdRunner.LoopWaiting event
    ''' </summary>
    ''' <remarks></remarks>
    Private Sub CmdRunner_LoopWaiting() Handles CmdRunner.LoopWaiting

        Static dtLastConsoleOutputParse As System.DateTime = System.DateTime.UtcNow

        UpdateStatusFile()

        If System.DateTime.UtcNow.Subtract(dtLastConsoleOutputParse).TotalSeconds >= 15 Then
            dtLastConsoleOutputParse = System.DateTime.UtcNow

            ParseConsoleOutputFile()
            If Not mToolVersionWritten AndAlso Not String.IsNullOrWhiteSpace(mMSGFDBUtils.MSGFPlusVersion) Then
                mToolVersionWritten = StoreToolVersionInfo()
            End If

            LogProgress("MSGF+")

        End If

    End Sub

    Private Sub mMSGFDBUtils_IgnorePreviousErrorEvent(messageToIgnore as string)
        mMessage = mMessage.Replace(messageToIgnore, string.Empty).Trim(";"c, " "c)
    End Sub

#End Region

End Class
