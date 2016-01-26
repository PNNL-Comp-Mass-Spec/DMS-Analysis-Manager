'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Created 07/29/2011
'
'*********************************************************************************************************

Option Strict On

Imports AnalysisManagerBase
Imports System.IO
Imports System.Runtime.InteropServices

Public Class clsAnalysisToolRunnerMSGFDB
    Inherits clsAnalysisToolRunnerBase

    '*********************************************************************************************************
    'Class for running MSGFDB or MSGF+ analysis
    '*********************************************************************************************************

#Region "Constants and Enums"
    Private Enum eInputFileFormatTypes
        Unknown = 0
        CDTA = 1
        MzXML = 2
        MzML = 3
    End Enum
#End Region

#Region "Module Variables"

    Private mToolVersionWritten As Boolean

    ' Path to MSGFDB.jar or MSGFPlus.jar
    Private mMSGFDbProgLoc As String

    Private mMSGFDbProgLocHPC As String

    Private mMSGFPlus As Boolean

    Private mResultsIncludeAutoAddedDecoyPeptides As Boolean = False

    Private mWorkingDirectoryInUse As String

#If EnableHPC = "True" Then
    Private mUsingHPC As Boolean
#End If

    Private mMSGFPlusComplete As Boolean
    Private mMSGFPlusCompletionTime As DateTime

    Private WithEvents mMSGFDBUtils As clsMSGFDBUtils

    Private WithEvents CmdRunner As clsRunDosProgram

#If EnableHPC = "True" Then
    ' Include these references to enable HPC
    '
    ' <Reference Include="HPC_Connector">
    '   <HintPath>..\..\AM_Common\HPC_Connector.dll</HintPath>
    ' </Reference>
    ' <Reference Include="HPC_Submit, Version=1.0.0.0, Culture=neutral, processorArchitecture=MSIL">
    '   <SpecificVersion>False</SpecificVersion>
    '   <HintPath>..\..\AM_Common\HPC_Submit.dll</HintPath>
    ' </Reference>
    ' <Reference Include="Microsoft.Hpc.Scheduler, Version=2.0.0.0, Culture=neutral, PublicKeyToken=31bf3856ad364e35, processorArchitecture=MSIL">
    '   <SpecificVersion>False</SpecificVersion>
    '   <HintPath>..\..\AM_Common\Microsoft.Hpc.Scheduler.dll</HintPath>
    ' </Reference>
    ' <Reference Include="Microsoft.Hpc.Scheduler.Properties, Version=2.0.0.0, Culture=neutral, PublicKeyToken=31bf3856ad364e35" />
    ' <Reference Include="MyEMSLReader">
    '   <HintPath>..\..\AM_Common\MyEMSLReader.dll</HintPath>
    ' </Reference>


    Private WithEvents mComputeCluster As HPC_Submit.WindowsHPC2012
    Private mHPCJobID As Integer
    Private WithEvents mHPCMonitorInitTimer As Timers.Timer
#End If


#End Region

#Region "Methods"
    ''' <summary>
    ''' Runs MSGFDB tool
    ''' </summary>
    ''' <returns>CloseOutType enum indicating success or failure</returns>
    ''' <remarks></remarks>
    Public Overrides Function RunTool() As IJobParams.CloseOutType

        Dim result As IJobParams.CloseOutType
        Dim blnTooManySkippedSpectra = False

        Try
            'Call base class for initial setup
            If Not MyBase.RunTool = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            If m_DebugLevel > 4 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerMSGFDB.RunTool(): Enter")
            End If

#If EnableHPC = "True" Then
            mHPCMonitorInitTimer = New Timers.Timer(30000)
            mHPCMonitorInitTimer.Enabled = False
#End If

            ' Determine whether or not we'll be running MSGF+ in HPC (high performance computing) mode
            Dim udtHPCOptions As clsAnalysisResources.udtHPCOptionsType = clsAnalysisResources.GetHPCOptions(m_jobParams, m_MachName)

            ' Verify that program files exist

            ' javaProgLoc will typically be "C:\Program Files\Java\jre8\bin\Java.exe"
            Dim javaProgLoc = GetJavaProgLoc()
            If String.IsNullOrEmpty(javaProgLoc) Then
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            ' Run MSGF+ (includes indexing the fasta file)
            Dim fiMSGFPlusResults As FileInfo = Nothing
            Dim blnProcessingError = False

            result = RunMSGFPlus(javaProgLoc, udtHPCOptions, fiMSGFPlusResults, blnProcessingError, blnTooManySkippedSpectra)
            If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                If String.IsNullOrEmpty(m_message) Then
                    m_message = "Unknown error running MSGF+"
                End If
                Return result
            End If

            ' Look for the .mzid file
            ' If it exists, then call PostProcessMSGFDBResults even if blnProcessingError is true

            fiMSGFPlusResults.Refresh()
            If fiMSGFPlusResults.Exists Then
                result = PostProcessMSGFDBResults(fiMSGFPlusResults.Name, javaProgLoc, udtHPCOptions)
                If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                    If String.IsNullOrEmpty(m_message) Then
                        m_message = "Unknown error post-processing the MSGF+ results"
                    End If
                    blnProcessingError = True
                End If
            Else
                If String.IsNullOrEmpty(m_message) Then
                    m_message = "MSGF+ results file not found: " & fiMSGFPlusResults.Name
                    blnProcessingError = True
                End If
            End If

            ' Copy any newly created files from PIC back to the local working directory
            ' ToDo: Uncomment this code if we run the PeptideToProteinMapper on HPC
            ' SynchronizeFolders(udtHPCOptions.WorkDirPath, m_WorkDir)

            If Not mMSGFPlusComplete Then
                blnProcessingError = True
                If String.IsNullOrEmpty(m_message) Then
                    m_message = "MSGF+ did not reach completion"
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
                End If
            End If

            m_progress = clsMSGFDBUtils.PROGRESS_PCT_COMPLETE

            'Stop the job timer
            m_StopTime = DateTime.UtcNow

            'Add the current job data to the summary file
            If Not UpdateSummaryFile() Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Error creating summary file, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step"))
            End If

            'Make sure objects are released
            Threading.Thread.Sleep(500)         ' 500 msec delay
            PRISM.Processes.clsProgRunner.GarbageCollectNow()

            If udtHPCOptions.UsingHPC Then
                ' Delete files in the working directory on PIC
                m_FileTools.DeleteDirectoryFiles(udtHPCOptions.WorkDirPath, False)
            End If

            If blnProcessingError Or result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                ' Something went wrong
                ' In order to help diagnose things, we will move whatever files were created into the result folder, 
                '  archive it using CopyFailedResultsToArchiveFolder, then return IJobParams.CloseOutType.CLOSEOUT_FAILED
                CopyFailedResultsToArchiveFolder()
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            result = MakeResultsFolder()
            If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                'MakeResultsFolder handles posting to local log, so set database error message and exit
                m_message = "Error making results folder"
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            result = MoveResultFiles()
            If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                ' Note that MoveResultFiles should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
                m_message = "Error moving files into results folder"
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            result = CopyResultsFolderToServer()
            If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                ' Note that CopyResultsFolderToServer should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
                Return result
            End If

        Catch ex As Exception
            m_message = clsGlobal.AppendToComment(m_message, "Error in MSGFDbPlugin->RunTool: " & ex.Message)
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        If blnTooManySkippedSpectra Then
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        Else
            Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
        End If


    End Function

    Public Shared Function MakeHPCBatchFile(ByVal workDirPath As String, ByVal batchFileName As String, ByVal commandToRun As String) As String

        Const waitTimeSeconds = 35

        Dim batchFilePath = Path.Combine(workDirPath, batchFileName)

        Using swBatchFile = New StreamWriter(New FileStream(batchFilePath, FileMode.Create, FileAccess.Write, FileShare.Read))
            swBatchFile.WriteLine("@echo off")
            swBatchFile.WriteLine(commandToRun)
            'swBatchFile.WriteLine("ping 1.1.1.1 -n 1 -w " & waitTimeSeconds * 1000 & " > nul")
            swBatchFile.WriteLine("\\picfs.pnl.gov\projects\DMS\DMS_Programs\Utilities\sleep " & waitTimeSeconds)
            swBatchFile.WriteLine("echo Success")
        End Using

        Return batchFilePath

    End Function

    ''' <summary>
    ''' Index the Fasta file (if needed) then run MSGF+
    ''' </summary>
    ''' <param name="javaProgLoc"></param>
    ''' <param name="fiMSGFPlusResults"></param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Private Function RunMSGFPlus(
      ByVal javaProgLoc As String,
      ByVal udtHPCOptions As clsAnalysisResources.udtHPCOptionsType,
      <Out()> ByRef fiMSGFPlusResults As FileInfo,
      <Out()> ByRef blnProcessingError As Boolean,
      <Out()> ByRef blnTooManySkippedSpectra As Boolean) As IJobParams.CloseOutType

        Dim strMSGFJarfile As String

        mMSGFPlus = True
        strMSGFJarfile = clsMSGFDBUtils.MSGFPLUS_JAR_NAME

        If mMSGFPlus Then
            fiMSGFPlusResults = New FileInfo(Path.Combine(m_WorkDir, m_Dataset & "_msgfplus.mzid"))
        Else
            fiMSGFPlusResults = New FileInfo(Path.Combine(m_WorkDir, m_Dataset & "_msgfdb.txt"))
        End If

        blnProcessingError = False
        blnTooManySkippedSpectra = False

        ' Determine the path to MSGFDB (or MSGF+)
        ' It is important that you pass "MSGFDB" to this function, even if mMSGFPlus = True
        ' The reason?  The PeptideHitResultsProcessor (and possibly other software) expects the Tool Version file to be named Tool_Version_Info_MSGFDB.txt
        mMSGFDbProgLoc = DetermineProgramLocation("MSGFDB", "MSGFDbProgLoc", strMSGFJarfile)

        If String.IsNullOrWhiteSpace(mMSGFDbProgLoc) Then
            ' Returning CLOSEOUT_FAILED will cause the plugin to immediately exit; results and console output files will not be saved
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        If udtHPCOptions.UsingHPC Then
            ' Make sure the MSGF+ program is up-to-date on the HPC share
            ' Warning: if MSGF+ is running and the .jar file gets updated, then the running jobs will fail because MSGF+ will throw an exception
            ' This function will store the path to the MSGF+ jar file in mMSGFDbProgLocHPC
            VerifyHPCMSGFDb(udtHPCOptions)
        End If

        ' Note: we will store the MSGFDB version info in the database after the first line is written to file MSGFDB_ConsoleOutput.txt
        mToolVersionWritten = False

#If EnableHPC = "True" Then
        mUsingHPC = False
#End If

        mMSGFPlusComplete = False

        Dim eInputFileFormat = eInputFileFormatTypes.Unknown
        Dim strScanTypeFilePath As String = String.Empty
        Dim strAssumedScanType As String = String.Empty

        Dim result = DetermineAssumedScanType(strAssumedScanType, eInputFileFormat, strScanTypeFilePath)
        If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            ' Immediately exit the plugin; results and console output files will not be saved
            Return result
        End If

        ' Initialize mMSGFDBUtils
        mMSGFDBUtils = New clsMSGFDBUtils(m_mgrParams, m_jobParams, m_JobNum, m_WorkDir, m_DebugLevel, mMSGFPlus)

        ' Get the FASTA file and index it if necessary
        ' Passing in the path to the parameter file so we can look for TDA=0 when using large .Fasta files
        Dim strParameterFilePath As String = Path.Combine(m_WorkDir, m_jobParams.GetParam("parmFileName"))
        Dim javaExePath = String.Copy(javaProgLoc)
        Dim msgfdbJarFilePath = String.Copy(mMSGFDbProgLoc)

        If udtHPCOptions.UsingHPC Then
            If udtHPCOptions.SharePath.StartsWith("\\winhpcfs") Then
                javaExePath = "\\winhpcfs\projects\DMS\jre8\bin\java.exe"
            Else
                javaExePath = "\\picfs.pnl.gov\projects\DMS\jre8\bin\java.exe"
            End If
            msgfdbJarFilePath = mMSGFDbProgLocHPC
        End If

        Dim fastaFilePath As String = String.Empty
        Dim fastaFileSizeKB As Single
        Dim fastaFileIsDecoy As Boolean

        result = mMSGFDBUtils.InitializeFastaFile(
          javaExePath, msgfdbJarFilePath,
          fastaFileSizeKB, fastaFileIsDecoy, fastaFilePath,
          strParameterFilePath, udtHPCOptions)

        If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            ' Immediately exit the plugin; results and console output files will not be saved
            Return result
        End If

        Dim strInstrumentGroup As String = m_jobParams.GetJobParameter("JobParameters", "InstrumentGroup", String.Empty)

        ' Read the MSGFDB Parameter File
        Dim strMSGFDbCmdLineOptions As String = String.Empty

        result = mMSGFDBUtils.ParseMSGFDBParameterFile(fastaFileSizeKB, fastaFileIsDecoy, strAssumedScanType, strScanTypeFilePath, strInstrumentGroup, udtHPCOptions, strMSGFDbCmdLineOptions)
        If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            ' Immediately exit the plugin; results and console output files will not be saved
            Return result
        ElseIf String.IsNullOrEmpty(strMSGFDbCmdLineOptions) Then
            If String.IsNullOrEmpty(m_message) Then
                m_message = "Problem parsing MSGF+ parameter file"
            End If
            ' Immediately exit the plugin; results and console output files will not be saved
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        ' This will be set to True if the parameter file contains both TDA=1 and showDecoy=1
        mResultsIncludeAutoAddedDecoyPeptides = mMSGFDBUtils.ResultsIncludeAutoAddedDecoyPeptides


        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Running MSGF+")

        ' If an MSGFDB analysis crashes with an "out-of-memory" error, we need to reserve more memory for Java 
        ' The amount of memory required depends on both the fasta file size and the size of the input .mzML file, since data from all spectra are cached in memory
        ' Customize this on a per-job basis using the MSGFDBJavaMemorySize setting in the settings file 
        ' (job 611216 succeeded with a value of 5000)

        ' Prior to January 2016, MSGF+ used 4 to 7 threads, and if MSGFDBJavaMemorySize was too small, 
        ' we ran the risk of one thread crashing and the results files missing the search results for the spectra assigned to that thread
        ' For large _dta.txt files, 2000 MB of memory could easily be small enough to result in crashing threads
        ' Consequently, the default is now 4000 MB
        '
        ' Furthermore, the 2016-Jan-21 release uses 128 search tasks (or 10 tasks per thread if over 12 threads), 
        ' executing the tasks via a pool, meaning the memory overhead of each thread is lower vs. previous versions that 
        ' had large numbers of tasks on a small, finite number of threads

        Dim intJavaMemorySize = m_jobParams.GetJobParameter("MSGFDBJavaMemorySize", 4000)
        If intJavaMemorySize < 512 Then intJavaMemorySize = 512

        If udtHPCOptions.UsingHPC AndAlso intJavaMemorySize < 10000 Then
            ' Automatically bump up the memory to use to 28 GB  (the machines have 32 GB per socket)
            intJavaMemorySize = 28000
        End If

        'Set up and execute a program runner to run MSGFDB
        Dim cmdStr = " -Xmx" & intJavaMemorySize.ToString & "M -jar " & msgfdbJarFilePath

        ' Define the input file, output file, and fasta file
        Select Case eInputFileFormat
            Case eInputFileFormatTypes.CDTA
                cmdStr &= " -s " & m_Dataset & "_dta.txt"
            Case eInputFileFormatTypes.MzML
                cmdStr &= " -s " & m_Dataset & clsAnalysisResources.DOT_MZML_EXTENSION
            Case eInputFileFormatTypes.MzXML
                cmdStr &= " -s " & m_Dataset & clsAnalysisResources.DOT_MZXML_EXTENSION
            Case Else
                m_message = "Unsupported InputFileFormat: " & eInputFileFormat
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
                ' Immediately exit the plugin; results and console output files will not be saved
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Select

        cmdStr &= " -o " & fiMSGFPlusResults.Name
        cmdStr &= " -d " & PossiblyQuotePath(fastaFilePath)

        ' Append the remaining options loaded from the parameter file
        cmdStr &= " " & strMSGFDbCmdLineOptions

        ' Make sure the machine has enough free memory to run MSGFDB
        Dim blnLogFreeMemoryOnSuccess = Not m_DebugLevel < 1

        If Not udtHPCOptions.UsingHPC Then
            If Not clsAnalysisResources.ValidateFreeMemorySize(intJavaMemorySize, "MSGF+", blnLogFreeMemoryOnSuccess) Then
                m_message = "Not enough free memory to run MSGF+"
                ' Immediately exit the plugin; results and console output files will not be saved
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If
        End If

        mWorkingDirectoryInUse = String.Copy(m_WorkDir)

        Dim blnSuccess = False

        If udtHPCOptions.UsingHPC Then

#If EnableHPC = "True" Then
            Dim criticalError As Boolean
            blnSuccess = StartMSGFPlusHPC(javaExePath, msgfdbJarFilePath, fiMSGFPlusResults.Name, CmdStr, udtHPCOptions, criticalError)

            If criticalError Then
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

#Else
            Throw New Exception("HPC Support is disabled in project AnalysisManagerMSGFDBPlugin")
#End If

        Else
            blnSuccess = StartMSGFPlusLocal(javaExePath, cmdStr)
        End If

        If Not blnSuccess And String.IsNullOrEmpty(mMSGFDBUtils.ConsoleOutputErrorMsg) Then
            ' Wait 2 seconds to give the log file a chance to finalize
            Threading.Thread.Sleep(2000)

            ' Parse the console output file one more time in hopes of finding an error message
            ParseConsoleOutputFile(mWorkingDirectoryInUse)
        End If

        If Not mToolVersionWritten Then
            If String.IsNullOrWhiteSpace(mMSGFDBUtils.MSGFDbVersion) Then
                ParseConsoleOutputFile(mWorkingDirectoryInUse)
            End If
            mToolVersionWritten = StoreToolVersionInfo()
        End If

        If Not String.IsNullOrEmpty(mMSGFDBUtils.ConsoleOutputErrorMsg) Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mMSGFDBUtils.ConsoleOutputErrorMsg)
        End If

        fiMSGFPlusResults.Refresh()

        If blnSuccess Then
            If Not mMSGFPlusComplete Then
                mMSGFPlusComplete = True
                mMSGFPlusCompletionTime = DateTime.UtcNow
            End If
        Else

            Dim msg As String
            If mMSGFPlusComplete Then
                msg = "MSGF+ log file reported it was complete, but aborted the ProgRunner since Java was frozen"
            Else
                msg = "Error running MSGF+"
            End If
            m_message = clsGlobal.AppendToComment(m_message, msg)

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg & ", job " & m_JobNum)

            If mMSGFPlusComplete Then
                ' Don't treat this as a fatal error
                ' in particular, HPC jobs don't always close out cleanly
                blnProcessingError = False
                m_EvalMessage = String.Copy(m_message)
                m_message = String.Empty
            Else
                blnProcessingError = True
            End If

            If Not udtHPCOptions.UsingHPC And Not mMSGFPlusComplete Then
                If CmdRunner.ExitCode <> 0 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "MSGF+ returned a non-zero exit code: " & CmdRunner.ExitCode.ToString)
                Else
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Call to MSGF+ failed (but exit code is 0)")
                End If
            End If

        End If

        If mMSGFPlusComplete Then
            If mMSGFDBUtils.TaskCountCompleted < mMSGFDBUtils.TaskCountTotal Then
                Dim savedCountCompleted = mMSGFDBUtils.TaskCountCompleted

                ' MSGF+ finished, but the log file doesn't report that all of the threads finished
                ' Wait 5 more seconds, then parse the log file again
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "MSGF+ finished, but the log file reports " & mMSGFDBUtils.TaskCountCompleted & " / " & mMSGFDBUtils.TaskCountTotal & " completed tasks")
                Threading.Thread.Sleep(5000)
                mMSGFDBUtils.ParseMSGFDBConsoleOutputFile(mWorkingDirectoryInUse)

                If mMSGFDBUtils.TaskCountCompleted = mMSGFDBUtils.TaskCountTotal Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Reparsing the MSGF+ log file now indicates that all tasks finished")
                ElseIf mMSGFDBUtils.TaskCountCompleted > savedCountCompleted Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Reparsing the MSGF+ log file now indicates that " & mMSGFDBUtils.TaskCountCompleted & " tasks finished.  That is an increase over the previous value but still not all " & mMSGFDBUtils.TaskCountTotal & " tasks")
                Else
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Reparsing the MSGF+ log file indicated the same number of completed tasks")
                End If

            End If

            If mMSGFDBUtils.TaskCountCompleted < mMSGFDBUtils.TaskCountTotal Then

                If mMSGFDBUtils.TaskCountCompleted = mMSGFDBUtils.TaskCountTotal - 1 Then
                    ' All but one of the tasks finished
                    m_EvalMessage = "MSGF+ finished, but the logs indicate that one of the " & mMSGFDBUtils.TaskCountTotal & " tasks did not complete; this could indicate an error"
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, m_EvalMessage)
                Else
                    ' 2 or more tasks did not finish
                    mMSGFPlusComplete = False
                    m_message = "MSGF+ finished, but the logs are incomplete, showing " & mMSGFDBUtils.TaskCountCompleted & " / " & mMSGFDBUtils.TaskCountTotal & " completed search tasks"
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)

                    ' Do not return CLOSEOUT_FAILED, as that causes the plugin to immediately exit; results and console output files would not be saved in that case
                    ' Instead, set processingError to true
                    blnProcessingError = True
                    Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
                End If
            End If
        Else
            If mMSGFDBUtils.TaskCountCompleted > 0 Then
                If String.IsNullOrWhiteSpace(m_message) Then
                    m_message = "MSGF+ processing failed"
                End If
                m_message &= "; logs show " & mMSGFDBUtils.TaskCountCompleted & " / " & mMSGFDBUtils.TaskCountTotal & " completed search tasks"
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
            End If

            ' Do not return CLOSEOUT_FAILED, as that causes the plugin to immediately exit; results and console output files would not be saved in that case
            ' Instead, set processingError to true
            blnProcessingError = True
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        m_progress = clsMSGFDBUtils.PROGRESS_PCT_MSGFDB_COMPLETE
        m_StatusTools.UpdateAndWrite(m_progress)
        If m_DebugLevel >= 3 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "MSGF+ Search Complete")
        End If

        If mMSGFDBUtils.ContinuumSpectraSkipped > 0 Then
            ' See if any spectra were processed
            If Not fiMSGFPlusResults.Exists Then
                ' Note that DMS stored procedure AutoResetFailedJobs looks for jobs with these phrases in the job comment
                '   "None of the spectra are centroided; unable to process"
                '   "skipped xx% of the spectra because they did not appear centroided"
                '   "skip xx% of the spectra because they did not appear centroided"
                '
                ' Failed jobs that are found to have this comment will have their settings files auto-updated and the job will auto-reset

                m_message = clsAnalysisResources.SPECTRA_ARE_NOT_CENTROIDED & " with MSGF+"
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
                blnProcessingError = True
            Else
                ' Compute the fraction of all potential spectra that were skipped
                ' If over 20% of the spectra were skipped, and if the source spectra were not centroided, 
                '   then blnTooManySkippedSpectra will be set to True and the job step will be marked as failed

                Dim dblFractionSkipped As Double
                Dim strPercentSkipped As String
                Dim spectraAreCentroided As Boolean

                ' Examine the job parameters to determine if the spectra were definitely centroided by the MSXML_Gen or DTA_Gen tools
                If m_jobParams.GetJobParameter("CentroidMSXML", False) OrElse
                   m_jobParams.GetJobParameter("CentroidDTAs", False) OrElse
                   m_jobParams.GetJobParameter("CentroidMGF", False) Then
                    spectraAreCentroided = True
                End If

                dblFractionSkipped = mMSGFDBUtils.ContinuumSpectraSkipped / (mMSGFDBUtils.ContinuumSpectraSkipped + mMSGFDBUtils.SpectraSearched)
                strPercentSkipped = (dblFractionSkipped * 100).ToString("0.0") & "%"

                If dblFractionSkipped > 0.2 And Not spectraAreCentroided Then
                    m_message = "MSGF+ skipped " & strPercentSkipped & " of the spectra because they did not appear centroided"
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
                    blnTooManySkippedSpectra = True
                Else
                    m_EvalMessage = "MSGF+ processed some of the spectra, but it skipped " & mMSGFDBUtils.ContinuumSpectraSkipped & " spectra that were not centroided (" & strPercentSkipped & " skipped)"
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, m_EvalMessage)
                End If

            End If

        End If

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

#If EnableHPC = "True" Then
    Private Function StartMSGFPlusHPC(
      ByVal javaExePath As String,
      ByVal msgfdbJarFilePath As String,
      ByVal resultsFileName As String,
      ByVal CmdStr As String,
      ByVal udtHPCOptions As clsAnalysisResources.udtHPCOptionsType,
      <Out()> ByRef criticalError As Boolean) As Boolean

        mWorkingDirectoryInUse = String.Copy(udtHPCOptions.WorkDirPath)
        mUsingHPC = True
        criticalError = False

        ' Synchronize local files to the remote working directory on PIC

        Dim lstFileNameFilterSpec = New List(Of String)
        Dim lstFileNameExclusionSpec = New List(Of String) From {m_Dataset & "_ScanStats.txt", m_Dataset & "_ScanStatsEx.txt", "Mass_Correction_Tags.txt"}

        SynchronizeFolders(m_WorkDir, mWorkingDirectoryInUse, lstFileNameFilterSpec, lstFileNameExclusionSpec)

        Dim jobStep = m_jobParams.GetJobParameter("StepParameters", "Step", 1)

        Dim jobName As String = "MSGF+_Job" & m_JobNum & "_Step" & jobStep

        Dim hpcJobInfo = New HPC_Connector.JobToHPC(udtHPCOptions.HeadNode, jobName, taskName:="MSGF+")

        hpcJobInfo.JobParameters.PriorityLevel = HPC_Connector.PriorityLevel.Normal
        hpcJobInfo.JobParameters.TemplateName = "DMS"        ' If using 32 cores, could use Template "Single"
        hpcJobInfo.JobParameters.ProjectName = "DMS"

        hpcJobInfo.JobParameters.TargetHardwareUnitType = HPC_Connector.HardwareUnitType.Socket
        hpcJobInfo.JobParameters.isExclusive = True

        ' If requesting a socket or a node, there is no need to set the number of cores
        ' hpcJobInfo.JobParameters.MinNumberOfCores = udtHPCOptions.MinimumCores
        ' hpcJobInfo.JobParameters.MaxNumberOfCores = udtHPCOptions.MinimumCores

        If udtHPCOptions.SharePath.StartsWith("\\picfs") Then
            ' April 2014 note: When using picfs.pnl.gov we must reserve an entire node due to file system issues of the Windows Nodes talking to the Isilon file system
            ' Each node has two sockets
            ' Each socket has 16 cores
            ' Thus a node has 32 cores
            hpcJobInfo.JobParameters.TargetHardwareUnitType = HPC_Connector.HardwareUnitType.Node

            ' Make a batch file that will run the java program, then sleep for 35 seconds, which should allow the file system to release the file handles
            Dim batchFilePath = MakeHPCBatchFile(udtHPCOptions.WorkDirPath, "HPC_MSGFPlus_Task.bat", javaExePath & " " & CmdStr)
            m_jobParams.AddResultFileToSkip(batchFilePath)

            hpcJobInfo.TaskParameters.CommandLine = batchFilePath
        Else
            ' Simply run java; no need to add a delay
            hpcJobInfo.TaskParameters.CommandLine = javaExePath & " " & CmdStr
        End If

        hpcJobInfo.TaskParameters.WorkDirectory = udtHPCOptions.WorkDirPath
        hpcJobInfo.TaskParameters.StdOutFilePath = Path.Combine(udtHPCOptions.WorkDirPath, clsMSGFDBUtils.MSGFDB_CONSOLE_OUTPUT_FILE)
        hpcJobInfo.TaskParameters.TaskTypeOption = HPC_Connector.HPCTaskType.Basic
        hpcJobInfo.TaskParameters.FailJobOnFailure = True

        ' Set the maximum runtime to 3 days
        ' Note that this runtime includes the time the job is queued, plus also the time the job is running
        hpcJobInfo.JobParameters.MaxRunTimeHours = 72

        If m_DebugLevel >= 1 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, hpcJobInfo.TaskParameters.CommandLine)
        End If

        If mMSGFPlus Then
            Dim mzidToTSVTask = New HPC_Connector.ParametersTask("MZID_To_TSV")
            Dim tsvFileName = m_Dataset & clsMSGFDBUtils.MSGFDB_TSV_SUFFIX
            Const tsvConversionJavaMemorySizeMB = 4000

            Dim cmdStrConvertToTSV = clsMSGFDBUtils.GetMZIDtoTSVCommandLine(resultsFileName, tsvFileName, udtHPCOptions.WorkDirPath, msgfdbJarFilePath, tsvConversionJavaMemorySizeMB)

            If udtHPCOptions.SharePath.StartsWith("\\picfs") Then
                ' Make a batch file that will run the java program, then sleep for 35 seconds, which should allow the file system to release the file handles
                Dim tsvBatchFilePath = MakeHPCBatchFile(udtHPCOptions.WorkDirPath, "HPC_TSV_Task.bat", javaExePath & " " & cmdStrConvertToTSV)
                m_jobParams.AddResultFileToSkip(tsvBatchFilePath)

                mzidToTSVTask.CommandLine = tsvBatchFilePath
            Else
                ' Simply run java; no need to add a delay
                mzidToTSVTask.CommandLine = javaExePath & " " & cmdStrConvertToTSV
            End If

            mzidToTSVTask.WorkDirectory = udtHPCOptions.WorkDirPath
            mzidToTSVTask.StdOutFilePath = Path.Combine(udtHPCOptions.WorkDirPath, "MzIDToTsv_ConsoleOutput.txt")
            mzidToTSVTask.TaskTypeOption = HPC_Connector.HPCTaskType.Basic
            mzidToTSVTask.FailJobOnFailure = True

            If m_DebugLevel >= 1 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, mzidToTSVTask.CommandLine)
            End If

            hpcJobInfo.SubsequentTaskParameters.Add(mzidToTSVTask)
        End If

        Dim sPICHPCUsername = m_mgrParams.GetParam("PICHPCUser", "")
        Dim sPICHPCPassword = m_mgrParams.GetParam("PICHPCPassword", "")

        If String.IsNullOrEmpty(sPICHPCUsername) Then
            m_message = "Manager parameter PICHPCUser is undefined; unable to schedule HPC job"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
            criticalError = True
            Return False
        End If

        If String.IsNullOrEmpty(sPICHPCPassword) Then
            m_message = "Manager parameter PICHPCPassword is undefined; unable to schedule HPC job"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
            criticalError = True
            Return False
        End If

        mComputeCluster = New HPC_Submit.WindowsHPC2012(sPICHPCUsername, clsGlobal.DecodePassword(sPICHPCPassword))
        mHPCJobID = mComputeCluster.Send(hpcJobInfo)

        Dim blnSuccess As Boolean

        If mHPCJobID <= 0 Then
            m_message = "MSGF+ Job was not created in HPC: " & mComputeCluster.ErrorMessage
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
            blnSuccess = False
        Else
            blnSuccess = True
        End If

        If blnSuccess Then
            If mComputeCluster.Scheduler Is Nothing Then
                m_message = "Error: HPC Scheduler is null for MSGF+"
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
                blnSuccess = False
            End If
        End If

        If blnSuccess Then
            Dim hpcJob = mComputeCluster.Scheduler.OpenJob(mHPCJobID)

            mHPCMonitorInitTimer.Enabled = True

            blnSuccess = mComputeCluster.MonitorJob(hpcJob)

            If Not blnSuccess Then

                m_message = "HPC Job Monitor returned false"
                If Not String.IsNullOrWhiteSpace(mComputeCluster.ErrorMessage) Then
                    m_message &= ": " & mComputeCluster.ErrorMessage
                End If
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
            End If

            ' Copy any newly created files from PIC back to the local working directory
            SynchronizeFolders(udtHPCOptions.WorkDirPath, m_WorkDir)

            ' Rename the Tool_Version_Info file
            Dim fiToolVersionInfo = New FileInfo(Path.Combine(m_WorkDir, "Tool_Version_Info_MSGFDB_HPC.txt"))
            If Not fiToolVersionInfo.Exists Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "ToolVersionInfo file not found; this will lead to problems with IDPicker: " & fiToolVersionInfo.FullName)
            Else
                fiToolVersionInfo.MoveTo(Path.Combine(m_WorkDir, "Tool_Version_Info_MSGFDB.txt"))
            End If
        End If

        Return blnSuccess

    End Function
#End If

    Private Function StartMSGFPlusLocal(ByVal javaExePath As String, ByVal CmdStr As String) As Boolean

        If m_DebugLevel >= 1 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, javaExePath & " " & CmdStr)
        End If

        CmdRunner = New clsRunDosProgram(m_WorkDir)

        With CmdRunner
            .CreateNoWindow = True
            .CacheStandardOutput = True
            .EchoOutputToConsole = True

            .WriteConsoleOutputToFile = True
            .ConsoleOutputFilePath = Path.Combine(m_WorkDir, clsMSGFDBUtils.MSGFDB_CONSOLE_OUTPUT_FILE)
        End With

        m_progress = clsMSGFDBUtils.PROGRESS_PCT_MSGFDB_STARTING
        ResetProgRunnerCpuUsage()

        ' Start the program and wait for it to finish
        ' However, while it's running, LoopWaiting will get called via events
        Dim success = CmdRunner.RunProgram(javaExePath, CmdStr, "MSGF+", True)

        Return success

    End Function

    ''' <summary>
    ''' Convert the .mzid file created by MSGF+ to a .tsv file
    ''' </summary>
    ''' <param name="strMZIDFileName"></param>
    ''' <param name="javaProgLoc"></param>
    ''' <returns>The name of the .tsv file if successful; empty string if an error</returns>
    ''' <remarks></remarks>
    Private Function ConvertMZIDToTSV(ByVal strMZIDFileName As String, ByVal javaProgLoc As String, udtHPCOptions As clsAnalysisResources.udtHPCOptionsType) As String

        Dim blnConversionRequired = True

        Dim strTSVFilePath As String = Path.Combine(m_WorkDir, m_Dataset & clsMSGFDBUtils.MSGFDB_TSV_SUFFIX)

        If udtHPCOptions.UsingHPC Then
            ' The TSV file should have already been created by the HPC job, then copied locally via SynchronizeFolders

            Dim fiTSVFile = New FileInfo(strTSVFilePath)
            If fiTSVFile.Exists Then
                blnConversionRequired = False
            Else
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "MSGF+ TSV file was not created by HPC; missing " & fiTSVFile.Name)
            End If
        End If

        If blnConversionRequired Then
            strTSVFilePath = mMSGFDBUtils.ConvertMZIDToTSV(javaProgLoc, mMSGFDbProgLoc, m_Dataset, strMZIDFileName)

            If String.IsNullOrEmpty(strTSVFilePath) Then
                If String.IsNullOrEmpty(m_message) Then
                    m_message = "Error calling mMSGFDBUtils.ConvertMZIDToTSV; path not returned"
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
                End If
                Return String.Empty
            End If
        End If

        Dim splitFastaEnabled = m_jobParams.GetJobParameter("SplitFasta", False)

        If splitFastaEnabled Then
            Dim tsvFileName = ParallelMSGFPlusRenameFile(Path.GetFileName(strTSVFilePath))
            Return tsvFileName
        Else
            Return Path.GetFileName(strTSVFilePath)
        End If

    End Function

    Private Sub CopyFailedResultsToArchiveFolder()

        Dim result As IJobParams.CloseOutType

        Dim strFailedResultsFolderPath As String = m_mgrParams.GetParam("FailedResultsFolderPath")
        If String.IsNullOrWhiteSpace(strFailedResultsFolderPath) Then strFailedResultsFolderPath = "??Not Defined??"

        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Processing interrupted; copying results to archive folder: " & strFailedResultsFolderPath)

        ' Bump up the debug level if less than 2
        If m_DebugLevel < 2 Then m_DebugLevel = 2

        ' Try to save whatever files are in the work directory (however, delete the _DTA.txt and _DTA.zip files first)
        Dim strFolderPathToArchive As String
        strFolderPathToArchive = String.Copy(m_WorkDir)

        mMSGFDBUtils.DeleteFileInWorkDir(m_Dataset & "_dta.txt")
        mMSGFDBUtils.DeleteFileInWorkDir(m_Dataset & "_dta.zip")

        ' Make the results folder
        result = MakeResultsFolder()
        If result = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            ' Move the result files into the result folder
            result = MoveResultFiles()
            If result = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                ' Move was a success; update strFolderPathToArchive
                strFolderPathToArchive = Path.Combine(m_WorkDir, m_ResFolderName)
            End If
        End If

        ' Copy the results folder to the Archive folder
        Dim objAnalysisResults = New clsAnalysisResults(m_mgrParams, m_jobParams)
        objAnalysisResults.CopyFailedResultsToArchiveFolder(strFolderPathToArchive)

    End Sub

    Private Function CreateScanTypeFile(<Out()> ByRef strScanTypeFilePath As String) As Boolean

        Dim objScanTypeFileCreator As clsScanTypeFileCreator
        objScanTypeFileCreator = New clsScanTypeFileCreator(m_WorkDir, m_Dataset)

        strScanTypeFilePath = String.Empty

        If objScanTypeFileCreator.CreateScanTypeFile() Then
            If m_DebugLevel >= 1 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Created ScanType file: " & Path.GetFileName(objScanTypeFileCreator.ScanTypeFilePath))
            End If
            strScanTypeFilePath = objScanTypeFileCreator.ScanTypeFilePath
            Return True
        Else
            Dim strErrorMessage = "Error creating scan type file: " & objScanTypeFileCreator.ErrorMessage
            m_message = String.Copy(strErrorMessage)

            If Not String.IsNullOrEmpty(objScanTypeFileCreator.ExceptionDetails) Then
                strErrorMessage &= "; " & objScanTypeFileCreator.ExceptionDetails
            End If

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, strErrorMessage)
            Return False
        End If

    End Function

    Private Function DetermineAssumedScanType(
      <Out()> ByRef strAssumedScanType As String,
      <Out()> ByRef eInputFileFormat As eInputFileFormatTypes,
      <Out()> ByRef strScanTypeFilePath As String) As IJobParams.CloseOutType

        Dim strScriptNameLCase As String
        strAssumedScanType = String.Empty

        strScriptNameLCase = m_jobParams.GetParam("ToolName").ToLower()
        strScanTypeFilePath = String.Empty

        If strScriptNameLCase.Contains("mzxml") OrElse strScriptNameLCase.Contains("msgfplus_bruker") Then
            eInputFileFormat = eInputFileFormatTypes.MzXML
        ElseIf strScriptNameLCase.Contains("mzml") Then
            eInputFileFormat = eInputFileFormatTypes.MzML
        Else
            eInputFileFormat = eInputFileFormatTypes.CDTA

            ' Make sure the _DTA.txt file is valid
            If Not ValidateCDTAFile() Then
                Return IJobParams.CloseOutType.CLOSEOUT_NO_DTA_FILES
            End If

            strAssumedScanType = m_jobParams.GetParam("AssumedScanType")

            If String.IsNullOrWhiteSpace(strAssumedScanType) Then
                ' Create the ScanType file (lists scan type for each scan number)
                If Not CreateScanTypeFile(strScanTypeFilePath) Then
                    Return IJobParams.CloseOutType.CLOSEOUT_FAILED
                End If
            End If

        End If

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    Private Sub MonitorProgress()

        Const SECONDS_BETWEEN_UPDATE = 30

        Static dtLastConsoleOutputParse As DateTime = DateTime.UtcNow

        UpdateStatusFile()

        ' Parse the console output file every 30 seconds
        If DateTime.UtcNow.Subtract(dtLastConsoleOutputParse).TotalSeconds >= SECONDS_BETWEEN_UPDATE Then
            dtLastConsoleOutputParse = DateTime.UtcNow

            ParseConsoleOutputFile(mWorkingDirectoryInUse)
            If Not mToolVersionWritten AndAlso Not String.IsNullOrWhiteSpace(mMSGFDBUtils.MSGFDbVersion) Then
                mToolVersionWritten = StoreToolVersionInfo()
            End If

            UpdateProgRunnerCpuUsage(CmdRunner, SECONDS_BETWEEN_UPDATE)

            LogProgress("MSGF+")

            If m_progress >= clsMSGFDBUtils.PROGRESS_PCT_MSGFDB_COMPLETE - Single.Epsilon Then
                If Not mMSGFPlusComplete Then
                    mMSGFPlusComplete = True
                    mMSGFPlusCompletionTime = DateTime.UtcNow
                Else
                    If DateTime.UtcNow.Subtract(mMSGFPlusCompletionTime).TotalMinutes >= 5 Then
                        ' MSGF+ is stuck at 96% complete and has been that way for 5 minutes
                        ' Java is likely frozen and thus the process should be aborted
                        Dim warningMessage = "MSGF+ has been stuck at " & clsMSGFDBUtils.PROGRESS_PCT_MSGFDB_COMPLETE.ToString("0") & "% complete for 5 minutes; aborting since Java appears frozen"
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, warningMessage)

                        ' Bump up mMSGFPlusCompletionTime by one hour
                        ' This will prevent this function from logging the above message every 30 seconds if the .abort command fails
                        mMSGFPlusCompletionTime = mMSGFPlusCompletionTime.AddHours(1)
#If EnableHPC = "True" Then
                        If mUsingHPC Then
                            mComputeCluster.AbortNow()
                        Else
                            CmdRunner.AbortProgramNow()
                        End If
#Else
                        CmdRunner.AbortProgramNow()
#End If

                    End If
                End If
            End If

        End If
    End Sub

    ''' <summary>
    ''' Renames the results file created by a Parallel MSGF+ instance to have _Part##.mzid as a suffix
    ''' </summary>
    ''' <param name="resultsFileName"></param>
    ''' <returns>The path to the new file if success, otherwise the original filename</returns>
    ''' <remarks></remarks>
    Private Function ParallelMSGFPlusRenameFile(ByVal resultsFileName As String) As String

        Dim filePathNew = "??"

        Try
            Dim fiFile = New FileInfo(Path.Combine(m_WorkDir, resultsFileName))

            Dim iteration = clsAnalysisResources.GetSplitFastaIteration(m_jobParams, m_message)

            Dim fileNameNew = Path.GetFileNameWithoutExtension(fiFile.Name) & "_Part" & iteration & fiFile.Extension

            If Not fiFile.Exists Then Return resultsFileName

            filePathNew = Path.Combine(m_WorkDir, fileNameNew)
            fiFile.MoveTo(filePathNew)

            Return fileNameNew

        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error renaming file " & resultsFileName & " to " & filePathNew, ex)
            Return (resultsFileName)
        End Try

    End Function

    ''' <summary>
    ''' Parse the MSGFDB console output file to determine the MSGFDB version and to track the search progress
    ''' </summary>
    ''' <remarks></remarks>
    Private Sub ParseConsoleOutputFile(ByVal workingDirectory As String)

        Dim sngMSGFBProgress As Single = 0

        Try
            If Not mMSGFDBUtils Is Nothing Then
                sngMSGFBProgress = mMSGFDBUtils.ParseMSGFDBConsoleOutputFile(workingDirectory)
            End If

            If m_progress < sngMSGFBProgress Then
                m_progress = sngMSGFBProgress
            End If

        Catch ex As Exception
            ' Ignore errors here
            If m_DebugLevel >= 2 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error parsing console output file: " & ex.Message)
            End If
        End Try

    End Sub

    ''' <summary>
    ''' Convert the .mzid file to a TSV file and create the PeptideToProtein map file
    ''' </summary>
    ''' <param name="resultsFileName"></param>
    ''' <param name="javaProgLoc"></param>
    ''' <param name="udtHPCOptions"></param>
    ''' <returns>True if success, false if an error</returns>
    ''' <remarks>Assumes that the calling function has verified that resultsFileName exists</remarks>
    Private Function PostProcessMSGFDBResults(
      ByVal resultsFileName As String,
      ByVal javaProgLoc As String,
      ByVal udtHPCOptions As clsAnalysisResources.udtHPCOptionsType) As IJobParams.CloseOutType

        Dim currentTask = "Starting"

        Try

            Dim result As IJobParams.CloseOutType
            Dim splitFastaEnabled = m_jobParams.GetJobParameter("SplitFasta", False)

            If splitFastaEnabled Then
                currentTask = "Calling ParallelMSGFPlusRenameFile for " & resultsFileName
                resultsFileName = ParallelMSGFPlusRenameFile(resultsFileName)

                currentTask = "Calling ParallelMSGFPlusRenameFile for MSGFDB_ConsoleOutput.txt"
                ParallelMSGFPlusRenameFile("MSGFDB_ConsoleOutput.txt")
            End If

            ' Gzip the output file
            currentTask = "Zipping " & resultsFileName
            result = mMSGFDBUtils.ZipOutputFile(Me, resultsFileName)
            If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                Return result
            End If

            If Not mMSGFPlus Then
                m_jobParams.AddResultFileToSkip(resultsFileName & ".temp.tsv")
            End If

            Dim strMSGFDBResultsFileName As String
            If Path.GetExtension(resultsFileName).ToLower() = ".mzid" Then

                ' Convert the .mzid file to a .tsv file 
                ' If running on HPC this should have already happened, but we need to call ConvertMZIDToTSV() anyway to possibly rename the .tsv file

                currentTask = "Calling ConvertMZIDToTSV"
                UpdateStatusRunning(clsMSGFDBUtils.PROGRESS_PCT_MSGFDB_CONVERT_MZID_TO_TSV)
                strMSGFDBResultsFileName = ConvertMZIDToTSV(resultsFileName, javaProgLoc, udtHPCOptions)

                If String.IsNullOrEmpty(strMSGFDBResultsFileName) Then
                    Return IJobParams.CloseOutType.CLOSEOUT_FAILED
                End If

            Else
                strMSGFDBResultsFileName = String.Copy(resultsFileName)
            End If

            ' Create the Peptide to Protein map file
            ' ToDo: If udtHPCOptions.UsingPIC = True, then run this on PIC by calling 64-bit PeptideToProteinMapper.exe

            UpdateStatusRunning(clsMSGFDBUtils.PROGRESS_PCT_MSGFDB_MAPPING_PEPTIDES_TO_PROTEINS)

            Dim localOrgDbFolder = m_mgrParams.GetParam("orgdbdir")

            If udtHPCOptions.UsingHPC Then
                ' Override the OrgDbDir to point to Picfs
                localOrgDbFolder = Path.Combine(udtHPCOptions.SharePath, "DMS_Temp_Org")
            End If

            currentTask = "Calling CreatePeptideToProteinMapping"
            result = mMSGFDBUtils.CreatePeptideToProteinMapping(strMSGFDBResultsFileName, mResultsIncludeAutoAddedDecoyPeptides, localOrgDbFolder)
            If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS And result <> IJobParams.CloseOutType.CLOSEOUT_NO_DATA Then
                Return result
            End If

            Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

        Catch ex As Exception
            m_message = clsGlobal.AppendToComment(m_message, "Error in PostProcessMSGFDBResults")
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

    End Function

    ''' <summary>
    ''' Stores the tool version info in the database
    ''' </summary>
    ''' <remarks></remarks>
    Private Function StoreToolVersionInfo() As Boolean

        Dim strToolVersionInfo As String

        If m_DebugLevel >= 2 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info")
        End If

        strToolVersionInfo = String.Copy(mMSGFDBUtils.MSGFDbVersion)

        ' Store paths to key files in ioToolFiles
        Dim ioToolFiles As New List(Of FileInfo)
        ioToolFiles.Add(New FileInfo(mMSGFDbProgLoc))

        Try
            ' Need to pass blnSaveToolVersionTextFile to True so that the ToolVersionInfo file gets created
            ' The PeptideListToXML program uses that file when creating .pepXML files
            Return MyBase.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles, blnSaveToolVersionTextFile:=True)
        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion: " & ex.Message)
            Return False
        End Try

    End Function

    Private Function VerifyHPCMSGFDb(ByVal udtHPCOptions As clsAnalysisResources.udtHPCOptionsType) As Boolean

        Try
            ' Make sure the copy of MSGF+ is up-to-date on PICfs
            Dim fiMSGFDbProg = New FileInfo(mMSGFDbProgLoc)
            Dim strMSGFDbRelativePath = fiMSGFDbProg.Directory.FullName
            Dim chDMSProgramsIndex = strMSGFDbRelativePath.ToLower().IndexOf("\dms_programs\", StringComparison.Ordinal)

            If chDMSProgramsIndex < 0 Then
                m_message = "Unable to determine the relative path to the MSGF+ program folder; could not find \dms_programs\ in " & strMSGFDbRelativePath
                Return False
            End If

            strMSGFDbRelativePath = strMSGFDbRelativePath.Substring(chDMSProgramsIndex + 1)

            Dim strTargetDirectory = Path.Combine(udtHPCOptions.SharePath, strMSGFDbRelativePath)
            mMSGFDbProgLocHPC = Path.Combine(strTargetDirectory, fiMSGFDbProg.Name)

            Dim success = SynchronizeFolders(fiMSGFDbProg.Directory.FullName, strTargetDirectory, fiMSGFDbProg.Name)

            If Not success Then
                If String.IsNullOrWhiteSpace(m_message) Then
                    m_message = "SynchronizeFolders returned false validating " & fiMSGFDbProg.Name & " on HPC"
                End If
                Return False
            End If

        Catch ex As Exception
            m_message = clsGlobal.AppendToComment(m_message, "Error in VerifyHPCMSGFDb")
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex)
            Return False
        End Try

        Return True

    End Function

#End Region

#Region "Event Handlers"

    ''' <summary>
    ''' Event handler for CmdRunner.LoopWaiting event
    ''' </summary>
    ''' <remarks></remarks>
    Private Sub CmdRunner_LoopWaiting() Handles CmdRunner.LoopWaiting
        MonitorProgress()
    End Sub

    Private Sub mMSGFDBUtils_ErrorEvent(ByVal errorMessage As String, ByVal detailedMessage As String) Handles mMSGFDBUtils.ErrorEvent
        m_message = String.Copy(errorMessage)
        If String.IsNullOrEmpty(detailedMessage) Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, errorMessage)
        Else
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, detailedMessage)
        End If

    End Sub

    Private Sub mMSGFDBUtils_IgnorePreviousErrorEvent() Handles mMSGFDBUtils.IgnorePreviousErrorEvent
        m_message = String.Empty
    End Sub

    Private Sub mMSGFDBUtils_MessageEvent(messageText As String) Handles mMSGFDBUtils.MessageEvent
        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, messageText)
    End Sub

    Private Sub mMSGFDBUtils_WarningEvent(warningMessage As String) Handles mMSGFDBUtils.WarningEvent
        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, warningMessage)
    End Sub

#If EnableHPC = "True" Then

	Private Sub mComputeCluster_ErrorEvent(sender As Object, e As HPC_Submit.MessageEventArgs) Handles mComputeCluster.ErrorEvent
		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, e.Message)
	End Sub

	Private Sub mComputeCluster_MessageEvent(sender As Object, e As HPC_Submit.MessageEventArgs) Handles mComputeCluster.MessageEvent
		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, e.Message)
	End Sub

	Private Sub mComputeCluster_ProgressEvent(sender As Object, e As HPC_Submit.ProgressEventArgs) Handles mComputeCluster.ProgressEvent
		mHPCMonitorInitTimer.Enabled = False
		MonitorProgress()
    End Sub

	''' <summary>
	''' This timer is started just before the call to mComputeCluster.MonitorJob
	''' The event will fire very 30 seconds, allowing the manager to update its status
	''' </summary>
	''' <param name="sender"></param>
	''' <param name="e"></param>
	''' <remarks>When event mComputeCluster.ProgressEvent fires, it will disable this timer</remarks>
	Private Sub mHPCMonitorInitTimer_Elapsed(sender As Object, e As Timers.ElapsedEventArgs) Handles mHPCMonitorInitTimer.Elapsed
        UpdateStatusRunning()
	End Sub

#End If

#End Region

End Class
