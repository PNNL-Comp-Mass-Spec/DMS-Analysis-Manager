Option Strict On

'*********************************************************************************************************
' Written by Matt Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2006, Battelle Memorial Institute
' Created 03/02/2010
'
'*********************************************************************************************************

imports AnalysisManagerBase
Imports PRISM.Files
Imports AnalysisManagerBase.clsGlobal

Public Class clsAnalysisToolRunnerXTHPC
    Inherits clsAnalysisToolRunnerBase

    '*********************************************************************************************************
    'Class for running XTandem analysis
    '*********************************************************************************************************

#Region "Module Variables"
    Protected Const PROGRESS_PCT_XTANDEM_RUNNING As Single = 5
    Protected Const PROGRESS_PCT_PEPTIDEHIT_START As Single = 95
    Protected Const PROGRESS_PCT_PEPTIDEHIT_COMPLETE As Single = 99
    Protected Const HPC_NAME As String = " svc-dms cu0login1 -i C:\DMS_Programs\Chinook\SSH_Keys\Chinook.ppk" '"svc-dms chinook.emsl.pnl.gov -pw PUT_PASSWORD_HERE" '
    Protected WithEvents CmdRunner As clsRunDosProgram
    Private m_HPCJobNumber As String()
    '--------------------------------------------------------------------------------------------
    'Future section to monitor XTandem log file for progress determination
    '--------------------------------------------------------------------------------------------
    'Dim WithEvents m_StatFileWatch As FileSystemWatcher
    'Protected m_XtSetupFile As String = "default_input.xml"
    '--------------------------------------------------------------------------------------------
    'End future section
    '--------------------------------------------------------------------------------------------
#End Region

#Region "Events"
    Public Event LoopWaiting()   'Class is waiting until next time it's due to check status of called program (good time for external processing)
#End Region

#Region "Methods"
    ''' <summary>
    ''' Runs XTandem tool
    ''' </summary>
    ''' <returns>CloseOutType enum indicating success or failure</returns>
    ''' <remarks></remarks>
    Public Overrides Function RunTool() As IJobParams.CloseOutType

        Dim CmdStr As String
        Dim result As IJobParams.CloseOutType
        Dim ParallelZipNum As Integer
        Dim GetJobNoOutputFile_CmdFile As String
        Dim i As Integer

        'Do the base class stuff
        If Not MyBase.RunTool = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then Return IJobParams.CloseOutType.CLOSEOUT_FAILED

        ' Make sure the _DTA.txt file is valid
        'If Not ValidateCDTAFile() Then
        '    Return IJobParams.CloseOutType.CLOSEOUT_NO_DTA_FILES
        'End If

        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Running XTandem")

        CmdRunner = New clsRunDosProgram(m_WorkDir)

        If m_DebugLevel > 4 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerXT.OperateAnalysisTool(): Enter")
        End If

        ' verify that program file exists
        Dim progLoc As String = m_mgrParams.GetParam("puttyprogloc")
        If Not System.IO.File.Exists(progLoc) Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Cannot find Putty (putty.exe) program file")
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        ' verify that program file exists
        Dim progSftpLoc As String = m_mgrParams.GetParam("puttysftpprogloc")
        If Not System.IO.File.Exists(progSftpLoc) Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Cannot find PuttySFTP (psftp.exe) program file")
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        '--------------------------------------------------------------------------------------------
        'Future section to monitor XTandem log file for progress determination
        '--------------------------------------------------------------------------------------------
        ''Get the XTandem log file name for a File Watcher to monitor
        'Dim XtLogFileName As String = GetXTLogFileName(System.IO.Path.Combine(m_WorkDir, m_XtSetupFile))
        'If XtLogFileName = "" Then
        '	m_logger.PostEntry("Error getting XTandem log file name", ILogger.logMsgType.logError, True)
        '	Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        'End If

        ''Setup and start a File Watcher to monitor the XTandem log file
        'StartFileWatcher(m_workdir, XtLogFileName)
        '--------------------------------------------------------------------------------------------
        'End future section
        '--------------------------------------------------------------------------------------------
        '
        'Set up and execute a program runner to run Putty program to get list of fasta files
        CmdStr = "-l " & HPC_NAME & " -m " & "CreateFastaFileList.txt"
        If Not CmdRunner.RunProgram(progLoc, CmdStr, "Putty", True) Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error running Putty to create directories on super computer, job " & m_JobNum)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        'Set up and execute a program runner to run PuttySFTP program to get files from Supercomputer
        CmdStr = "-l " & HPC_NAME & " -b " & "GetFastaFileList.txt"
        If Not CmdRunner.RunProgram(progSftpLoc, CmdStr, "PuttySFTP", True) Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error running Putty SFTP to copy files to super computer, job " & m_JobNum)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        'Compare fasta files to see if we need to copy it over
        If Not FastaFilesEqual Then
            'Set up and execute a program runner to run PuttySFTP program to transfer fasta file to Supercomputer
            CmdStr = "-l " & HPC_NAME & " -b " & "PutFastaJob" & m_JobNum
            If Not CmdRunner.RunProgram(progSftpLoc, CmdStr, "PuttySFTP", True) Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error running Putty SFTP to copy fasta file to super computer, job " & m_JobNum)
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If
        End If

        'Set up and execute a program runner to run Putty program to create directories
        CmdStr = "-l " & HPC_NAME & " -m " & "CreateDir_Job" & m_JobNum
        If Not CmdRunner.RunProgram(progLoc, CmdStr, "Putty", True) Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error running Putty to create directories on super computer, job " & m_JobNum)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        ParallelZipNum = CInt(m_jobParams.GetParam("NumberOfClonedSteps"))

        'Set up and execute a program runner to run PuttySFTP program to transfer files to Supercomputer
        For i = 1 To ParallelZipNum
            CmdStr = "-l " & HPC_NAME & " -b " & "PutCmds_Job" & m_JobNum & "_" & i
            If Not CmdRunner.RunProgram(progSftpLoc, CmdStr, "PuttySFTP", True) Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error running Putty SFTP to copy files to super computer, job " & m_JobNum)
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If
        Next

        'All files have been copied, now run the command to start the jobs
        For i = 1 To ParallelZipNum
            CmdStr = "-l " & HPC_NAME & " -m " & "StartXT_Job" & m_JobNum & "_" & i
            If Not CmdRunner.RunProgram(progLoc, CmdStr, "Putty", True) Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error running Putty to schedule the jobs on the super computer, job " & m_JobNum)
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If
        Next

        'Get job number from supercomputer and append to filename.
        For i = 1 To ParallelZipNum
            GetJobNoOutputFile_CmdFile = System.IO.Path.Combine(m_mgrParams.GetParam("workdir"), "GetJobOutputCmds_Job" & m_JobNum & "_" & i)
            MakeGetJobOutputFilesCmdFile(GetJobNoOutputFile_CmdFile, i.ToString)
        Next

        Dim MonitorInterval As Integer
        MonitorInterval = 2700 ' Wait 45 Seconds

        'Wait a few seconds to make sure jobs have been assigned
        System.Threading.Thread.Sleep(MonitorInterval)

        'First we need to get the output files from the msub directories
        Dim HPC_Result As IJobParams.CloseOutType = IJobParams.CloseOutType.CLOSEOUT_NO_OUT_FILES
        HPC_Result = RetrieveJobOutputFilesFromHPC(progSftpLoc)
        If HPC_Result = IJobParams.CloseOutType.CLOSEOUT_FAILED Then
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If
        System.Threading.Thread.Sleep(MonitorInterval)

        ReDim m_HPCJobNumber(ParallelZipNum)
        'Next we need to open each output file and see if a job number was assigned.
        HPC_Result = IJobParams.CloseOutType.CLOSEOUT_NO_OUT_FILES
        While HPC_Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS
            HPC_Result = BuildGetOutputFilesCmdFile(progSftpLoc)
            If HPC_Result = IJobParams.CloseOutType.CLOSEOUT_FAILED Then
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If
            System.Threading.Thread.Sleep(MonitorInterval)
        End While

        'Build the cancel job command files in case we have to cancel the jobs
        HPC_Result = BuildCancelJobFilesCmdFile()
        If HPC_Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            'Not sure if we can cancel jobs at this point
            'CancelHPCRunningJobs(progLoc)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        'Now we need to download the result files.  We check to see if the error file empty
        HPC_Result = IJobParams.CloseOutType.CLOSEOUT_NO_OUT_FILES
        While HPC_Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS
            HPC_Result = RetrieveJobResultFilesFromHPC(progSftpLoc)
            If HPC_Result = IJobParams.CloseOutType.CLOSEOUT_FAILED Then
                'If error is detected, the Cancel all jobs that are running.
                CancelHPCRunningJobs(progLoc)
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If
            System.Threading.Thread.Sleep(MonitorInterval)
        End While

        'Now we need piece each result file together to form 1 result file.
        result = ConstructSingleXTandemResultFile()
        If HPC_Result = IJobParams.CloseOutType.CLOSEOUT_FAILED Then
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If
        System.Threading.Thread.Sleep(MonitorInterval)

        '--------------------------------------------------------------------------------------------
        'Future section to monitor XTandem log file for progress determination
        '--------------------------------------------------------------------------------------------
        ''Turn off file watcher
        'm_StatFileWatch.EnableRaisingEvents = False
        '--------------------------------------------------------------------------------------------
        'End future section
        '--------------------------------------------------------------------------------------------

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

        'Zip the output file
        result = ZipMainOutputFile()
        If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            'TODO: What do we do here?
            Return result
        End If

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

        result = RemoveHPCDirectories(progLoc)
        If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            'TODO: What do we do here?
            Return result
        End If

        If Not clsGlobal.RemoveNonResultFiles(m_mgrParams.GetParam("workdir"), m_DebugLevel) Then
            'TODO: Figure out what to do here
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS 'ZipResult

    End Function

    Protected Function MakeGetJobOutputFilesCmdFile(ByVal inputFilename As String, ByVal File_Index As String) As Boolean
        Dim result As Boolean = True

        Dim WorkingDir As String = m_mgrParams.GetParam("WorkDir")

        Dim JobNum As String = m_jobParams.GetParam("Job")

        Try
            ' Create an instance of StreamWriter to write to a file.
            Dim inputFile As System.IO.StreamWriter = New System.IO.StreamWriter(inputFilename)

            inputFile.Write(WriteUnix("cd " & clsAnalysisXTHPCGlobals.HPC_ROOT_DIRECTORY & "/Job" & JobNum & "_msub" & File_Index & "/"))

            inputFile.Write(WriteUnix("get X-Tandem_Job" & JobNum & "_" & File_Index & ".output"))

            inputFile.Close()

        Catch E As Exception
            ' Let the user know what went wrong.
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisResourcesXTHPC.MakeGetJobOutputFilesCmdFile, An error occurred while making the get putput files command files" & E.Message)
            result = False
            Return result
        End Try

        Return result
    End Function

    Protected Function RetrieveJobOutputFilesFromHPC(ByVal progSftpLoc As String) As IJobParams.CloseOutType
        Dim CmdStr As String
        Dim i As Integer
        Dim ParallelZipNum As Integer
        Dim JobOutputFilename As String

        Try
            ParallelZipNum = CInt(m_jobParams.GetParam("NumberOfClonedSteps"))

            For i = 1 To ParallelZipNum
                JobOutputFilename = "GetJobOutputCmds_Job" & m_JobNum & "_" & i
                CmdStr = "-l " & HPC_NAME & " -b " & JobOutputFilename
                If Not CmdRunner.RunProgram(progSftpLoc, CmdStr, "PuttySFTP", True) Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error running Putty SFTP to copy files to super computer, job " & m_JobNum)
                    Return IJobParams.CloseOutType.CLOSEOUT_FAILED
                End If
                If Not System.IO.File.Exists(System.IO.Path.Combine(m_WorkDir, "X-Tandem_Job" & m_JobNum & "_" & i & ".output")) Then
                    Return IJobParams.CloseOutType.CLOSEOUT_FAILED
                End If
            Next

        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisResourcesXTHPC.RetrieveJobOutputFilesFromHPC, An error occurred while trying to retrieve job output files" & ex.Message)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    Protected Function CancelHPCRunningJobs(ByVal progLoc As String) As IJobParams.CloseOutType
        Dim CmdStr As String
        Dim JobOutputFilename As String

        Try
            JobOutputFilename = "Cancel_Job" & m_JobNum
            CmdStr = "-l " & HPC_NAME & " -m " & JobOutputFilename
            If Not CmdRunner.RunProgram(progLoc, CmdStr, "Putty", True) Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error running Putty to Cancel Jobs on super computer, job " & m_JobNum)
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisResourcesXTHPC.CancelHPCRunningJobs, An error occurred while trying to cancel jobs on super computer" & ex.Message)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function


    Protected Function RemoveHPCDirectories(ByVal progLoc As String) As IJobParams.CloseOutType
        Dim CmdStr As String
        Dim WorkingDir As String = m_mgrParams.GetParam("WorkDir")
        Dim RemoveJobFilename As String
        Try
            RemoveJobFilename = System.IO.Path.Combine(WorkingDir, "Remove_Job" & m_JobNum)
            CmdStr = "-l " & HPC_NAME & " -m " & RemoveJobFilename
            If Not CmdRunner.RunProgram(progLoc, CmdStr, "Putty", True) Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error running Putty to Remove Job Directories on super computer, job " & m_JobNum)
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisResourcesXTHPC.RemoveHPCDirectories, An error occurred while trying to remove job directories on super computer" & ex.Message)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    ''' <summary>
    ''' 
    ''' </summary>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Protected Function BuildGetOutputFilesCmdFile(ByVal progSftpLoc As String) As IJobParams.CloseOutType
        Dim Get_CmdFile As String
        Dim i As Integer
        Dim ParallelZipNum As Integer
        Dim HPC_JobNum As String

        Try
            ParallelZipNum = CInt(m_jobParams.GetParam("NumberOfClonedSteps"))

            For i = 1 To ParallelZipNum
                HPC_JobNum = GetHPCJobNumber(i.ToString)
                If Not String.IsNullOrEmpty(HPC_JobNum) AndAlso IsNumeric(HPC_JobNum) Then
                    Get_CmdFile = System.IO.Path.Combine(m_mgrParams.GetParam("workdir"), "GetResultFilesCmds_Job" & m_JobNum & "_" & i)
                    MakeGetOutputFilesCmdFile(Get_CmdFile, HPC_JobNum, CStr(i))
                    clsGlobal.m_FilesToDeleteExt.Add(System.IO.Path.GetFileName(Get_CmdFile))
                    m_HPCJobNumber(i) = HPC_JobNum
                Else
                    Return IJobParams.CloseOutType.CLOSEOUT_FAILED
                End If
            Next

        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisResourcesXTHPC.BuildGetOutputFilesCmdFile, An error occurred while building get output command files" & ex.Message)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    Private Function RetrieveJobResultFilesFromHPC(ByVal progSftpLoc As String) As IJobParams.CloseOutType
        Dim CmdStr As String
        Dim i As Integer
        Dim ParallelZipNum As Integer
        Dim JobOutputFilename As String
        Dim ErrorResultFile As String
        Dim ErrorResult As String
        Try
            ParallelZipNum = CInt(m_jobParams.GetParam("NumberOfClonedSteps"))

            For i = 1 To ParallelZipNum
                JobOutputFilename = "GetResultFilesCmds_Job" & m_JobNum & "_" & i
                CmdStr = "-l " & HPC_NAME & " -b " & JobOutputFilename
                ErrorResultFile = System.IO.Path.Combine(m_WorkDir, m_JobNum & "_Part" & i & ".err." & m_HPCJobNumber(i))
                If Not CmdRunner.RunProgram(progSftpLoc, CmdStr, "PuttySFTP", True) Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error running Putty SFTP to copy files to super computer, job " & m_JobNum)
                    Return IJobParams.CloseOutType.CLOSEOUT_FAILED
                End If
                If Not System.IO.File.Exists(System.IO.Path.Combine(m_WorkDir, m_JobNum & "_Part" & i & ".output." & m_HPCJobNumber(i))) Then
                    Return IJobParams.CloseOutType.CLOSEOUT_NO_OUT_FILES
                End If
                If Not System.IO.File.Exists(ErrorResultFile) Then
                    Return IJobParams.CloseOutType.CLOSEOUT_NO_OUT_FILES
                Else
                    Dim fiErrorLocal As New System.IO.FileInfo(ErrorResultFile)
                    If fiErrorLocal.Length > 0 Then
                        '***********Log error to log file here***********
                        ErrorResult = ReadEntireFile(ErrorResultFile)
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error file " & ErrorResultFile & " contains the following error: " & ErrorResult & ", job " & m_JobNum)
                        Return IJobParams.CloseOutType.CLOSEOUT_FAILED
                    End If
                End If
                If Not System.IO.File.Exists(System.IO.Path.Combine(m_WorkDir, m_jobParams.GetParam("DatasetNum") & "_" & i & "_xt.xml")) Then
                    Return IJobParams.CloseOutType.CLOSEOUT_NO_OUT_FILES
                End If
            Next

        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisResourcesXTHPC.RetrieveJobResultFilesFromHPC, An error occurred while trying to retrieve job result files" & ex.Message)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    Protected Function GetHPCJobNumber(ByVal File_Index As String) As String
        Dim HPC_JobNum As String = ""
        Dim HPC_JobError As String = ""
        Dim HPC_OutputFilename As String = ""
        Dim Cnt As Integer = 0

        HPC_OutputFilename = System.IO.Path.Combine(m_WorkDir, "X-Tandem_Job" & m_JobNum & "_" & File_Index & ".output")

        If System.IO.File.Exists(HPC_OutputFilename) Then
            Dim srReader As System.IO.StreamReader = New System.IO.StreamReader(HPC_OutputFilename)
            Do While srReader.Peek >= 0
                HPC_JobNum = srReader.ReadLine.Trim
                HPC_JobError += HPC_JobNum
                If HPC_JobNum.Length > 0 And Cnt < 2 Then
                    If IsNumeric(HPC_JobNum) Then
                        Return HPC_JobNum
                    End If
                End If

                If HPC_JobNum.Length > 0 And Cnt >= 2 Then
                    HPC_JobNum += HPC_JobNum
                End If
                Cnt += 1
            Loop
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisResourcesXTHPC.GetHPCJobNumber, Output file " & HPC_OutputFilename & " contained the following error:" & HPC_JobError)
            Return ""
        Else
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisResourcesXTHPC.GetHPCJobNumber, Output file was not found:" & HPC_OutputFilename & " ")
            Return ""
        End If

        Return HPC_JobNum

    End Function

    Protected Function ReadEntireFile(ByVal Filename As String) As String
        Dim HPC_JobError As String = ""

        Try
            If System.IO.File.Exists(Filename) Then
                Dim srReader As System.IO.StreamReader = New System.IO.StreamReader(Filename)
                Do While srReader.Peek >= 0
                    HPC_JobError += srReader.ReadLine
                Loop
            End If
        Catch E As Exception
            ' Let the user know what went wrong.
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisResourcesXTHPC.ReadEntireFile, The file could not be read" & E.Message)
            Return E.Message
        End Try

        Return HPC_JobError

    End Function

    Protected Function BuildCancelJobFilesCmdFile() As IJobParams.CloseOutType

        Dim WorkingDir As String = m_mgrParams.GetParam("WorkDir")

        Dim InputFilename As String = System.IO.Path.Combine(WorkingDir, "Cancel_Job" & m_JobNum)

        Dim i As Integer

        Dim ParallelZipNum As Integer

        Try
            clsGlobal.m_FilesToDeleteExt.Add(System.IO.Path.GetFileName(InputFilename))

            ParallelZipNum = CInt(m_jobParams.GetParam("NumberOfClonedSteps"))

            ' Create an instance of StreamWriter to write to a file.
            Dim inputFile As System.IO.StreamWriter = New System.IO.StreamWriter(InputFilename)

            inputFile.Write(WriteUnix("cd " & clsAnalysisXTHPCGlobals.HPC_ROOT_DIRECTORY))

            For i = 1 To ParallelZipNum

                inputFile.Write(WriteUnix("canceljob " & m_HPCJobNumber(i)))

            Next

            inputFile.Close()

        Catch E As Exception
            ' Let the user know what went wrong.
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisResourcesXTHPC.BuildCancelJobFilesCmdFile, The Cancel jobs file could not be written" & E.Message)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try


        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    ''' <summary>
    ''' 
    ''' </summary>
    ''' <param name="inputFilename"></param>
    ''' <param name="HPCJobNumber"></param>
    ''' <param name="File_Index"></param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Protected Function MakeGetOutputFilesCmdFile(ByVal inputFilename As String, ByVal HPCJobNumber As String, ByVal File_Index As String) As Boolean
        Dim result As Boolean = True

        Dim WorkingDir As String = m_mgrParams.GetParam("WorkDir")

        Try
            ' Create an instance of StreamWriter to write to a file.
            Dim inputFile As System.IO.StreamWriter = New System.IO.StreamWriter(inputFilename)

            inputFile.Write(WriteUnix("cd " & clsAnalysisXTHPCGlobals.HPC_ROOT_DIRECTORY & "Job" & m_JobNum & "_" & File_Index & "/"))

            inputFile.Write(WriteUnix("get " & m_JobNum & "_Part" & File_Index & ".output." & HPCJobNumber))

            inputFile.Write(WriteUnix("get " & m_JobNum & "_Part" & File_Index & ".err." & HPCJobNumber))

            inputFile.Write(WriteUnix("get " & m_jobParams.GetParam("DatasetNum") & "_" & File_Index & "_xt.xml"))

            inputFile.Close()

        Catch E As Exception
            ' Let the user know what went wrong.
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisResourcesXTHPC.MakeGetOutputFilesCmdFile, The file could not be written" & E.Message)
            result = False
            Return result
        End Try

        Return result
    End Function

    Protected Function WriteUnix(ByVal inputString As String) As String

        inputString = inputString & ControlChars.Lf

        Return inputString

    End Function

    Protected Function FastaFilesEqual() As Boolean
        Dim result As Boolean = False

        Dim WorkingDir As String = m_mgrParams.GetParam("WorkDir")

        Dim OrgDBName As String = m_jobParams.GetParam("generatedFastaName")

        Dim LocalOrgDBFolder As String = m_mgrParams.GetParam("orgdbdir")

        Dim fastaSizeLocal As String

        Dim fastaSizeHPC As String

        Try
            If System.IO.File.Exists(System.IO.Path.Combine(WorkingDir, "fastafiles.txt")) Then
                Dim fiFastaLocal As New System.IO.FileInfo(System.IO.Path.Combine(LocalOrgDBFolder, OrgDBName))
                fastaSizeLocal = fiFastaLocal.Length.ToString

                'Check to see if file is empty or less than 100 Mb.  If so, just copy fasta
                Dim fiFastaLocalHPC As New System.IO.FileInfo(System.IO.Path.Combine(WorkingDir, "fastafiles.txt"))
                If (fiFastaLocal.Length = 0) Or (fiFastaLocal.Length < 100000000) Then
                    Return False
                End If

                ' Create an instance of StreamWriter to read from a file.
                Dim listFile As System.IO.StreamReader = New System.IO.StreamReader(System.IO.Path.Combine(WorkingDir, "fastafiles.txt"))
                fastaSizeHPC = listFile.ReadLine.Trim
                listFile.Close()
                If fastaSizeHPC = fastaSizeLocal Then
                    Return True
                End If

            Else

                Return False

            End If

        Catch E As Exception
            ' Let the user know what went wrong.
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisResourcesXTHPC.FastaFilesEqual, The file could not be read" & E.Message)
            result = False
            Return result
        End Try

        Return result
    End Function

    ''' <summary>
    ''' Make sure the _DTA.txt file exists and has at lease one spectrum in it
    ''' </summary>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Protected Function ValidateCDTAFile() As Boolean
        Dim strInputFilePath As String
        Dim srReader As System.IO.StreamReader

        Dim blnDataFound As Boolean = False

        Try
            strInputFilePath = System.IO.Path.Combine(m_WorkDir, m_jobParams.GetParam("datasetNum") & "_dta.txt")

            If Not System.IO.File.Exists(strInputFilePath) Then
                m_message = "_DTA.txt file not found: " & strInputFilePath
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
                Return False
            End If

            srReader = New System.IO.StreamReader(New System.IO.FileStream(strInputFilePath, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.ReadWrite))

            Do While srReader.Peek >= 0
                If srReader.ReadLine.Trim.Length > 0 Then
                    blnDataFound = True
                    Exit Do
                End If
            Loop

            srReader.Close()

            If Not blnDataFound Then
                m_message = "The _DTA.txt file is empty"
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
            End If

        Catch ex As Exception
            m_message = "Exception in ValidateCDTAFile"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message)
            Return False
        End Try

        Return blnDataFound

    End Function

    ''' <summary>
    ''' Zips concatenated XML output file
    ''' </summary>
    ''' <returns>CloseOutType enum indicating success or failure</returns>
    ''' <remarks></remarks>
    Private Function ZipMainOutputFile() As IJobParams.CloseOutType
        Dim TmpFile As String
        Dim FileList() As String
        Dim ZipFileName As String

        Try
            Dim Zipper As New ZipTools(m_WorkDir, m_mgrParams.GetParam("zipprogram"))
            FileList = System.IO.Directory.GetFiles(m_WorkDir, "*_xt.xml")
            For Each TmpFile In FileList
                ZipFileName = System.IO.Path.Combine(m_WorkDir, System.IO.Path.GetFileNameWithoutExtension(TmpFile)) & ".zip"
                If Not Zipper.MakeZipFile("-fast", ZipFileName, System.IO.Path.GetFileName(TmpFile)) Then
                    Dim Msg As String = "Error zipping output files, job " & m_JobNum
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, Msg)
                    m_message = AppendToComment(m_message, "Error zipping output files")
                    Return IJobParams.CloseOutType.CLOSEOUT_FAILED
                End If
            Next
        Catch ex As Exception
            Dim Msg As String = "clsAnalysisToolRunnerXT.ZipMainOutputFile, Exception zipping output files, job " & m_JobNum & ": " & ex.Message
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, Msg)
            m_message = AppendToComment(m_message, "Error zipping output files")
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        'Delete the XML output files
        Try
            FileList = System.IO.Directory.GetFiles(m_WorkDir, "*_xt.xml")
            For Each TmpFile In FileList
                System.IO.File.SetAttributes(TmpFile, System.IO.File.GetAttributes(TmpFile) And (Not System.IO.FileAttributes.ReadOnly))
                System.IO.File.Delete(TmpFile)
            Next
        Catch Err As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerXT.ZipMainOutputFile, Error deleting _xt.xml file, job " & m_JobNum & Err.Message)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function


    Protected Function ConstructSingleXTandemResultFile() As IJobParams.CloseOutType
        Dim WorkingDir As String = m_mgrParams.GetParam("WorkDir")
        Dim lineText As String
        Dim ResultFile As System.IO.StreamWriter = New System.IO.StreamWriter(System.IO.Path.Combine(WorkingDir, "QC_Shew_09_02-pt5_g_19May09_Falcon_09-04-24_xt.xml"))
        Dim SkipLinesCnt As Integer
        Dim StopWriting As Boolean = False
        Dim CurrentMaxNum As Integer
        Dim NewMaxNum As Integer
        Dim OriginalGroupID As Integer
        Dim NewLabelText As String = ""
        Dim EndOfFile As System.Collections.Generic.List(Of String) = New System.Collections.Generic.List(Of String)
        Dim i As Integer
        Dim j As Integer
        Dim ParallelZipNum As Integer

        Try
            ParallelZipNum = CInt(m_jobParams.GetParam("NumberOfClonedSteps"))
            For i = 1 To 4
                Dim CurrentResultFile As System.IO.StreamReader = New System.IO.StreamReader(System.IO.Path.Combine(WorkingDir, "QC_Shew_09_02-pt5_g_19May09_Falcon_09-04-24_" & i & "_xt.xml"))
                Do While CurrentResultFile.Peek >= 0
                    If i > 1 Then
                        lineText = CurrentResultFile.ReadLine
                        If SkipLinesCnt < 3 Then
                            'do nothing 
                        Else
                            If lineText.Contains("<group label=""input parameters""") Then
                                StopWriting = True
                            End If
                            If lineText.Contains("<group id=""") Then
                                OriginalGroupID = RetrieveGroupIdNumber(lineText)
                                NewMaxNum = RetrieveMaxNumber(lineText, NewMaxNum, CurrentMaxNum)
                                lineText = lineText.Replace("<group id=""" & OriginalGroupID.ToString, "<group id=""" & (OriginalGroupID + CurrentMaxNum).ToString)
                                NewLabelText = "label=""" & OriginalGroupID.ToString
                            End If
                            If lineText.Contains(NewLabelText) Then
                                lineText = lineText.Replace(NewLabelText, "label=""" & (OriginalGroupID + CurrentMaxNum).ToString)
                            End If
                            If Not StopWriting Then
                                ResultFile.WriteLine(lineText)
                            End If
                        End If
                        SkipLinesCnt += 1
                    Else
                        lineText = CurrentResultFile.ReadLine
                        If lineText.Contains("<group label=""input parameters""") Then
                            StopWriting = True
                        End If
                        If lineText.Contains("<group id=""") Then
                            CurrentMaxNum = RetrieveMaxNumber(lineText, CurrentMaxNum, 0)
                        End If
                        If Not StopWriting Then
                            ResultFile.WriteLine(lineText)
                        Else
                            EndOfFile.Add(lineText)
                        End If
                    End If
                Loop
                StopWriting = False
                SkipLinesCnt = 0
                CurrentResultFile.Close()
                If i > 1 Then
                    CurrentMaxNum = NewMaxNum
                End If
            Next
            For j = 0 To EndOfFile.Count - 1
                ResultFile.WriteLine(EndOfFile(j))
            Next
            ResultFile.Close()
        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerXT.ConstructSingleXTandemResultFile, Error concatenating _xt.xml files, job " & m_JobNum & ex.Message)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    Protected Function RetrieveGroupIdNumber(ByVal LineOfText As String) As Integer
        Dim TmpNumText As String
        Dim NewNumText As String
        Try
            TmpNumText = LineOfText.Substring(11, LineOfText.Length - 12)
            NewNumText = TmpNumText.Substring(0, TmpNumText.IndexOf(""""))
        Catch Err As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerXT.RetrieveGroupIdNumber, Error obtaining group id from *_xt.xml files, job " & m_JobNum & Err.Message)
            Return -1
        End Try

        Return CInt(NewNumText)

    End Function

    Protected Function RetrieveMaxNumber(ByVal LineOfText As String, ByVal CurrentMaxNum As Integer, ByVal OffsetNum As Integer) As Integer
        Dim NewNumText As Integer
        Try
            NewNumText = RetrieveGroupIdNumber(LineOfText)
            If NewNumText + OffsetNum < CurrentMaxNum Then
                NewNumText = CurrentMaxNum
            Else
                NewNumText = NewNumText + OffsetNum
            End If
        Catch Err As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerXT.RetrieveMaxNumber, Error obtaining max group id from *_xt.xml files, job " & m_JobNum & Err.Message)
            Return -1
        End Try

        Return NewNumText

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
            m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.RUNNING_TOOL, PROGRESS_PCT_XTANDEM_RUNNING, 0, "", "", "", False)
        End If

    End Sub

    '--------------------------------------------------------------------------------------------
    'Future section to monitor XTandem log file for progress determination
    '--------------------------------------------------------------------------------------------
    '	Private Sub StartFileWatcher(ByVal DirToWatch As String, ByVal FileToWatch As String)

    ''Watches the XTandem status file and reports changes

    ''Setup
    'm_StatFileWatch = New FileSystemWatcher
    'With m_StatFileWatch
    '	.BeginInit()
    '	.Path = DirToWatch
    '	.IncludeSubdirectories = False
    '	.Filter = FileToWatch
    '	.NotifyFilter = NotifyFilters.LastWrite Or NotifyFilters.Size
    '	.EndInit()
    'End With

    ''Start monitoring
    'm_StatFileWatch.EnableRaisingEvents = True

    '	End Sub
    '--------------------------------------------------------------------------------------------
    'End future section
    '--------------------------------------------------------------------------------------------
#End Region

End Class
