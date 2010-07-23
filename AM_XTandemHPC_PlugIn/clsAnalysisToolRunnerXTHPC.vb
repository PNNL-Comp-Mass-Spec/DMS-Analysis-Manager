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

    ' Use the following when connecting to Chinook via chinook.emsl.pnl.gov
    'Protected Const HPC_NAME As String = "svc-dms chinook.emsl.pnl.gov -pw Password_Here "

    ' Use the following when connecting to Chinook via Pub-88 (which is hard-wired to cu0login1)
    Protected Const HPC_NAME As String = " svc-dms cu0login1 -i C:\DMS_Programs\Chinook\SSH_Keys\Chinook.ppk "

    Protected WithEvents CmdRunner As clsRunDosProgram

    Private m_NumClonedSteps As Integer = 1
    Private m_Dataset As String = ""

    ' 2D array, with the first dimension going from 1 to m_NumClonedSteps and the second dimension going from 0 to 1
    '   m_NumClonedSteps(1,0) holds the job number for the first step
    '   m_NumClonedSteps(1,1) holds the status for the first step

    '   m_NumClonedSteps(2,0) holds the job number for the second step (if m_NumClonedSteps > 1)
    '   m_NumClonedSteps(2,1) holds the status for the second step (if m_NumClonedSteps > 1)

    Private m_HPCJobStatus(1, 1) As String
    Private m_HPCAccountName As String = "Undefined"

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

        Dim GetJobNoOutputFile_CmdFile As String
        Dim i As Integer

        'Do the base class stuff
        If Not MyBase.RunTool = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then Return IJobParams.CloseOutType.CLOSEOUT_FAILED

        ' Update m_NumClonedSteps
        m_NumClonedSteps = CInt(m_jobParams.GetParam("NumberOfClonedSteps"))
        m_Dataset = m_jobParams.GetParam("datasetNum")

        ' Update the account name
        m_HPCAccountName = m_jobParams.GetParam("HPCAccountName")
        If m_HPCAccountName Is Nothing OrElse m_HPCAccountName.Length = 0 Then
            m_HPCAccountName = clsAnalysisResourcesXTHPC.HPC_ACCOUNT_NAME
        End If

        ' '' Make sure the _DTA.txt file is valid
        ' ''If Not ValidateCDTAFile() Then
        ' ''    Return IJobParams.CloseOutType.CLOSEOUT_NO_DTA_FILES
        ' ''End If

        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Running XTandem")

        CmdRunner = New clsRunDosProgram(m_WorkDir)

        If m_DebugLevel > 4 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerXT.OperateAnalysisTool(): Enter")
        End If

        ' verify that program file exists
        Dim progLoc As String = m_mgrParams.GetParam("PuttyProgLoc")
        If Not System.IO.File.Exists(progLoc) Then
            If progLoc.Length = 0 Then progLoc = "Parameter 'PuttyProgLoc' not defined for this manager"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Cannot find Putty (putty.exe) program file: " & progLoc)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        ' verify that program file exists
        Dim progSftpLoc As String = m_mgrParams.GetParam("PuttySFTPProgLoc")
        If Not System.IO.File.Exists(progSftpLoc) Then
            If progLoc.Length = 0 Then progLoc = "Parameter 'PuttySFTPProgLoc' not defined for this manager"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Cannot find PuttySFTP (psftp.exe) program file: " & progLoc)
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
        'Set up and execute a program runner to run Putty program to create directories
        CmdStr = "-l " & HPC_NAME & " -m " & "CreateFastaFileList.txt"
        If Not CmdRunner.RunProgram(progLoc, CmdStr, "Putty", True) Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error running Putty to create directories on super computer, job " & m_JobNum & ", Command: " & CmdStr)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        'Set up and execute a program runner to run PuttySFTP program to get files from Supercomputer
        CmdStr = "-l " & HPC_NAME & " -b " & "GetFastaFileList.txt"
        If Not CmdRunner.RunProgram(progSftpLoc, CmdStr, "PuttySFTP", True) Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error running Putty SFTP to copy files to super computer, job " & m_JobNum & ", Command: " & CmdStr)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        'Compare fasta files to see if we need to copy it over
        If Not FastaFilesEqual() Then
            'Set up and execute a program runner to run PuttySFTP program to transfer fasta file to Supercomputer
            CmdStr = "-l " & HPC_NAME & " -b " & "PutFasta_Job" & m_JobNum
            If Not CmdRunner.RunProgram(progSftpLoc, CmdStr, "PuttySFTP", True) Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error running Putty SFTP to copy fasta file to super computer, job " & m_JobNum & ", Command: " & CmdStr)
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If
        End If

        'Set up and execute a program runner to run Putty program to create directories
        CmdStr = "-l " & HPC_NAME & " -m " & "CreateDir_Job" & m_JobNum
        If Not CmdRunner.RunProgram(progLoc, CmdStr, "Putty", True) Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error running Putty to create directories on super computer, job " & m_JobNum & ", Command: " & CmdStr)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If


        'Set up and execute a program runner to run PuttySFTP program to transfer files to Supercomputer
        For i = 1 To m_NumClonedSteps
            CmdStr = "-l " & HPC_NAME & " -b " & "PutCmds_Job" & m_JobNum & "_" & i
            If Not CmdRunner.RunProgram(progSftpLoc, CmdStr, "PuttySFTP", True) Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error running Putty SFTP to copy files to super computer, job " & m_JobNum & ", Command: " & CmdStr)
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If
        Next

        'All files have been copied, now run the command to start the jobs
        For i = 1 To m_NumClonedSteps
            CmdStr = "-l " & HPC_NAME & " -m " & "StartXT_Job" & m_JobNum & "_" & i
            If Not CmdRunner.RunProgram(progLoc, CmdStr, "Putty", True) Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error running Putty to schedule the jobs on the super computer, job " & m_JobNum & ", Command: " & CmdStr)
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If
        Next

        'Get job number from supercomputer and append to filename.
        For i = 1 To m_NumClonedSteps
            GetJobNoOutputFile_CmdFile = System.IO.Path.Combine(m_WorkDir, "GetJobOutputCmds_Job" & m_JobNum & "_" & i)
            MakeGetJobOutputFilesCmdFile(GetJobNoOutputFile_CmdFile, i.ToString)
        Next

        Dim MonitorInterval As Integer
        MonitorInterval = 10000 ' Wait 10 Seconds

        'Wait a few seconds to make sure jobs have been assigned
        System.Threading.Thread.Sleep(MonitorInterval)

        'First we need to get the output files from the msub directories
        Dim HPC_Result As IJobParams.CloseOutType = IJobParams.CloseOutType.CLOSEOUT_NO_OUT_FILES
        HPC_Result = RetrieveJobOutputFilesFromHPC(progSftpLoc)
        If HPC_Result = IJobParams.CloseOutType.CLOSEOUT_FAILED Then
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If
        System.Threading.Thread.Sleep(MonitorInterval)

        ReDim m_HPCJobStatus(m_NumClonedSteps, 1)

        'Next we need to open each output file and see if a job number was assigned.
        HPC_Result = IJobParams.CloseOutType.CLOSEOUT_NO_OUT_FILES
        While HPC_Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS
            HPC_Result = MakeGetOutputFilesCmdFile(progSftpLoc)
            If HPC_Result = IJobParams.CloseOutType.CLOSEOUT_FAILED Then
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If
            System.Threading.Thread.Sleep(MonitorInterval)
        End While

        'Build the check job command file so we can check the status of the job(s)
        HPC_Result = MakeCheckJobCmdFiles()
        If HPC_Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            'Need to cancel jobs at this point if an error occurred
            CancelHPCRunningJobs(progLoc)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        'Build the get check job command file so we can check the status of each job
        HPC_Result = MakeGetCheckjobStatusFilesCmdFile()
        If HPC_Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            'Need to cancel jobs at this point if an error occurred
            CancelHPCRunningJobs(progLoc)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        'Build the cancel job command files in case we have to cancel the jobs
        HPC_Result = MakeCancelJobFilesCmdFile()
        If HPC_Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            CancelHPCRunningJobs(progLoc)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        'Build the showq command file to check the status of all the jobs scheduled for svc-dms user
        HPC_Result = MakeShowQCmdFile()
        If HPC_Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            CancelHPCRunningJobs(progLoc)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        'Build the get showq files from HPC
        HPC_Result = MakeGetShowQResultFileCmdFile()
        If HPC_Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            CancelHPCRunningJobs(progLoc)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        'Build the gbalance command file
        HPC_Result = MakeGBalanceCmdFile()
        If HPC_Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            CancelHPCRunningJobs(progLoc)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        'Build the get gbalance command file
        HPC_Result = MakeGetBalanceFileCmdFile()
        If HPC_Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            CancelHPCRunningJobs(progLoc)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        ' Now we need to wait for the status to change to complete for each job submitted.  
        ' If status other than Completed, Idle, or Running appear, then quit job
        ' Increase MonitorInterval to 30 seconds
        MonitorInterval = 30000

        ' This where we will monitor the status of the job(s), cancel assigned jobs if an error occurs, and copy 
        ' result files if job(s) are successful.  A status could still indicate 'Complete' so we need to 
        ' check for errors. We'll also check to make sure the error file is empty
        HPC_Result = IJobParams.CloseOutType.CLOSEOUT_NO_OUT_FILES
        While HPC_Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS
            System.Threading.Thread.Sleep(MonitorInterval)
            HPC_Result = MonitorHPCJobs(progSftpLoc, progLoc)
            If HPC_Result = IJobParams.CloseOutType.CLOSEOUT_FAILED Then
                'If error is detected, the Cancel all jobs that are running.
                CancelHPCRunningJobs(progLoc)
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If
        End While

        'Now we need piece each result file together to form 1 result file.
        result = ConstructSingleXTandemResultFile()
        If result = IJobParams.CloseOutType.CLOSEOUT_FAILED Then

            ' Move the source files and any results to the Failed Job folder
            ' Useful for debugging XTandem problems
            CopyFailedResultsToArchiveFolder()

            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

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
            ' Move the source files and any results to the Failed Job folder
            ' Useful for debugging XTandem problems
            CopyFailedResultsToArchiveFolder()
            Return result
        End If

        result = MakeResultsFolder()
        If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            'TODO: What do we do here?
            Return result
        End If

        ' If we get here, we can safely delete any zero-byte .err files
        DeleteZeroByteErrorFiles()

        result = MoveResultFiles()
        If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            'TODO: What do we do here?
            ' Note that MoveResultFiles should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
            Return result
        End If

        result = CopyResultsFolderToServer()
        If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            'TODO: What do we do here?
            ' Note that CopyResultsFolderToServer should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
            Return result
        End If

        ' If we get here, we can safely delete the _i_dta.txt files from the transfer folder
        DeleteDTATextFilesInTransferFolder()


        result = RemoveHPCDirectories(progLoc)
        If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            'TODO: What do we do here?
            Return result
        End If

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS 'ZipResult

    End Function


    Protected Sub CopyFailedResultsToArchiveFolder()

        Dim result As IJobParams.CloseOutType
        Dim i As Integer

        Dim strFailedResultsFolderPath As String = m_mgrParams.GetParam("FailedResultsFolderPath")
        If String.IsNullOrEmpty(strFailedResultsFolderPath) Then strFailedResultsFolderPath = "??Not Defined??"

        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Processing interrupted; copying results to archive folder: " & strFailedResultsFolderPath)

        ' Bump up the debug level if less than 2
        If m_DebugLevel < 2 Then m_DebugLevel = 2

        ' Try to save whatever files are in the work directory (however, delete the _DTA.txt and _DTA.zip files first)
        Dim strFolderPathToArchive As String
        strFolderPathToArchive = String.Copy(m_WorkDir)

        Try
            System.IO.File.Delete(System.IO.Path.Combine(m_WorkDir, m_Dataset & "_dta.zip"))
            System.IO.File.Delete(System.IO.Path.Combine(m_WorkDir, m_Dataset & "_dta.txt"))
        Catch ex As Exception
            ' Ignore errors here
        End Try

        For i = 1 To m_NumClonedSteps
            Try
                System.IO.File.Delete(System.IO.Path.Combine(m_WorkDir, m_Dataset & "_" & i.ToString & "_dta.txt"))
            Catch ex As Exception
                ' Ignore errors here
            End Try
        Next

        ' Make the results folder
        result = MakeResultsFolder()
        If result = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            ' Move the result files into the result folder
            If result = MoveResultFiles() Then
                ' Move was a success; update strFolderPathToArchive
                strFolderPathToArchive = System.IO.Path.Combine(m_WorkDir, m_ResFolderName)
            End If
        End If

        ' Copy the results folder to the Archive folder
        Dim objAnalysisResults As clsAnalysisResults = New clsAnalysisResults(m_mgrParams, m_jobParams)
        objAnalysisResults.CopyFailedResultsToArchiveFolder(strFolderPathToArchive)

    End Sub

    Protected Function MakeGetJobOutputFilesCmdFile(ByVal inputFilename As String, ByVal File_Index As String) As Boolean
        Dim result As Boolean = True

        Try
            ' Create an instance of StreamWriter to write to a file.
            Dim swOut As System.IO.StreamWriter = New System.IO.StreamWriter(inputFilename)

            WriteUnix(swOut, "cd " & clsAnalysisXTHPCGlobals.HPC_ROOT_DIRECTORY & "/Job" & m_JobNum & "_msub" & File_Index & "/")

            WriteUnix(swOut, "get X-Tandem_Job" & m_JobNum & "_" & File_Index & ".output")

            swOut.Close()

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

        Dim JobOutputFilename As String

        Try
            For i = 1 To m_NumClonedSteps
                JobOutputFilename = "GetJobOutputCmds_Job" & m_JobNum & "_" & i
                CmdStr = "-l " & HPC_NAME & " -b " & JobOutputFilename
                If Not CmdRunner.RunProgram(progSftpLoc, CmdStr, "PuttySFTP", True) Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error running Putty SFTP to retrieve job output files from super computer, job " & m_JobNum & ", Command: " & CmdStr)
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
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error running Putty to Cancel Jobs on super computer, job " & m_JobNum & ", Command: " & CmdStr)
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
        Dim RemoveJobFilename As String
        Try
            RemoveJobFilename = System.IO.Path.Combine(m_WorkDir, "Remove_Job" & m_JobNum)
            CmdStr = "-l " & HPC_NAME & " -m " & RemoveJobFilename
            If Not CmdRunner.RunProgram(progLoc, CmdStr, "Putty", True) Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error running Putty to Remove Job Directories on super computer, job " & m_JobNum & ", Command: " & CmdStr)
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisResourcesXTHPC.RemoveHPCDirectories, An error occurred while trying to remove job directories on super computer" & ex.Message)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    Protected Function MakeCancelJobFilesCmdFile() As IJobParams.CloseOutType

        Dim InputFilename As String = System.IO.Path.Combine(m_WorkDir, "Cancel_Job" & m_JobNum)

        Dim i As Integer

        Try
            clsGlobal.m_FilesToDeleteExt.Add(System.IO.Path.GetFileName(InputFilename))


            ' Create an instance of StreamWriter to write to a file.
            Dim swOut As System.IO.StreamWriter = New System.IO.StreamWriter(InputFilename)

            WriteUnix(swOut, "cd " & clsAnalysisXTHPCGlobals.HPC_ROOT_DIRECTORY)

            For i = 1 To m_NumClonedSteps

                WriteUnix(swOut, "canceljob " & m_HPCJobStatus(i, 0))

            Next

            swOut.Close()

        Catch E As Exception
            ' Let the user know what went wrong.
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisResourcesXTHPC.BuildCancelJobFilesCmdFile, The Cancel jobs file could not be written: " & E.Message)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try


        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    ''' <summary>
    ''' 
    ''' </summary>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Protected Function MakeGetOutputFilesCmdFile(ByVal progSftpLoc As String) As IJobParams.CloseOutType
        Dim Get_CmdFile As String
        Dim i As Integer

        Dim HPC_JobNum As String

        Try
            For i = 1 To m_NumClonedSteps
                HPC_JobNum = RetrieveHPCJobNumber(i.ToString)
                If Not String.IsNullOrEmpty(HPC_JobNum) AndAlso IsNumeric(HPC_JobNum) Then
                    Get_CmdFile = System.IO.Path.Combine(m_WorkDir, "GetResultFilesCmds_Job" & m_JobNum & "_" & i)
                    MakeGetOutputFilesCmdFile(Get_CmdFile, HPC_JobNum, CStr(i))
                    clsGlobal.m_FilesToDeleteExt.Add(System.IO.Path.GetFileName(Get_CmdFile))
                    m_HPCJobStatus(i, 0) = HPC_JobNum
                    m_HPCJobStatus(i, 1) = "scheduled"
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


    Private Function MonitorHPCJobs(ByVal progSftpLoc As String, ByVal progloc As String) As IJobParams.CloseOutType
        Dim i As Integer

        Dim intStepsCompleted As Integer = 0

        Try

            'Now Read the ShowQ_Jobxxxx file to update status
            UpdateJobStatus(progSftpLoc, progloc)

            'Now we need to determine what to do based on the status of the job
            For i = 1 To m_NumClonedSteps
                Select Case m_HPCJobStatus(i, 1).ToLower

                    Case "idle", "scheduled"  'Job hasn't started
                        'Update the status using checkjob
                        UpdateCheckJobStatus(progSftpLoc, progloc, i)

                    Case "blocked"  'Job is blocked
                        'GetBalance to see number of hours remaining
                        GetCurrentBalance(progSftpLoc, progloc, i)

                    Case "batchhold", "systemhold", "userhold"  'Job is on hold so break since we can be waiting indefinitely
                        'Perform clean up here
                        UpdateCheckJobStatus(progSftpLoc, progloc, i)

                    Case "starting", "running", "notfound"  'Job is running or no longer in list, we can now use checkjob
                        UpdateCheckJobStatus(progSftpLoc, progloc, i)

                    Case "removed", "vacated"  'Job is done, now we can check for results
                        ' set job to exit
                        m_HPCJobStatus(i, 1) = "exit"

                    Case "completed"  'Job is done, now we can check for results
                        RetrieveJobResultFilesFromHPC(progSftpLoc, i)

                    Case Else
                        'Unknown state, try to update the status using checkjob
                        UpdateCheckJobStatus(progSftpLoc, progloc, i)

                End Select

            Next

            'Now check to see if all HPC job statuses are complete
            For i = 1 To m_NumClonedSteps
                If m_DebugLevel >= 4 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "... Current state for step " & i.ToString & ": " & m_HPCJobStatus(i, 1))
                End If

                If m_HPCJobStatus(i, 1).ToLower = "done" Then
                    intStepsCompleted += 1

                ElseIf m_HPCJobStatus(i, 1).ToLower = "exit" Then
                    'This may be where we want to exit if in an "unwanted" state
                    If m_DebugLevel >= 1 Then
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Step " & i & " has state " & m_HPCJobStatus(i, 1) & "; will abort processing")
                    End If
                End If
            Next

            If intStepsCompleted >= m_NumClonedSteps Then
                If m_DebugLevel >= 2 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "clsAnalysisResourcesXTHPC.MonitorHPCJobs, HPC processing is now complete")
                End If

                Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
            Else
                ' Need to continue processing
                Return IJobParams.CloseOutType.CLOSEOUT_NO_DATA
            End If

        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisResourcesXTHPC.MonitorHPCJobs, An error occurred while trying to retrieve job result files" & ex.Message)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        Return IJobParams.CloseOutType.CLOSEOUT_NO_DATA

    End Function

    ''' <summary>
    ''' 
    ''' </summary>
    ''' <param name="progSftpLoc"></param>
    ''' <param name="progloc"></param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Private Function GetCurrentBalance(ByVal progSftpLoc As String, ByVal progloc As String, ByVal File_Index As Integer) As IJobParams.CloseOutType

        Static dtLastWarnTimeEmptyFile As DateTime

        Dim CmdStr As String

        Dim CommandfileName As String

        Dim HPCgBalanceFilePath As String

        Dim srReader As System.IO.StreamReader

        Dim lineText As String

        Dim strAccountName As String = String.Empty
        Dim sngBalanceHours As Single

        Dim LineCnt As Integer = 1

        Dim HPCBalanceThresholdValueHours As Single = 5

        Try
            HPCgBalanceFilePath = System.IO.Path.Combine(m_WorkDir, "ShowBalance_Job" & m_JobNum & ".txt")

            CommandfileName = "CreateShowBalance_Job" & m_JobNum
            CmdStr = "-l " & HPC_NAME & " -m " & CommandfileName
            If Not CmdRunner.RunProgram(progloc, CmdStr, "Putty", True) Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error running Putty to create Queue file from super computer, job " & m_JobNum & ", Command: " & CmdStr)
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            'Make sure showqresults file was created.
            System.Threading.Thread.Sleep(15000)

            CommandfileName = "GetBalance_Job" & m_JobNum
            CmdStr = "-l " & HPC_NAME & " -b " & CommandfileName
            If Not CmdRunner.RunProgram(progSftpLoc, CmdStr, "PuttySFTP", True) Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error running Putty SFTP to get Queue file from super computer, job " & m_JobNum & ", Command: " & CmdStr)
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            If System.IO.File.Exists(HPCgBalanceFilePath) Then

                Dim fiQResultsLocal As New System.IO.FileInfo(HPCgBalanceFilePath)

                If fiQResultsLocal.Length > 0 Then

                    srReader = New System.IO.StreamReader(New System.IO.FileStream(HPCgBalanceFilePath, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.ReadWrite))

                    Do While srReader.Peek >= 0
                        lineText = srReader.ReadLine.Trim

                        ' Skip the first two lines (they are header lines)
                        If lineText.Length > 0 AndAlso LineCnt > 2 Then
                            ' Parse this to determine the account name and available hours

                            
                            sngBalanceHours = RetrieveGBalanceData(lineText, strAccountName)
                            If sngBalanceHours < 0 Then
                                ' Error parsing out the available hours; a warning has already been logged
                            Else
                                If strAccountName = "Idle on MPP3" Then
                                    ' Need to override this name
                                    strAccountName = "mscfidle"
                                End If

                                If strAccountName.ToLower = m_HPCAccountName.ToLower AndAlso _
                                   sngBalanceHours < HPCBalanceThresholdValueHours Then

                                    '  EMSL user proposal m_HPCAccountName, which is associated with the svc-dms user, has fewer than HPCBalanceThresholdValueHours CPU-hours remaining

                                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisResourcesXTHPC.GetCurrentBalance, Remaining balance for account " & strAccountName & " is less than " & HPCBalanceThresholdValueHours & " hours; aborting job")
                                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisResourcesXTHPC.GetCurrentBalance, GBalance data: " & lineText)
                                    m_HPCJobStatus(File_Index, 1) = "exit"

                                End If
                            End If
                        End If
                        LineCnt += 1
                    Loop

                    srReader.Close()
                Else
                    ' File is empty
                    ' This might mean there is no time left, but it could also indicate another error
                    ' Will continue waiting, but post a log message
                    If System.DateTime.Now.Subtract(dtLastWarnTimeEmptyFile).TotalMinutes >= 5 Then
                        dtLastWarnTimeEmptyFile = System.DateTime.Now
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "clsAnalysisResourcesXTHPC.GetCurrentBalance, GBalance result file is empty; this likely indicates no hours are available for HPC account " & m_HPCAccountName)
                    End If
                End If

            End If

        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisResourcesXTHPC.GetCurrentBalance, An error occurred while trying to gbalance status file" & ex.Message)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function


    ''' <summary>
    ''' Reads the CheckJobStatus file for job m_HPCJobStatus(indexNum, 0))
    ''' </summary>
    ''' <param name="progSftpLoc"></param>
    ''' <param name="progloc"></param>
    ''' <param name="indexNum"></param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Private Function UpdateCheckJobStatus(ByVal progSftpLoc As String, ByVal progloc As String, ByVal indexNum As Integer) As Boolean
        Dim CmdStr As String

        Dim CommandfileName As String

        Dim HPCCheckjobFilePath As String

        Dim srReader As System.IO.StreamReader

        Dim lineText As String

        Dim HPCStatus As String

        Try

            HPCCheckjobFilePath = System.IO.Path.Combine(m_WorkDir, "CheckjobStatus_" & m_HPCJobStatus(indexNum, 0))

            CommandfileName = "CreateCheckjob_Job" & m_JobNum & "_" & indexNum.ToString
            CmdStr = "-l " & HPC_NAME & " -m " & CommandfileName
            If Not CmdRunner.RunProgram(progloc, CmdStr, "Putty", True) Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error running Putty to create CheckJob file from super computer, job " & m_JobNum & ", Command: " & CmdStr)
                Return False
            End If

            'Make sure checkjob file was created.
            System.Threading.Thread.Sleep(15000)

            CommandfileName = "GetCheckjob_HPCJob" & m_HPCJobStatus(indexNum, 0)
            CmdStr = "-l " & HPC_NAME & " -b " & CommandfileName
            If Not CmdRunner.RunProgram(progSftpLoc, CmdStr, "PuttySFTP", True) Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error running Putty SFTP to get CheckJob file from super computer, job " & m_JobNum & ", Command: " & CmdStr)
                Return False
            End If

            If System.IO.File.Exists(HPCCheckjobFilePath) Then

                Dim fiQResultsLocal As New System.IO.FileInfo(HPCCheckjobFilePath)

                If fiQResultsLocal.Length > 0 Then

                    srReader = New System.IO.StreamReader(New System.IO.FileStream(HPCCheckjobFilePath, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.ReadWrite))

                    Do While srReader.Peek >= 0
                        lineText = srReader.ReadLine.Trim
                        If lineText.Length > 0 AndAlso lineText.StartsWith("State:") Then
                            HPCStatus = lineText.Substring(6)
                            m_HPCJobStatus(indexNum, 1) = HPCStatus.Trim
                            Exit Do
                        End If
                    Loop

                    srReader.Close()

                End If

            End If

        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisResourcesXTHPC.UpdateCheckJobStatus, An error occurred while trying to retrieve job status files" & ex.Message)
            Return False
        End Try

        Return True

    End Function


    ''' <summary>
    ''' Reads the ShowQ results file to parse out the jobs in the queue
    ''' </summary>
    ''' <param name="progSftpLoc"></param>
    ''' <param name="progloc"></param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Private Function UpdateJobStatus(ByVal progSftpLoc As String, ByVal progloc As String) As IJobParams.CloseOutType
        Dim CmdStr As String

        Dim CommandfileName As String

        Dim HPCQueueFilePath As String

        Dim i As Integer

        Dim srReader As System.IO.StreamReader

        Dim lineText As String

        Dim intHPCJobNum As Integer
        Dim strHPCStatus As String

        Try
            HPCQueueFilePath = System.IO.Path.Combine(m_WorkDir, "ShowQ_ResultsJob" & m_JobNum & ".txt")

            CommandfileName = "CreateShowQ_Job" & m_JobNum
            CmdStr = "-l " & HPC_NAME & " -m " & CommandfileName
            If Not CmdRunner.RunProgram(progloc, CmdStr, "Putty", True) Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error running Putty to create Queue file from super computer, job " & m_JobNum & ", Command: " & CmdStr)
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            'Make sure showqresults file was created.
            System.Threading.Thread.Sleep(15000)

            CommandfileName = "GetShowQResults_Job" & m_JobNum
            CmdStr = "-l " & HPC_NAME & " -b " & CommandfileName
            If Not CmdRunner.RunProgram(progSftpLoc, CmdStr, "PuttySFTP", True) Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error running Putty SFTP to get Queue file from super computer, job " & m_JobNum & ", Command: " & CmdStr)
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            If System.IO.File.Exists(HPCQueueFilePath) Then

                Dim fiQResultsLocal As New System.IO.FileInfo(HPCQueueFilePath)

                If fiQResultsLocal.Length > 0 Then

                    srReader = New System.IO.StreamReader(New System.IO.FileStream(HPCQueueFilePath, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.ReadWrite))

                    Do While srReader.Peek >= 0
                        lineText = srReader.ReadLine.Trim
                        If lineText.Length > 0 Then
                            strHPCStatus = RetrieveJobQueueStatus(lineText, intHPCJobNum)

                            If intHPCJobNum > 0 Then
                                For i = 1 To m_NumClonedSteps
                                    If intHPCJobNum.ToString = m_HPCJobStatus(i, 0) Then
                                        ' Update the status
                                        m_HPCJobStatus(i, 1) = strHPCStatus
                                    End If
                                Next
                            End If
                        End If
                    Loop

                    srReader.Close()
                Else
                    'Check to make sure a job is not stuck in "idle" or "Scheduled"
                    'If in that state and file is empty, set to running so checkjob can be used
                    For i = 1 To m_NumClonedSteps
                        If m_HPCJobStatus(i, 1).ToLower = "idle" Or m_HPCJobStatus(i, 1).ToLower = "scheduled" Then
                            m_HPCJobStatus(i, 1) = "running"
                        End If
                    Next

                End If

            End If

        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisResourcesXTHPC.UpdateJobStatus, An error occurred while trying to retrieve job status file: " & ex.Message)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    Private Function RetrieveJobResultFilesFromHPC(ByVal progSftpLoc As String, ByVal CloneStepNum As Integer) As IJobParams.CloseOutType
        Dim CmdStr As String

        Dim CommandfileName As String

        Dim JobOutputFilePath As String
        Dim ErrorResultFilePath As String
        Dim XTResultsFilePath As String

        Dim fiResultsFile As System.IO.FileInfo

        Dim dtTransferStartTime As System.DateTime

        Dim sngFileSizeMB As Single
        Dim sngTransferTimeSeconds As Single
        Dim sngTransferRate As Single

        Dim strErrorResult As String

        Try

            If m_DebugLevel >= 1 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "clsAnalysisResourcesXTHPC.RetrieveJobResultFilesFromHPC, retrieving results from HPC for step " & CloneStepNum)
            End If

            dtTransferStartTime = System.DateTime.Now

            CommandfileName = "GetResultFilesCmds_Job" & m_JobNum & "_" & CloneStepNum
            CmdStr = "-l " & HPC_NAME & " -b " & CommandfileName
            If Not CmdRunner.RunProgram(progSftpLoc, CmdStr, "PuttySFTP", True) Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error running Putty SFTP to get results files from super computer, job " & m_JobNum & ", Command: " & CmdStr)
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            ' Define the file paths
            JobOutputFilePath = System.IO.Path.Combine(m_WorkDir, m_JobNum & "_Part" & CloneStepNum & ".output." & m_HPCJobStatus(CloneStepNum, 0))
            ErrorResultFilePath = System.IO.Path.Combine(m_WorkDir, m_JobNum & "_Part" & CloneStepNum & ".err." & m_HPCJobStatus(CloneStepNum, 0))
            XTResultsFilePath = System.IO.Path.Combine(m_WorkDir, m_Dataset & "_" & CloneStepNum & "_xt.xml")

            ' Make sure the Results file exists
            fiResultsFile = New System.IO.FileInfo(XTResultsFilePath)

            If Not fiResultsFile.Exists Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisResourcesXTHPC.RetrieveJobResultFilesFromHPC, X!Tandem results file not found: " & XTResultsFilePath)
                Return IJobParams.CloseOutType.CLOSEOUT_NO_OUT_FILES
            End If

            If m_DebugLevel >= 1 Then
                ' Log the transfer stats
                sngFileSizeMB = CSng(fiResultsFile.Length / 1024.0 / 1024.0)
                sngTransferTimeSeconds = CSng(System.DateTime.Now.Subtract(dtTransferStartTime).TotalSeconds)

                If sngTransferTimeSeconds > 0 Then
                    sngTransferRate = sngFileSizeMB / sngTransferTimeSeconds
                Else
                    sngTransferRate = 0
                End If

                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "... X!Tandem results file retrieved from HPC; " & sngFileSizeMB.ToString("0.0") & " MB transferred in " & sngTransferTimeSeconds.ToString("0.0") & " seconds (" & sngTransferRate.ToString("0.00") & " MB/sec)")
            End If

            If Not System.IO.File.Exists(JobOutputFilePath) Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisResourcesXTHPC.RetrieveJobResultFilesFromHPC, Job output file not found: " & JobOutputFilePath)
                Return IJobParams.CloseOutType.CLOSEOUT_NO_OUT_FILES
            End If

            If Not System.IO.File.Exists(ErrorResultFilePath) Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisResourcesXTHPC.RetrieveJobResultFilesFromHPC, Error result file not found: " & ErrorResultFilePath)
                Return IJobParams.CloseOutType.CLOSEOUT_NO_OUT_FILES
            Else
                Dim fiErrorLocal As New System.IO.FileInfo(ErrorResultFilePath)
                If fiErrorLocal.Length > 0 Then
                    '***********Log error to log file here***********
                    strErrorResult = ReadEntireFile(ErrorResultFilePath)
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error file " & ErrorResultFilePath & " contains the following error: " & strErrorResult)
                    m_HPCJobStatus(CloneStepNum, 1) = "error"
                    Return IJobParams.CloseOutType.CLOSEOUT_FAILED
                End If
            End If

        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisResourcesXTHPC.RetrieveJobResultFilesFromHPC, An error occurred while trying to retrieve job result files" & ex.Message)
            m_HPCJobStatus(CloneStepNum, 1) = "error"
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        m_HPCJobStatus(CloneStepNum, 1) = "done"
        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    Protected Function RetrieveHPCJobNumber(ByVal File_Index As String) As String
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
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisResourcesXTHPC.GetHPCJobNumber, Output file was not found: " & HPC_OutputFilename)
            Return ""
        End If

        Return HPC_JobNum

    End Function

    Protected Function ReadEntireFile(ByVal Filename As String) As String
        Dim HPC_JobError As String = ""

        Try
            If System.IO.File.Exists(Filename) Then
                Dim srReader As System.IO.StreamReader = New System.IO.StreamReader(Filename)
                HPC_JobError = srReader.ReadToEnd
            End If
        Catch E As Exception
            ' Let the user know what went wrong.
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisResourcesXTHPC.ReadEntireFile, Error reading file '" & Filename & "': " & E.Message)
            Return E.Message
        End Try

        Return HPC_JobError

    End Function

    ''' <summary>
    ''' 
    ''' </summary>
    ''' <remarks></remarks>
    Protected Sub DeleteZeroByteErrorFiles()
        Dim ioFolder As System.IO.DirectoryInfo
        Dim ioFileInfo As System.IO.FileInfo

        Try
            ioFolder = New System.IO.DirectoryInfo(m_WorkDir)
            For Each ioFileInfo In ioFolder.GetFiles("*.err.*")
                If ioFileInfo.Length = 0 Then
                    ioFileInfo.Delete()
                End If
            Next
        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "clsAnalysisResourcesXTHPC.DeleteZeroByteErrorFiles, Error deleting file: " & ex.Message)
        End Try

    End Sub

    ''' <summary>
    ''' 
    ''' </summary>
    ''' <remarks></remarks>
    Protected Sub DeleteDTATextFilesInTransferFolder()

        Dim strTransferFolderPath As String
        Dim strDtaFilePath As String

        Dim i As Integer
        Dim ioFileInfo As System.IO.FileInfo

        Try
            strTransferFolderPath = System.IO.Path.Combine(m_jobParams.GetParam("transferFolderPath"), m_Dataset)
            strTransferFolderPath = System.IO.Path.Combine(strTransferFolderPath, m_ResFolderName)

            ' Now that we have successfully concatenated things, delete each of the input files
            For i = 1 To m_NumClonedSteps

                strDtaFilePath = System.IO.Path.Combine(strTransferFolderPath, m_Dataset & "_" & i.ToString & "_dta.txt")

                ioFileInfo = New System.IO.FileInfo(strDtaFilePath)
                If ioFileInfo.Exists Then
                    ioFileInfo.Delete()
                Else
                    If m_DebugLevel >= 1 Then
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "clsAnalysisResourcesXTHPC.DeleteDTATextFilesInTransferFolder, File not found; unable to delete it: " & strDtaFilePath)
                    End If
                End If

            Next i

        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "clsAnalysisResourcesXTHPC.DeleteDTATextFilesInTransferFolder, Error deleting file: " & ex.Message)
        End Try

    End Sub

    ''' <summary>
    ''' 
    ''' </summary>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Protected Function MakeShowQCmdFile() As IJobParams.CloseOutType
        Dim result As Boolean = True

        Dim InputFilename As String = System.IO.Path.Combine(m_WorkDir, "CreateShowQ_Job" & m_JobNum)

        Try
            clsGlobal.m_FilesToDeleteExt.Add(System.IO.Path.GetFileName(InputFilename))

            ' Create an instance of StreamWriter to write to a file.
            Dim swOut As System.IO.StreamWriter = New System.IO.StreamWriter(InputFilename)

            WriteUnix(swOut, "/apps/moab/current/bin/showq | grep ""svc-dms"" > " & clsAnalysisXTHPCGlobals.HPC_ROOT_DIRECTORY & "Job" & m_JobNum & "_msub1" & "/" & "ShowQ_ResultsJob" & m_JobNum & ".txt")

            swOut.Close()

        Catch E As Exception
            ' Let the user know what went wrong.
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisResourcesXTHPC.MakeShowQCmdFile, The file could not be written: " & E.Message)
            result = False
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
    End Function

    ''' <summary>
    ''' Makes the file to obtain the balance for the svc-dms user's account
    ''' </summary>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Protected Function MakeGBalanceCmdFile() As IJobParams.CloseOutType
        Dim result As Boolean = True

        Dim InputFilename As String = System.IO.Path.Combine(m_WorkDir, "CreateShowBalance_Job" & m_JobNum)

        Try
            clsGlobal.m_FilesToDeleteExt.Add(System.IO.Path.GetFileName(InputFilename))

            ' Create an instance of StreamWriter to write to a file.
            Dim swOut As System.IO.StreamWriter = New System.IO.StreamWriter(InputFilename)

            WriteUnix(swOut, "/mscf/mscf/gold/bin/gbalance -u svc-dms -h > " & clsAnalysisXTHPCGlobals.HPC_ROOT_DIRECTORY & "Job" & m_JobNum & "_msub1" & "/" & "Show_BalanceJob" & m_JobNum & ".txt")

            swOut.Close()

        Catch E As Exception
            ' Let the user know what went wrong.
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisResourcesXTHPC.MakeGetBalanceCmdFile, The file could not be written: " & E.Message)
            result = False
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
    End Function

    ''' <summary>
    ''' Retrives the file containing the balance for the user svc-dms
    ''' </summary>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Protected Function MakeGetBalanceFileCmdFile() As IJobParams.CloseOutType
        Dim result As Boolean = True

        Dim InputFilename As String = System.IO.Path.Combine(m_WorkDir, "GetBalance_Job" & m_JobNum)

        Try
            clsGlobal.m_FilesToDeleteExt.Add(System.IO.Path.GetFileName(InputFilename))

            ' Create an instance of StreamWriter to write to a file.
            Dim swOut As System.IO.StreamWriter = New System.IO.StreamWriter(InputFilename)

            WriteUnix(swOut, "cd " & clsAnalysisXTHPCGlobals.HPC_ROOT_DIRECTORY & "Job" & m_JobNum & "_msub1" & "/")

            WriteUnix(swOut, "get " & "ShowBalance_Job" & m_JobNum & ".txt")

            swOut.Close()

        Catch E As Exception
            ' Let the user know what went wrong.
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisResourcesXTHPC.MakeGetBalanceFileCmdFile, The file could not be written: " & E.Message)
            result = False
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
    End Function

    ''' <summary>
    ''' 
    ''' </summary>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Protected Function MakeGetShowQResultFileCmdFile() As IJobParams.CloseOutType
        Dim result As Boolean = True

        Dim InputFilename As String = System.IO.Path.Combine(m_WorkDir, "GetShowQResults_Job" & m_JobNum)

        Try
            clsGlobal.m_FilesToDeleteExt.Add(System.IO.Path.GetFileName(InputFilename))

            ' Create an instance of StreamWriter to write to a file.
            Dim swOut As System.IO.StreamWriter = New System.IO.StreamWriter(InputFilename)

            WriteUnix(swOut, "cd " & clsAnalysisXTHPCGlobals.HPC_ROOT_DIRECTORY & "Job" & m_JobNum & "_msub1" & "/")

            WriteUnix(swOut, "get " & "ShowQ_ResultsJob" & m_JobNum & ".txt")

            swOut.Close()

        Catch E As Exception
            ' Let the user know what went wrong.
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisResourcesXTHPC.MakeGetShowQResultFileCmdFile, The file could not be written: " & E.Message)
            result = False
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

        Try
            ' Create an instance of StreamWriter to write to a file.
            Dim swOut As System.IO.StreamWriter = New System.IO.StreamWriter(inputFilename)

            WriteUnix(swOut, "cd " & clsAnalysisXTHPCGlobals.HPC_ROOT_DIRECTORY & "Job" & m_JobNum & "_" & File_Index & "/")

            WriteUnix(swOut, "get " & m_JobNum & "_Part" & File_Index & ".output." & HPCJobNumber)

            WriteUnix(swOut, "get " & m_JobNum & "_Part" & File_Index & ".err." & HPCJobNumber)

            WriteUnix(swOut, "get " & m_Dataset & "_" & File_Index & "_xt.xml")

            WriteUnix(swOut, "get " & "XTandem_Processing_Log.txt")

            swOut.Close()

        Catch E As Exception
            ' Let the user know what went wrong.
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisResourcesXTHPC.MakeGetOutputFilesCmdFile, The file could not be written: " & E.Message)
            result = False
            Return result
        End Try

        Return result
    End Function

    ''' <summary>
    ''' 
    ''' </summary>
    ''' <param name="swOut"></param>
    ''' <remarks></remarks>
    Protected Sub WriteUnix(ByRef swOut As System.IO.StreamWriter)
        WriteUnix(swOut, String.Empty)
    End Sub

    ''' <summary>
    ''' 
    ''' </summary>
    ''' <param name="swOut"></param>
    ''' <param name="inputString"></param>
    ''' <remarks></remarks>
    Protected Sub WriteUnix(ByRef swOut As System.IO.StreamWriter, ByVal inputString As String)

        swOut.Write(inputString & ControlChars.Lf)

    End Sub

    ''' <summary>
    ''' 
    ''' </summary>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Protected Function FastaFilesEqual() As Boolean
        Dim result As Boolean = False

        Dim OrgDBName As String = m_jobParams.GetParam("generatedFastaName")

        Dim LocalOrgDBFolder As String = m_mgrParams.GetParam("orgdbdir")

        Dim fastaSizeLocal As String

        Dim fastaSizeHPC As String

        Try
            If System.IO.File.Exists(System.IO.Path.Combine(m_WorkDir, "fastafiles.txt")) Then
                Dim fiFastaLocal As New System.IO.FileInfo(System.IO.Path.Combine(LocalOrgDBFolder, OrgDBName))
                fastaSizeLocal = fiFastaLocal.Length.ToString

                'Check to see if file is empty or less than 100 Mb.  If so, just copy fasta
                Dim fiFastaLocalHPC As New System.IO.FileInfo(System.IO.Path.Combine(m_WorkDir, "fastafiles.txt"))
                If (fiFastaLocalHPC.Length = 0) Or (fiFastaLocal.Length < 100000000) Then
                    Return False
                End If

                ' Create an instance of StreamWriter to read from a file.
                Dim listFile As System.IO.StreamReader = New System.IO.StreamReader(System.IO.Path.Combine(m_WorkDir, "fastafiles.txt"))
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
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisResourcesXTHPC.FastaFilesEqual, The file could not be read: " & E.Message)
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
            strInputFilePath = System.IO.Path.Combine(m_WorkDir, m_Dataset & "_dta.txt")

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

    ''' <summary>
    ''' 
    ''' </summary>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Protected Function ConstructSingleXTandemResultFile() As IJobParams.CloseOutType

        Const DELETE_INPUT_FILES As Boolean = False

        Dim lineText As String

        Dim swConcatenatedResultFile As System.IO.StreamWriter
        Dim srCurrentResultFile As System.IO.StreamReader

        Dim strFilePath As String = String.Empty
        Dim ioFileInfo As System.IO.FileInfo

        Dim intLinesProcessed As Integer
        Dim StopWriting As Boolean = False
        Dim CurrentMaxNum As Integer
        Dim NewMaxNum As Integer
        Dim OriginalGroupID As Integer

        Dim NewIDMatchText As String = ""
        Dim NewIDReplaceText As String = ""

        Dim NewLabelMatchText As String = ""
        Dim NewLabelReplaceText As String = ""

        Dim EndOfFile As System.Collections.Generic.List(Of String) = New System.Collections.Generic.List(Of String)
        Dim CachedPerformanceParameters As System.Collections.Generic.List(Of String) = New System.Collections.Generic.List(Of String)

        Dim i As Integer
        Dim j As Integer
        Dim k As Integer

        Try

            swConcatenatedResultFile = New System.IO.StreamWriter(System.IO.Path.Combine(m_WorkDir, m_Dataset & "_xt.xml"))

            ' Note: even if m_NumClonedSteps = 1, we want to read and write the file to convert the line feeds to CRLF

            For i = 1 To m_NumClonedSteps
                srCurrentResultFile = New System.IO.StreamReader(System.IO.Path.Combine(m_WorkDir, m_Dataset & "_" & i & "_xt.xml"))

                intLinesProcessed = 0
                StopWriting = False

                NewIDMatchText = String.Empty
                NewLabelMatchText = String.Empty

                Do While srCurrentResultFile.Peek >= 0
                    If i > 1 Then
                        lineText = srCurrentResultFile.ReadLine
                        If intLinesProcessed < 3 Then
                            ' Skip the first 3 lines
                        Else
                            If lineText.Contains("<group label=""input parameters""") Then
                                StopWriting = True
                            Else
                                If lineText.Contains("<group id=""") Then
                                    OriginalGroupID = RetrieveGroupIDNumber(lineText)
                                    NewMaxNum = ComputeNewMaxNumber(OriginalGroupID, NewMaxNum, CurrentMaxNum)

                                    lineText = lineText.Replace("<group id=""" & OriginalGroupID.ToString, "<group id=""" & (OriginalGroupID + CurrentMaxNum).ToString)

                                    NewIDMatchText = "id=""" & OriginalGroupID.ToString
                                    NewIDReplaceText = "id=""" & (OriginalGroupID + CurrentMaxNum).ToString

                                    NewLabelMatchText = "label=""" & OriginalGroupID.ToString
                                    NewLabelReplaceText = "label=""" & (OriginalGroupID + CurrentMaxNum).ToString
                                End If

                                If NewLabelMatchText.Length > 0 Then
                                    FindAndReplace(lineText, NewLabelMatchText, NewLabelReplaceText)
                                End If

                                If NewIDMatchText.Length > 0 Then
                                    FindAndReplace(lineText, NewIDMatchText, NewIDReplaceText)
                                End If

                                swConcatenatedResultFile.WriteLine(lineText)
                            End If
                        End If
                        intLinesProcessed += 1

                        If StopWriting Then
                            ' The "input parameters" section has been reached
                            ' Read forward until we find the "performance parameters" line
                            ' Once that line is found, read forward until </group> is found
                            ' Cache this info in CachedPerformanceParameters()
                            ' When caching, we'll update the label for "performance parameters" to be "performance parameters, part i"

                            CachedPerformanceParameters.Add(ReadXTandemPerformanceParameters(srCurrentResultFile, i)) '(i)
                        End If
                    Else
                        lineText = srCurrentResultFile.ReadLine
                        If StopWriting Then
                            ' Cache this line in EndOfFile
                            EndOfFile.Add(lineText)
                        Else
                            If lineText.Contains("<group label=""input parameters""") Then
                                StopWriting = True
                            Else
                                If m_NumClonedSteps > 1 Then
                                    If lineText.Contains("<group id=""") Then
                                        CurrentMaxNum = ComputeNewMaxNumber(lineText, CurrentMaxNum, 0)
                                    End If
                                End If
                            End If

                            If StopWriting Then
                                ' Cache this line in EndOfFile
                                EndOfFile.Add(lineText)
                            Else
                                ' Append this line to the output file
                                swConcatenatedResultFile.WriteLine(lineText)
                            End If
                        End If
                    End If
                Loop

                srCurrentResultFile.Close()

                If i > 1 Then
                    CurrentMaxNum = NewMaxNum
                End If
            Next

            ' Now write out the contents in EndOfFile to swConcatenatedResultFile
            For j = 0 To EndOfFile.Count - 1
                If m_NumClonedSteps > 1 AndAlso EndOfFile(j).IndexOf("</bioml>") >= 0 Then
                    ' Write out the cached Performance Parameter values for the other parts
                    For k = 0 To CachedPerformanceParameters.Count - 1
                        swConcatenatedResultFile.WriteLine(CachedPerformanceParameters(k))
                    Next
                End If
                swConcatenatedResultFile.WriteLine(EndOfFile(j))
            Next
            swConcatenatedResultFile.Close()

            If DELETE_INPUT_FILES OrElse m_NumClonedSteps = 1 Then

                ' Now that we have successfully concatenated things, delete each of the input files
                For i = 1 To m_NumClonedSteps
                    strFilePath = System.IO.Path.Combine(m_WorkDir, m_Dataset & "_" & i.ToString & "_xt.xml")
                    ioFileInfo = New System.IO.FileInfo(strFilePath)

                    Try
                        ioFileInfo.Delete()
                    Catch ex As Exception
                        ' Log the error, but continue
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN, "clsAnalysisToolRunnerXT.ConstructSingleXTandemResultFile, Error deleting '" & strFilePath & "': " & ex.Message)
                    End Try

                Next i
            End If

        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerXT.ConstructSingleXTandemResultFile, Error concatenating _xt.xml files: " & ex.Message)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    ''' <summary>
    ''' 
    ''' </summary>
    ''' <param name="lineText"></param>
    ''' <param name="strOldValue"></param>
    ''' <param name="strNewValue"></param>
    ''' <remarks></remarks>
    Protected Sub FindAndReplace(ByRef lineText As String, ByRef strOldValue As String, ByRef strNewValue As String)
        Dim intMatchIndex As Integer

        intMatchIndex = lineText.IndexOf(strOldValue)

        If intMatchIndex > 0 Then
            lineText = lineText.Substring(0, intMatchIndex) + strNewValue + lineText.Substring(intMatchIndex + strOldValue.Length)
        ElseIf intMatchIndex = 0 Then
            lineText = strNewValue + lineText.Substring(intMatchIndex + strOldValue.Length)
        End If
    End Sub

    ''' <summary>
    ''' 
    ''' </summary>
    ''' <param name="srCurrentResultFile"></param>
    ''' <param name="intSegment"></param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Protected Function ReadXTandemPerformanceParameters(ByRef srCurrentResultFile As System.IO.StreamReader, ByVal intSegment As Integer) As String
        Const PERF_PARAMS As String = "label=""performance parameters"

        Dim sbCache As System.Text.StringBuilder
        Dim lineText As String
        Dim blnCacheLines As Boolean

        sbCache = New System.Text.StringBuilder

        Do While srCurrentResultFile.Peek >= 0

            lineText = srCurrentResultFile.ReadLine

            If Not blnCacheLines Then
                If lineText.Contains(PERF_PARAMS) Then
                    ' Update this line to show the segment number, then start caching
                    lineText = lineText.Replace(PERF_PARAMS, PERF_PARAMS & ", part " & intSegment.ToString)

                    sbCache.AppendLine(lineText)
                    blnCacheLines = True
                End If
            Else
                If lineText.Contains("</bioml>") Then
                    ' Do not cache this line; stop reading the file
                    Exit Do
                Else
                    sbCache.AppendLine(lineText)
                End If
            End If
        Loop

        Return sbCache.ToString

    End Function

    ''' <summary>
    ''' 
    ''' </summary>
    ''' <param name="LineOfText"></param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Protected Function RetrieveGroupIDNumber(ByVal LineOfText As String) As Integer

        Static reMatchNum As System.Text.RegularExpressions.Regex

        Dim objMatch As System.Text.RegularExpressions.Match
        Dim intGroupdID As Integer

        Dim strErrorMessage As String

        If reMatchNum Is Nothing Then
            reMatchNum = New System.Text.RegularExpressions.Regex("<group id=""(\d+)""", Text.RegularExpressions.RegexOptions.IgnoreCase Or Text.RegularExpressions.RegexOptions.Compiled)
        End If

        Try
            objMatch = reMatchNum.Match(LineOfText)
            If Not objMatch Is Nothing AndAlso objMatch.Success Then
                If Integer.TryParse(objMatch.Groups(1).Value, intGroupdID) Then
                    ' Match Succcess; intGroupID now contains the Group ID value in LineOfText
                Else
                    strErrorMessage = "Unable to parse out the Group ID value from " & LineOfText
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, strErrorMessage)
                    intGroupdID = 0
                End If
            Else
                strErrorMessage = "Did not match 'group id=xx' in " & LineOfText
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, strErrorMessage)
                intGroupdID = 0
            End If

        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerXT.RetrieveGroupIdNumber, Error obtaining group id from *_xt.xml files, job " & m_JobNum & ex.Message)
            intGroupdID = 0
        End Try

        Return intGroupdID

    End Function

    ''' <summary>
    ''' This function extracts out the account name and remaining available balance by parsing a line of data from the GBalance output file
    ''' </summary>
    ''' <param name="LineOfText">Line to parse</param>
    ''' <param name="strAccountName">Account name (output)</param>
    ''' <returns>Remaining balance, in hours</returns>
    ''' <remarks></remarks>
    Protected Function RetrieveGBalanceData(ByVal LineOfText As String, ByRef strAccountName As String) As Single

        ' Example file contents:

        ' Id  Name                 Amount   Reserved Balance  CreditLimit Available
        ' --- -------------------- -------- -------- -------- ----------- ---------
        ' 648 emsl33210 on Chinook 62267456        0 62267456           0  62267456

        Const REGEX_GBALANCE As String = "^\s*\d+ ([^ ]+) .+(\d+)"

        Static reMatch As System.Text.RegularExpressions.Regex

        Dim objMatch As System.Text.RegularExpressions.Match

        Dim intBalanceSeconds As System.Int64
        Dim sngBalanceHours As Single

        Dim strErrorMessage As String

        sngBalanceHours = -1
        strAccountName = String.Empty

        If reMatch Is Nothing Then
            reMatch = New System.Text.RegularExpressions.Regex(REGEX_GBALANCE, Text.RegularExpressions.RegexOptions.IgnoreCase Or Text.RegularExpressions.RegexOptions.Compiled)
        End If

        Try
            ' Make sure the line starts with a number, then extract out the propsal name and the available hours
            objMatch = reMatch.Match(LineOfText)
            If Not objMatch Is Nothing AndAlso objMatch.Success Then
                strAccountName = objMatch.Groups(1).Value

                If System.Int64.TryParse(objMatch.Groups(2).Value, intBalanceSeconds) Then
                    ' Match Succcess; intBalanceSeconds now contains the balance, in seconds
                    ' Convert to hours
                    sngBalanceHours = CSng(intBalanceSeconds / 60.0 / 60.0)
                Else
                    strErrorMessage = "Could not extract out the proposal name and available hours from GBalance line: " & LineOfText
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, strErrorMessage)
                End If
            Else
                strErrorMessage = "GBalance line is not of the expected form: " & LineOfText
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, strErrorMessage)
            End If

        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerXT.RetrieveGBalanceData, Error parsing GBalance info, job " & m_JobNum & ex.Message)
        End Try

        Return sngBalanceHours

    End Function

    ''' <summary>
    ''' Parses a line from the showq output to determine the job number and the job state
    ''' </summary>
    ''' <param name="LineOfText">Line to parse</param>
    ''' <param name="intJobNumber">Job number (output)</param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Protected Function RetrieveJobQueueStatus(ByVal LineOfText As String, ByVal intJobNumber As Integer) As String

        ' Example file contents:

        ' 1329458             svc-dms    Running    32     3:57:59  Tue Mar 30 12:01:54
        ' 1329461             svc-dms    Running    32     3:58:36  Tue Mar 30 12:02:31


        Const REGEX_JOBQ As String = "^\s*(\d+)\s+[^ ]+\s+([^ ]+)"

        Static reMatch As System.Text.RegularExpressions.Regex

        Dim objMatch As System.Text.RegularExpressions.Match

        Dim strState As String

        Dim strErrorMessage As String

        strState = ""
        intJobNumber = 0

        If reMatch Is Nothing Then
            reMatch = New System.Text.RegularExpressions.Regex(REGEX_JOBQ, Text.RegularExpressions.RegexOptions.IgnoreCase Or Text.RegularExpressions.RegexOptions.Compiled)
        End If

        Try
            ' See if the line is of the expected form
            objMatch = reMatch.Match(LineOfText)
            If Not objMatch Is Nothing AndAlso objMatch.Success Then
                If Integer.TryParse(objMatch.Groups(1).Value, intJobNumber) Then
                    strState = objMatch.Groups(2).Value
                Else
                    strErrorMessage = "Could not extract out the job number and state from ShowQ line: " & LineOfText
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, strErrorMessage)
                End If
            Else
                strErrorMessage = "ShowQ line is not of the expected form: " & LineOfText
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, strErrorMessage)
            End If

        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerXT.RetrieveJobQueueStatus, Error parsing ShowQ info, job " & m_JobNum & ex.Message)
        End Try

        Return strState

    End Function

    ''' <summary>
    ''' 
    ''' </summary>
    ''' <param name="LineOfText"></param>
    ''' <param name="CurrentMaxNum"></param>
    ''' <param name="OffsetNum"></param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Protected Function ComputeNewMaxNumber(ByVal LineOfText As String, ByVal CurrentMaxNum As Integer, ByVal OffsetNum As Integer) As Integer
        Dim intGroupID As Integer

        Try
            intGroupID = RetrieveGroupIDNumber(LineOfText)
            Return ComputeNewMaxNumber(intGroupID, CurrentMaxNum, OffsetNum)
        Catch Err As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerXT.ComputeNewMaxNumber, Error obtaining max group id from *_xt.xml files, job " & m_JobNum & Err.Message)
            Return CurrentMaxNum
        End Try

    End Function

    ''' <summary>
    ''' 
    ''' </summary>
    ''' <param name="intGroupID"></param>
    ''' <param name="CurrentMaxNum"></param>
    ''' <param name="OffsetNum"></param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Protected Function ComputeNewMaxNumber(ByVal intGroupID As Integer, ByVal CurrentMaxNum As Integer, ByVal OffsetNum As Integer) As Integer

        Try
            If intGroupID + OffsetNum < CurrentMaxNum Then
                Return CurrentMaxNum
            Else
                Return intGroupID + OffsetNum
            End If
        Catch Err As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerXT.ComputeNewMaxNumber, Error obtaining max group id from *_xt.xml files, job " & m_JobNum & Err.Message)
            Return CurrentMaxNum
        End Try

    End Function

    ''' <summary>
    ''' 
    ''' </summary>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Protected Function MakeCheckJobCmdFiles() As IJobParams.CloseOutType

        Dim i As Integer

        Try
            For i = 1 To m_NumClonedSteps

                Dim InputFilename As String = System.IO.Path.Combine(m_WorkDir, "CreateCheckjob_Job" & m_JobNum & "_" & i.ToString)

                clsGlobal.m_FilesToDeleteExt.Add(System.IO.Path.GetFileName(InputFilename))

                ' Create an instance of StreamWriter to write to a file.
                Dim swOut As System.IO.StreamWriter = New System.IO.StreamWriter(InputFilename)

                WriteUnix(swOut, "cd " & clsAnalysisXTHPCGlobals.HPC_ROOT_DIRECTORY)

                WriteUnix(swOut, "/apps/moab/current/bin/checkjob " & m_HPCJobStatus(i, 0) & " > " & clsAnalysisXTHPCGlobals.HPC_ROOT_DIRECTORY & "Job" & m_JobNum & "_msub" & i & "/" & "CheckjobStatus_" & m_HPCJobStatus(i, 0))

                swOut.Close()

                clsGlobal.m_FilesToDeleteExt.Add("CheckjobStatus_" & m_HPCJobStatus(i, 0))

            Next

        Catch E As Exception
            ' Let the user know what went wrong.
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisResourcesXTHPC.BuildCheckJobCmdFile, The Check job file could not be written: " & E.Message)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    ''' <summary>
    ''' 
    ''' </summary>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Protected Function MakeGetCheckjobStatusFilesCmdFile() As IJobParams.CloseOutType
        Dim result As Boolean = True

        Dim InputFilename As String

        Dim i As Integer

        Try
            For i = 1 To m_NumClonedSteps

                InputFilename = System.IO.Path.Combine(m_WorkDir, "GetCheckjob_HPCJob" & m_HPCJobStatus(i, 0))

                clsGlobal.m_FilesToDeleteExt.Add(System.IO.Path.GetFileName(InputFilename))

                ' Create an instance of StreamWriter to write to a file.
                Dim swOut As System.IO.StreamWriter = New System.IO.StreamWriter(InputFilename)

                WriteUnix(swOut, "cd " & clsAnalysisXTHPCGlobals.HPC_ROOT_DIRECTORY & "Job" & m_JobNum & "_msub" & i & "/")

                WriteUnix(swOut, "get " & "CheckjobStatus_" & m_HPCJobStatus(i, 0))

                swOut.Close()
            Next

        Catch E As Exception
            ' Let the user know what went wrong.
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisResourcesXTHPC.MakeGetShowQResultFileCmdFile, The file could not be written: " & E.Message)
            result = False
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



    ' '' Result files should now be available.  A status could still indicate 'Complete' so we need to 
    ' '' check for errors. We'll also check to make sure the error file is empty
    ''HPC_Result = IJobParams.CloseOutType.CLOSEOUT_NO_OUT_FILES
    ''While HPC_Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS
    ''    System.Threading.Thread.Sleep(MonitorInterval)
    ''    HPC_Result = RetrieveJobResultFilesFromHPC(progSftpLoc)
    ''    If HPC_Result = IJobParams.CloseOutType.CLOSEOUT_FAILED Then
    ''        'If error is detected, the Cancel all jobs that are running.
    ''        CancelHPCRunningJobs(progLoc)
    ''        Return IJobParams.CloseOutType.CLOSEOUT_FAILED
    ''    End If
    ''End While


End Class
