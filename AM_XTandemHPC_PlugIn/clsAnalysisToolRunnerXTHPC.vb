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
    Protected Const HPC_NAME As String = " svc-dms cu0login1 -i C:\DMS_Programs\Chinook\SSH_Keys\Chinook.ppk " '"svc-dms chinook.emsl.pnl.gov -pw PASSWORD_HERE " 
    Protected WithEvents CmdRunner As clsRunDosProgram

    Private m_HPCJobNumber As String()
    Private m_NumClonedSteps As Integer = 1
    Private m_Dataset As String = ""

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
            CmdStr = "-l " & HPC_NAME & " -b " & "PutFastaJob" & m_JobNum
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

        ReDim m_HPCJobNumber(m_NumClonedSteps)
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

        ' Now we need to wait for the result files to appear.  We'll also check to make sure the error file is empty
        ' Increase MonitorInterval to 30 seconds
        MonitorInterval = 30000

        HPC_Result = IJobParams.CloseOutType.CLOSEOUT_NO_OUT_FILES
        While HPC_Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS
            System.Threading.Thread.Sleep(MonitorInterval)
            HPC_Result = RetrieveJobResultFilesFromHPC(progSftpLoc)
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

        If Not clsGlobal.RemoveNonResultFiles(m_WorkDir, m_DebugLevel) Then
            'TODO: Figure out what to do here
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
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

        Dim JobNum As String = m_jobParams.GetParam("Job")

        Try
            ' Create an instance of StreamWriter to write to a file.
            Dim swOut As System.IO.StreamWriter = New System.IO.StreamWriter(inputFilename)

            WriteUnix(swOut, "cd " & clsAnalysisXTHPCGlobals.HPC_ROOT_DIRECTORY & "/Job" & JobNum & "_msub" & File_Index & "/")

            WriteUnix(swOut, "get X-Tandem_Job" & JobNum & "_" & File_Index & ".output")

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

    ''' <summary>
    ''' 
    ''' </summary>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Protected Function BuildGetOutputFilesCmdFile(ByVal progSftpLoc As String) As IJobParams.CloseOutType
        Dim Get_CmdFile As String
        Dim i As Integer

        Dim HPC_JobNum As String

        Try
            For i = 1 To m_NumClonedSteps
                HPC_JobNum = GetHPCJobNumber(i.ToString)
                If Not String.IsNullOrEmpty(HPC_JobNum) AndAlso IsNumeric(HPC_JobNum) Then
                    Get_CmdFile = System.IO.Path.Combine(m_WorkDir, "GetResultFilesCmds_Job" & m_JobNum & "_" & i)
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
        Dim CommandfileName As String
        Dim JobOutputFilePath As String
        Dim ErrorResultFilePath As String
        Dim XTResultsFilePath As String

        Dim strErrorResult As String

        Try
            ' TODO: make sure job is complete before grabbing the _xt.xml files

            ' Step 1: Run an 'ls m_Dataset & "_*_xt.xml"' command using Putty and redirect to a text file
            ' Step 2: Append to this file the results of an 'ls m_JobNum & "_Part" & i & ".output." & m_HPCJobNumber(i)' command
            ' Step 3: grab the text file using GetResultFilesCmds_Job
            ' Step 4: Count the number of _xt.xml files in the file that have a modification date > 30 seconds aftr the last mod time of the .output. file
            ' Step 5: Once we have m_NumClonedSteps valid _xt.xml files, then grab them using PuttySFTP

            ' Use CheckJob to determine job state

            For i = 1 To m_NumClonedSteps


                CommandfileName = "GetResultFilesCmds_Job" & m_JobNum & "_" & i
                CmdStr = "-l " & HPC_NAME & " -b " & CommandfileName
                If Not CmdRunner.RunProgram(progSftpLoc, CmdStr, "PuttySFTP", True) Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error running Putty SFTP to get results files from super computer, job " & m_JobNum & ", Command: " & CmdStr)
                    Return IJobParams.CloseOutType.CLOSEOUT_FAILED
                End If

                JobOutputFilePath = System.IO.Path.Combine(m_WorkDir, m_JobNum & "_Part" & i & ".output." & m_HPCJobNumber(i))
                ErrorResultFilePath = System.IO.Path.Combine(m_WorkDir, m_JobNum & "_Part" & i & ".err." & m_HPCJobNumber(i))
                XTResultsFilePath = System.IO.Path.Combine(m_WorkDir, m_Dataset & "_" & i & "_xt.xml")

                If Not System.IO.File.Exists(JobOutputFilePath) Then
                    Return IJobParams.CloseOutType.CLOSEOUT_NO_OUT_FILES
                End If

                If Not System.IO.File.Exists(ErrorResultFilePath) Then
                    Return IJobParams.CloseOutType.CLOSEOUT_NO_OUT_FILES
                Else
                    Dim fiErrorLocal As New System.IO.FileInfo(ErrorResultFilePath)
                    If fiErrorLocal.Length > 0 Then
                        '***********Log error to log file here***********
                        strErrorResult = ReadEntireFile(ErrorResultFilePath)
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error file " & ErrorResultFilePath & " contains the following error: " & strErrorResult)
                        Return IJobParams.CloseOutType.CLOSEOUT_FAILED
                    End If
                End If

                ' If the LS command
                If Not System.IO.File.Exists(XTResultsFilePath) Then
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

    Protected Sub DeleteDTATextFilesInTransferFolder()

        Dim strTransferFolderPath As String
        Dim strDtaFileName As String

        Dim i As Integer
        Dim ioFileInfo As System.IO.FileInfo

        Try
            strTransferFolderPath = System.IO.Path.Combine(m_jobParams.GetParam("transferFolderPath"), m_Dataset)

            ' Now that we have successfully concatenated things, delete each of the input files
            For i = 1 To m_NumClonedSteps

                strDtaFileName = m_Dataset & "_" & i.ToString & "_dta.txt"
                System.IO.Path.Combine(strTransferFolderPath, strDtaFileName)

                ioFileInfo = New System.IO.FileInfo(strDtaFileName)
                If ioFileInfo.Exists Then
                    ioFileInfo.Delete()
                End If

            Next i

        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "clsAnalysisResourcesXTHPC.DeleteDTATextFilesInTransferFolder, Error deleting file: " & ex.Message)
        End Try

    End Sub

    Protected Function BuildCancelJobFilesCmdFile() As IJobParams.CloseOutType

        Dim InputFilename As String = System.IO.Path.Combine(m_WorkDir, "Cancel_Job" & m_JobNum)

        Dim i As Integer

        Try
            clsGlobal.m_FilesToDeleteExt.Add(System.IO.Path.GetFileName(InputFilename))


            ' Create an instance of StreamWriter to write to a file.
            Dim swOut As System.IO.StreamWriter = New System.IO.StreamWriter(InputFilename)

            WriteUnix(swOut, "cd " & clsAnalysisXTHPCGlobals.HPC_ROOT_DIRECTORY)

            For i = 1 To m_NumClonedSteps

                WriteUnix(swOut, "canceljob " & m_HPCJobNumber(i))

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

            swOut.Close()

        Catch E As Exception
            ' Let the user know what went wrong.
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisResourcesXTHPC.MakeGetOutputFilesCmdFile, The file could not be written: " & E.Message)
            result = False
            Return result
        End Try

        Return result
    End Function

    Protected Sub WriteUnix(ByRef swOut As System.IO.StreamWriter)
        WriteUnix(swOut, String.Empty)
    End Sub

    Protected Sub WriteUnix(ByRef swOut As System.IO.StreamWriter, ByVal inputString As String)

        swOut.Write(inputString & ControlChars.Lf)

    End Sub

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
        Dim i As Integer
        Dim j As Integer

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
                            End If

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

                            If Not StopWriting Then
                                swConcatenatedResultFile.WriteLine(lineText)
                            End If
                        End If
                        intLinesProcessed += 1
                    Else
                        lineText = srCurrentResultFile.ReadLine
                        If lineText.Contains("<group label=""input parameters""") Then
                            StopWriting = True
                        End If

                        If m_NumClonedSteps > 1 Then
                            If lineText.Contains("<group id=""") Then
                                CurrentMaxNum = ComputeNewMaxNumber(lineText, CurrentMaxNum, 0)
                            End If
                        End If

                        If Not StopWriting Then
                            ' Append this line to the output file
                            swConcatenatedResultFile.WriteLine(lineText)
                        Else
                            ' Cache this line in EndOfFile
                            EndOfFile.Add(lineText)
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

    Protected Sub FindAndReplace(ByRef lineText As String, ByRef strOldValue As String, ByRef strNewValue As String)
        Dim intMatchIndex As Integer

        intMatchIndex = lineText.IndexOf(strOldValue)

        If intMatchIndex > 0 Then
            lineText = lineText.Substring(0, intMatchIndex) + strNewValue + lineText.Substring(intMatchIndex + strOldValue.Length)
        ElseIf intMatchIndex = 0 Then
            lineText = strNewValue + lineText.Substring(intMatchIndex + strOldValue.Length)
        End If
    End Sub

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

        Catch Err As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerXT.RetrieveGroupIdNumber, Error obtaining group id from *_xt.xml files, job " & m_JobNum & Err.Message)
            intGroupdID = 0
        End Try

        Return intGroupdID

    End Function

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
