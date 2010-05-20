'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2006, Battelle Memorial Institute
' Created 06/07/2006
'
' Last modified 09/19/2008
' Last modified 06/15/2009 JDS - Added logging using log4net
'*********************************************************************************************************

Imports System.IO
Imports PRISM.Files
Imports PRISM.Files.clsFileTools
Imports System.Text.RegularExpressions
Imports AnalysisManagerBase
Imports AnalysisManagerBase.clsGlobal
'Imports AnalysisManagerMSMSBase

Public Class clsAnalysisToolRunnerSeqBase
	Inherits clsAnalysisToolRunnerBase

	'*********************************************************************************************************
	'Base class for Sequest analysis
	'*********************************************************************************************************

#Region "Methods"
	''' <summary>
	''' Constructor
	''' </summary>
	''' <remarks>Presently not used</remarks>
	Public Sub New()

	End Sub

	''' <summary>
	''' Initializes class
	''' </summary>
	''' <param name="mgrParams">Object containing manager parameters</param>
	''' <param name="jobParams">Object containing job parameters</param>
    ''' <param name="StatusTools">Object for updating status file as job progresses</param>
	''' <remarks></remarks>
    Public Overrides Sub Setup(ByVal mgrParams As IMgrParams, ByVal jobParams As IJobParams, _
      ByVal StatusTools As IStatusFile)

        MyBase.Setup(mgrParams, jobParams, StatusTools)

        If m_DebugLevel > 3 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerSeqBase.Setup()")
        End If
    End Sub

	''' <summary>
	''' Runs the analysis tool
	''' </summary>
	''' <returns>IJobParams.CloseOutType value indicating success or failure</returns>
	''' <remarks></remarks>
	Public Overrides Function RunTool() As AnalysisManagerBase.IJobParams.CloseOutType

		Dim Result As IJobParams.CloseOutType

		'Do the base class stuff
		If Not MyBase.RunTool = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then Return IJobParams.CloseOutType.CLOSEOUT_FAILED

        ' Make sure at least one .DTA file exists
        If Not ValidateDTAFiles Then
            Return IJobParams.CloseOutType.CLOSEOUT_NO_DTA_FILES
        End If

        ' Count the number of .Dta files and cache in m_DtaCount
        CalculateNewStatus(True)

        ' Check whether or not we are resuming a job that stopped prematurely
        ' Look for a folder in the FailedResultsFolder that is named the same as the ResultsFolder for this job
        ' If that folder contains a file named "Resume.txt", then copy any .Out files from that folder to the work directory, then delete the corresponding .Dta files
        CheckForExistingOutFiles()

        'Run Sequest
        m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.RUNNING_TOOL, m_progress, m_DtaCount, "", "", "", False)

        'Make the .out files
        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Making OUT files, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step"))
        Try
            Result = MakeOUTFiles()
            If Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                Return Result
            End If
        Catch Err As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerSeqBase.RunTool(), Exception making OUT files, " & _
             Err.Message & "; " & clsGlobal.GetExceptionStackTrace(Err))
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        'Stop the job timer
        m_StopTime = Now

        'Add the current job data to the summary file
        If Not UpdateSummaryFile() Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN, "Error creating summary file, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step"))
        End If

        'Make sure objects are released
        System.Threading.Thread.Sleep(2000)        '2 second delay
        GC.Collect()
        GC.WaitForPendingFinalizers()

        ' Parse the Sequest .Log file to make sure the expected number of nodes was used in the analysis
        Dim strSequestLogFilePath As String
        Dim blnSuccess As Boolean

        strSequestLogFilePath = System.IO.Path.Combine(m_WorkDir, "sequest.log")
        blnSuccess = ValidateSequestNodeCount(strSequestLogFilePath)

        Result = MakeResultsFolder()
        If Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            'TODO: What do we do here?
            Return Result
        End If

        Result = MoveResultFiles()
        If Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            'TODO: What do we do here?
            ' Note that MoveResultFiles should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
            Return Result
        End If

        Result = CopyResultsFolderToServer()
        If Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            'TODO: What do we do here?
            ' Note that CopyResultsFolderToServer should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
            Return Result
        End If

        If Not clsGlobal.RemoveNonResultFiles(m_mgrParams.GetParam("workdir"), m_DebugLevel) Then
            m_message = AppendToComment(m_message, "Error deleting non-result files")
            'TODO: Figure out what to do here
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    ''' <summary>
    ''' Calculates status information for progress file
    ''' </summary>
    ''' <remarks>
    ''' Calculation in this class is based on Sequest processing. For other processing types,
    '''	override this function in derived class
    '''</remarks>
    Protected Overridable Sub CalculateNewStatus()
        CalculateNewStatus(True)
    End Sub

    ''' <summary>
    ''' Calculates status information for progress file
    ''' </summary>
    ''' <param name="blnUpdateDTACount">Set to True to update m_DtaCount</param>
    ''' <remarks>
    ''' Calculation in this class is based on Sequest processing. For other processing types,
    '''	override this function in derived class
    '''</remarks>
    Protected Overridable Sub CalculateNewStatus(ByVal blnUpdateDTACount As Boolean)

        Dim FileArray() As String
        Dim OutFileCount As Integer

        m_WorkDir = CheckTerminator(m_WorkDir)

        If blnUpdateDTACount Then
            'Get DTA count
            FileArray = Directory.GetFiles(m_WorkDir, "*.dta")
            m_DtaCount = FileArray.GetLength(0)
        End If

        'Get OUT file count
        FileArray = Directory.GetFiles(m_WorkDir, "*.out")
        OutFileCount = FileArray.GetLength(0)

        'Calculate % complete
        If m_DtaCount > 0 Then
            m_progress = 100.0! * CSng(OutFileCount / m_DtaCount)
        Else
            m_progress = 0
        End If

    End Sub

    Protected Sub CheckForExistingOutFiles()

        Const RESUME_FILE_NAME As String = "Resume.txt"

        Dim strFailedResultsFolderPath As String
        Dim ioSourceFolder As System.IO.DirectoryInfo
        Dim ioFileList() As System.IO.FileInfo
        Dim ioDtaFile As System.IO.FileInfo

        Dim strDTAFilePath As String
        Dim strExistingSeqLogFileRenamed As String

        Dim intIndex As Integer
        Dim intOutFilesCopied As Integer
        Dim intDtaFilesDeleted As Integer

        Try

            strFailedResultsFolderPath = m_mgrParams.GetParam("FailedResultsFolderPath")

            If strFailedResultsFolderPath Is Nothing OrElse strFailedResultsFolderPath.Length = 0 Then
                ' Failed results folder path is not defined; cannot continue
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "FailedResultsFolderPath is not defined for this manager; cannot look for existing .Out files for resuming Sequest")
                Exit Try
            End If

            If m_ResFolderName Is Nothing OrElse m_ResFolderName.Length = 0 Then
                ' Results folder name is not defined
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "m_ResFolderName is empty; this is unexpected")
                Exit Try
            End If

            strFailedResultsFolderPath = System.IO.Path.Combine(strFailedResultsFolderPath, m_ResFolderName)

            If m_DebugLevel >= 3 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Looking for existing .Out files at " & strFailedResultsFolderPath)
            End If

            ioSourceFolder = New System.IO.DirectoryInfo(strFailedResultsFolderPath)
            If ioSourceFolder.Exists Then
                If m_DebugLevel >= 1 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Archived results folder found for job " & m_JobNum & "; checking for a file named " & RESUME_FILE_NAME)
                End If

                If ioSourceFolder.GetFiles(RESUME_FILE_NAME).Length > 0 Then
                    ' Yes, folder contains a Resume file
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, RESUME_FILE_NAME & " file found in archived results folder (" & ioSourceFolder.FullName & "); will copy any .Out files to " & m_WorkDir)

                    ' Look for all of the existing .Out files
                    ' Copy all non-empty .Out files to the Work Directory

                    ioFileList = ioSourceFolder.GetFiles("*.out")
                    intOutFilesCopied = 0
                    intDtaFilesDeleted = 0

                    For intIndex = 0 To ioFileList.Length - 1
                        With ioFileList(intIndex)
                            If .Length > 0 Then
                                .CopyTo(System.IO.Path.Combine(m_WorkDir, .Name), True)
                                intOutFilesCopied += 1

                                ' Delete the corresponding .Dta file in m_Workdir
                                strDTAFilePath = System.IO.Path.Combine(m_WorkDir, System.IO.Path.GetFileNameWithoutExtension(.Name) & ".dta")
                                ioDtaFile = New System.IO.FileInfo(strDTAFilePath)
                                If ioDtaFile.Exists Then
                                    ioDtaFile.Delete()
                                    intDtaFilesDeleted += 1
                                End If
                            End If
                        End With
                    Next intIndex

                    If intOutFilesCopied = 0 Then
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Warning: did not find any existing .Out files in the archived results folder")
                    Else
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Copied " & intOutFilesCopied & " .Out files to the Work Directory; deleted " & intDtaFilesDeleted & " corresponding .Dta files in the Work Directory")

                        ioFileList = ioSourceFolder.GetFiles("sequest.log")
                        If ioFileList.Length > 0 Then
                            With ioFileList(0)
                                ' Copy the sequest.log file to the work directory, but rename it to include a time stamp
                                strExistingSeqLogFileRenamed = System.IO.Path.GetFileNameWithoutExtension(.Name) & "_" & _
                                                               .LastWriteTime.ToString("yyyyMMdd_HHmm") & _
                                                               System.IO.Path.GetExtension(.Name)

                                .CopyTo(System.IO.Path.Combine(m_WorkDir, strExistingSeqLogFileRenamed), True)
                            End With

                        End If
                    End If

                End If

            End If

        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error in CheckForExistingOutFiles: " & ex.Message)
        End Try

    End Sub

	''' <summary>
	''' Runs Sequest to make .out files
	''' </summary>
	''' <returns>CloseOutType enum indicating success or failure</returns>
	''' <remarks></remarks>
	Protected Overridable Function MakeOUTFiles() As IJobParams.CloseOutType

		'Creates Sequest .out files from DTA files
		Dim CmdStr As String
		Dim DumStr As String
		Dim DtaFiles() As String
		Dim RunProgs() As PRISM.Processes.clsProgRunner
		Dim Textfiles() As StreamWriter
		Dim NumFiles As Integer
		Dim ProcIndx As Integer
		Dim StillRunning As Boolean
        '12/19/2008 - The number of processors used to be configurable but now this is done with clustering.
        'This code is left here so we can still debug to make sure everything still works
        '		Dim NumProcessors As Integer = CInt(m_mgrParams.GetParam("numberofprocessors"))
        Dim NumProcessors As Integer = 1

		'Ensure output path doesn't have backslash
		m_WorkDir = CheckTerminator(m_WorkDir, False)

		'Get a list of .dta file names
		DtaFiles = Directory.GetFiles(m_WorkDir, "*.dta")
		NumFiles = DtaFiles.GetLength(0)
		If NumFiles = 0 Then
			m_message = AppendToComment(m_message, "No dta files found for Sequest processing")
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		'Set up a program runner and text file for each processor
		ReDim RunProgs(NumProcessors - 1)
		ReDim Textfiles(NumProcessors - 1)
		CmdStr = "-D" & Path.Combine(m_mgrParams.GetParam("orgdbdir"), m_jobParams.GetParam("generatedFastaName")) _
		  & " -P" & m_jobParams.GetParam("parmFileName") & " -R"
		For ProcIndx = 0 To NumProcessors - 1
			DumStr = Path.Combine(m_WorkDir, "FileList" & ProcIndx.ToString & ".txt")
            clsGlobal.FilesToDelete.Add(DumStr)

			RunProgs(ProcIndx) = New PRISM.Processes.clsProgRunner
			RunProgs(ProcIndx).Name = "Seq" & ProcIndx.ToString
			RunProgs(ProcIndx).CreateNoWindow = CBool(m_mgrParams.GetParam("createnowindow"))
			RunProgs(ProcIndx).Program = m_mgrParams.GetParam("seqprogloc")
			RunProgs(ProcIndx).Arguments = CmdStr & DumStr
			RunProgs(ProcIndx).WorkDir = m_WorkDir
			Textfiles(ProcIndx) = New StreamWriter(DumStr, False)
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, m_mgrParams.GetParam("seqprogloc") & CmdStr & DumStr)
        Next

		'Break up file list into lists for each processor
		ProcIndx = 0
		For Each DumStr In DtaFiles
			Textfiles(ProcIndx).WriteLine(DumStr)
			ProcIndx += 1
			If ProcIndx > (NumProcessors - 1) Then ProcIndx = 0
		Next

		'Close all the file lists
		For ProcIndx = 0 To Textfiles.GetUpperBound(0)
			If m_DebugLevel > 0 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerSeqBase.MakeOutFiles: Closing FileList" & ProcIndx)
            End If
			Try
				Textfiles(ProcIndx).Close()
				Textfiles(ProcIndx) = Nothing
			Catch Err As Exception
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerSeqBase.MakeOutFiles: " & Err.Message & "; " & _
                                   clsGlobal.GetExceptionStackTrace(Err))
            End Try
		Next

		'Run all the programs
		For ProcIndx = 0 To RunProgs.GetUpperBound(0)
			RunProgs(ProcIndx).StartAndMonitorProgram()
			System.Threading.Thread.Sleep(1000)
		Next

		'Wait for completion
		StillRunning = True
		While StillRunning
			StillRunning = False
			System.Threading.Thread.Sleep(5000)

            ' Synchronize the stored Debug level with the value stored in the database
            Const MGR_SETTINGS_UPDATE_INTERVAL_SECONDS As Integer = 300
            MyBase.GetCurrentMgrSettingsFromDB(MGR_SETTINGS_UPDATE_INTERVAL_SECONDS)

            CalculateNewStatus(False)
            m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.RUNNING_TOOL, m_progress, m_DtaCount, "", "", "", False)

			For ProcIndx = 0 To RunProgs.GetUpperBound(0)
				If m_DebugLevel > 4 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerSeqBase.MakeOutFiles(): RunProgs(" & ProcIndx.ToString & ").State = " & _
                     RunProgs(ProcIndx).State.ToString)
                End If
				If (RunProgs(ProcIndx).State <> 0) Then
					If m_DebugLevel > 4 Then
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerSeqBase.MakeOutFiles()_2: RunProgs(" & ProcIndx.ToString & ").State = " & _
                         RunProgs(ProcIndx).State.ToString)
                    End If
					If (RunProgs(ProcIndx).State <> 10) Then
						If m_DebugLevel > 4 Then
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerSeqBase.MakeOutFiles()_3: RunProgs(" & ProcIndx.ToString & ").State = " & _
                             RunProgs(ProcIndx).State.ToString)
                        End If
						StillRunning = True
						Exit For
					Else
						If m_DebugLevel > 0 Then
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerSeqBase.MakeOutFiles()_4: RunProgs(" & ProcIndx.ToString & ").State = " & _
                             RunProgs(ProcIndx).State.ToString)
                        End If
					End If
				End If
            Next
        End While

		'Clean up our object references
		If m_DebugLevel > 0 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerSeqBase.MakeOutFiles(), cleaning up runprog object references")
        End If
		For ProcIndx = 0 To RunProgs.GetUpperBound(0)
			RunProgs(ProcIndx) = Nothing
			If m_DebugLevel > 0 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Set RunProgs(" & ProcIndx.ToString & ") to Nothing")
            End If
		Next

		'Make sure objects are released
		System.Threading.Thread.Sleep(20000)		'20 second delay
		GC.Collect()
		GC.WaitForPendingFinalizers()

		'Verify out file creation
		If m_DebugLevel > 0 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerSeqBase.MakeOutFiles(), verifying out file creation")
        End If
		DtaFiles = Directory.GetFiles(m_WorkDir, "*.out")
		If DtaFiles.GetLength(0) < 1 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "No OUT files created, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step"))
            m_message = AppendToComment(m_message, "No OUT files created")
			Return IJobParams.CloseOutType.CLOSEOUT_NO_OUT_FILES
		Else
			'Add .out files to list of files for deletion
			For Each OutFile As String In DtaFiles
				clsGlobal.FilesToDelete.Add(OutFile)
            Next
            'Add .out extension to list of file extensions to delete
            clsGlobal.m_FilesToDeleteExt.Add(".out")
		End If

		'Package out files into concatenated text files 
		If Not ConcatOutFiles(m_WorkDir, m_jobParams.GetParam("datasetNum"), m_JobNum) Then
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

        'Try to ensure there are no open objects with file handles
        System.Threading.Thread.Sleep(10000)        'Move this to before GC after troubleshooting complete
        GC.Collect()
        GC.WaitForPendingFinalizers()


		'Zip concatenated .out files
		If Not ZipConcatOutFile(m_WorkDir, m_mgrParams.GetParam("zipprogram"), m_JobNum) Then
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		'If we got here, everything worked
		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

	''' <summary>
	''' Concatenates the .out files in the working directory to a single _out.txt file
	''' </summary>
	''' <param name="WorkDir">Working directory</param>
	''' <param name="DSName">Dataset name</param>
	''' <param name="JobNum">Job number</param>
	''' <returns>TRUE for success; FALSE for failure</returns>
	''' <remarks></remarks>
	Protected Overridable Function ConcatOutFiles(ByVal WorkDir As String, ByVal DSName As String, ByVal JobNum As String) As Boolean

		Dim ConcatTools As New clsConcatToolWrapper(WorkDir)

		If m_DebugLevel > 0 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerSeqBase.ConcatOutFiles(), concatenating .out files")
        End If

        'Make sure objects are released
        System.Threading.Thread.Sleep(5000)        '5 second delay
        GC.Collect()
        GC.WaitForPendingFinalizers()

		If ConcatTools.ConcatenateFiles(clsConcatToolWrapper.ConcatFileTypes.CONCAT_OUT, DSName) Then
			If m_DebugLevel > 0 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerSeqBase.ConcatOutFiles(), out file concatenation succeeded")
            End If
			Return True
		Else
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, ConcatTools.ErrMsg & ", job " & JobNum)
            m_message = AppendToComment(m_message, "Error concatenating out files")
			Return False
		End If

	End Function

    ''' <summary>
    ''' Make sure at least one .DTA file exists
    ''' Also makes sure at least one of the .DTA files has data
    ''' </summary>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Protected Function ValidateDTAFiles() As Boolean
        Dim ioWorkFolder As System.IO.DirectoryInfo
        Dim ioFiles() As System.IO.FileInfo
        Dim ioFile As System.IO.FileInfo

        Dim srReader As System.IO.StreamReader

        Dim blnDataFound As Boolean = False
        Dim intFilesChecked As Integer = 0

        Try
            ioWorkFolder = New System.IO.DirectoryInfo(m_WorkDir)

            ioFiles = ioWorkFolder.GetFiles("*.dta", SearchOption.TopDirectoryOnly)

            If ioFiles.Length = 0 Then
                m_message = "No .DTA files are present"
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
                Return False
            Else
                For Each ioFile In ioFiles
                    srReader = ioFile.OpenText

                    Do While srReader.Peek >= 0
                        If srReader.ReadLine.Trim.Length > 0 Then
                            blnDataFound = True
                            Exit Do
                        End If
                    Loop

                    srReader.Close()
                    intFilesChecked += 1

                    If blnDataFound Then Exit For
                Next

                If Not blnDataFound Then
                    If intFilesChecked = 1 Then
                        m_message = "One .DTA file is present, but it is empty"
                    Else
                        m_message = ioFiles.Length.ToString() & " .DTA files are present, but each is empty"
                    End If

                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
                    Return False
                End If

            End If

        Catch ex As Exception
            m_message = "Exception in ValidateDTAFiles"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message)
            Return False
        End Try

        Return blnDataFound

    End Function

    ''' <summary>
    ''' Opens the sequest.log file in the working directory
    ''' Parses out the number of nodes used and the number of slave processes spawned
    ''' Counts the number of DTA files analyzed by each process
    ''' </summary>
    ''' <remarks></remarks>
    ''' <returns>True if file found and information successfully parsed from it (regardless of the validity of the information); False if file not found or error parsing information</returns>
    Protected Function ValidateSequestNodeCount(ByVal strLogFilePath As String) As Boolean
        Return ValidateSequestNodeCount(strLogFilePath, False)
    End Function

    ''' <summary>
    ''' Opens the sequest.log file in the working directory
    ''' Parses out the number of nodes used and the number of slave processes spawned
    ''' Counts the number of DTA files analyzed by each process
    ''' </summary>
    ''' <param name="strLogFilePath">Path to the sequest.log file to parse</param>
    ''' <param name="blnLogToConsole">If true, then displays the various status messages at the console</param>
    ''' <remarks></remarks>
    ''' <returns>True if file found and information successfully parsed from it (regardless of the validity of the information); False if file not found or error parsing information</returns>
    Protected Function ValidateSequestNodeCount(ByVal strLogFilePath As String, ByVal blnLogToConsole As Boolean) As Boolean
        Const ERROR_CODE_A As Integer = 2
        Const ERROR_CODE_B As Integer = 4
        Const ERROR_CODE_C As Integer = 8
        Const ERROR_CODE_D As Integer = 16
        Const ERROR_CODE_E As Integer = 32

        Dim reStartingTask As System.Text.RegularExpressions.Regex
        Dim reWaitingForReadyMsg As System.Text.RegularExpressions.Regex
        Dim reReceivedReadyMsg As System.Text.RegularExpressions.Regex
        Dim reSpawnedSlaveProcesses As System.Text.RegularExpressions.Regex
        Dim reSearchedDTAFile As System.Text.RegularExpressions.Regex
        Dim objMatch As System.Text.RegularExpressions.Match

        Dim srLogFile As System.IO.StreamReader

        Dim strParam As String
        Dim strLineIn As String
        Dim strHostName As String

        ' This hash table is a map from host name to an entry in intDTACounts()
        Dim htHosts As System.Collections.Hashtable
        Dim htHostNodeCount As System.Collections.Hashtable

        Dim objHostIndex As Object
        Dim objEnum As System.Collections.IDictionaryEnumerator

        Dim intIndex As Integer
        Dim intHostIndex As Integer
        Dim intNodeCountThisHost As Integer

        ' The following array tracks the number of DTAs processed by each host (sum of stats for all nodes on that host)
        Dim intDTAProcessingStats() As Integer
        Dim intDTAProcessingStatCount As Integer

        ' This array tracks the number of DTAs processed per node on each host
        Dim sngHostProcessingRate() As Single
        Dim sngHostProcessingRateSorted() As Single

        Dim blnShowDetailedRates As Boolean

        Dim intHostCount As Integer
        Dim intNodeCountStarted As Integer
        Dim intNodeCountActive As Integer
        Dim intDTACount As Integer

        Dim intNodeCountExpected As Integer

        Dim strProcessingMsg As String

        Try

            m_EvalMessage = String.Empty
            m_EvalCode = 0
            blnShowDetailedRates = False

            If Not System.IO.File.Exists(strLogFilePath) Then
                strProcessingMsg = "Sequest.log file not found; cannot verify the sequest node count"
                If blnLogToConsole Then Console.WriteLine(strProcessingMsg & ": " & strLogFilePath)
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, strProcessingMsg)
                Return False
            End If

            ' Initialize the RegEx objects
            reStartingTask = New System.Text.RegularExpressions.Regex("Starting the SEQUEST task on (\d+) node", RegexOptions.Compiled Or RegexOptions.IgnoreCase Or RegexOptions.Singleline)
            reWaitingForReadyMsg = New System.Text.RegularExpressions.Regex("Waiting for ready messages from (\d+) node", RegexOptions.Compiled Or RegexOptions.IgnoreCase Or RegexOptions.Singleline)
            reReceivedReadyMsg = New System.Text.RegularExpressions.Regex("received ready messsage from (.+)\(", RegexOptions.Compiled Or RegexOptions.IgnoreCase Or RegexOptions.Singleline)
            reSpawnedSlaveProcesses = New System.Text.RegularExpressions.Regex("Spawned (\d+) slave processes", RegexOptions.Compiled Or RegexOptions.IgnoreCase Or RegexOptions.Singleline)
            reSearchedDTAFile = New System.Text.RegularExpressions.Regex("Searched dta file .+ on (.+)", RegexOptions.Compiled Or RegexOptions.IgnoreCase Or RegexOptions.Singleline)

            intHostCount = 0            ' Value for reStartingTask
            intNodeCountStarted = 0     ' Value for reWaitingForReadyMsg
            intNodeCountActive = 0      ' Value for reSpawnedSlaveProcesses
            intDTACount = 0

            ' Note: This value is obtained when the manager params are grabbed from the Manager Control DB
            ' Use this query to view/update expected node counts'
            '  SELECT M.M_Name, PV.MgrID, PV.Value
            '  FROM T_ParamValue AS PV INNER JOIN T_Mgrs AS M ON PV.MgrID = M.M_ID
            '  WHERE (PV.TypeID = 122)

            strParam = m_mgrParams.GetParam("SequestNodeCountExpected")
            If Integer.TryParse(strParam, intNodeCountExpected) Then
            Else
                intNodeCountExpected = 0
            End If

            ' Initialize the hash table that will track the number of spectra processed by each host
            htHosts = New System.Collections.Hashtable

            ' Initialze the hash table that will track the number of distinct nodes on each host
            htHostNodeCount = New System.Collections.Hashtable

            ' Initially reserve space for 50 hosts
            intDTAProcessingStatCount = 0
            ReDim intDTAProcessingStats(49)

            srLogFile = New System.IO.StreamReader(New System.IO.FileStream(strLogFilePath, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.Read))

            ' Read each line from the input file
            Do While srLogFile.Peek >= 0
                strLineIn = srLogFile.ReadLine

                If Not strLineIn Is Nothing AndAlso strLineIn.Length > 0 Then

                    ' See if the line matches one of the expected RegEx values
                    objMatch = reStartingTask.Match(strLineIn)
                    If Not objMatch Is Nothing AndAlso objMatch.Success Then
                        If Not Integer.TryParse(objMatch.Groups(1).Value, intHostCount) Then
                            strProcessingMsg = "Unable to parse out the Host Count from the 'Starting the SEQUEST task ...' entry in the Sequest.log file"
                            If blnLogToConsole Then Console.WriteLine(strProcessingMsg)
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, strProcessingMsg)
                        End If

                    Else
                        objMatch = reWaitingForReadyMsg.Match(strLineIn)
                        If Not objMatch Is Nothing AndAlso objMatch.Success Then
                            If Not Integer.TryParse(objMatch.Groups(1).Value, intNodeCountStarted) Then
                                strProcessingMsg = "Unable to parse out the Node Count from the 'Waiting for ready messages ...' entry in the Sequest.log file"
                                If blnLogToConsole Then Console.WriteLine(strProcessingMsg)
                                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, strProcessingMsg)
                            End If

                        Else
                            objMatch = reReceivedReadyMsg.Match(strLineIn)
                            If Not objMatch Is Nothing AndAlso objMatch.Success Then
                                strHostName = objMatch.Groups(1).Value

                                If htHostNodeCount.ContainsKey(strHostName) Then
                                    htHostNodeCount(strHostName) = CInt(htHostNodeCount(strHostName)) + 1
                                Else
                                    htHostNodeCount.Add(strHostName, 1)
                                End If

                            Else
                                objMatch = reSpawnedSlaveProcesses.Match(strLineIn)
                                If Not objMatch Is Nothing AndAlso objMatch.Success Then
                                    If Not Integer.TryParse(objMatch.Groups(1).Value, intNodeCountActive) Then
                                        strProcessingMsg = "Unable to parse out the Active Node Count from the 'Spawned xx slave processes ...' entry in the Sequest.log file"
                                        If blnLogToConsole Then Console.WriteLine(strProcessingMsg)
                                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, strProcessingMsg)
                                    End If

                                Else
                                    objMatch = reSearchedDTAFile.Match(strLineIn)
                                    If Not objMatch Is Nothing AndAlso objMatch.Success Then
                                        strHostName = objMatch.Groups(1).Value

                                        If Not strHostName Is Nothing Then
                                            objHostIndex = htHosts(strHostName)

                                            If objHostIndex Is Nothing Then
                                                ' Host not present in htHosts; add it
                                                intHostIndex = intDTAProcessingStatCount
                                                htHosts.Add(strHostName, intHostIndex)

                                                If intDTAProcessingStatCount >= intDTAProcessingStats.Length Then
                                                    ' Reserve more space
                                                    ReDim Preserve intDTAProcessingStats(intDTAProcessingStats.Length * 2 - 1)
                                                End If

                                                intDTAProcessingStats(intHostIndex) = 0

                                                ' Increment the track of the number of entries in intDTAProcessingStats
                                                intDTAProcessingStatCount += 1

                                            Else
                                                intHostIndex = CInt(objHostIndex)
                                            End If

                                            intDTAProcessingStats(intHostIndex) += 1

                                            intDTACount += 1
                                        End If
                                    Else
                                        ' Ignore this line
                                    End If
                                End If
                            End If
                        End If
                    End If

                End If
            Loop

            srLogFile.Close()

            Try
                ' Validate the stats

                strProcessingMsg = "HostCount=" & intHostCount & "; NodeCountActive=" & intNodeCountActive
                If m_DebugLevel >= 1 Then
                    If blnLogToConsole Then Console.WriteLine(strProcessingMsg)
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, strProcessingMsg)
                End If
                m_EvalMessage = String.Copy(strProcessingMsg)

                If intNodeCountActive < intNodeCountExpected OrElse intNodeCountExpected = 0 Then
                    strProcessingMsg = "Error: NodeCountActive less than expected value (" & intNodeCountExpected & ")"
                    If blnLogToConsole Then Console.WriteLine(strProcessingMsg)
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, strProcessingMsg)

                    ' Update the evaluation message and evaluation code
                    ' These will be used by sub CloseTask in clsAnalysisJob
                    '
                    ' An evaluation code with bit ERROR_CODE_A set will result in DMS_Pipeline DB views
                    '  V_Job_Steps_Stale_and_Failed and V_Sequest_Cluster_Warnings showing this message:
                    '  "SEQUEST node count is less than the expected value"

                    m_EvalMessage &= "; " & strProcessingMsg
                    m_EvalCode = m_EvalCode Or ERROR_CODE_A
                Else
                    If intNodeCountStarted <> intNodeCountActive Then
                        strProcessingMsg = "Warning: NodeCountStarted (" & intNodeCountStarted & ") <> NodeCountActive"
                        If blnLogToConsole Then Console.WriteLine(strProcessingMsg)
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, strProcessingMsg)
                        m_EvalMessage &= "; " & strProcessingMsg
                        m_EvalCode = m_EvalCode Or ERROR_CODE_B

                        ' Update the evaluation message and evaluation code
                        ' These will be used by sub CloseTask in clsAnalysisJob
                        ' An evaluation code with bit ERROR_CODE_A set will result in view V_Sequest_Cluster_Warnings in the DMS_Pipeline DB showing this message:
                        '  "SEQUEST node count is less than the expected value"

                    End If
                End If

                If intDTAProcessingStatCount < intHostCount Then
                    ' Only record an error here if the number of DTAs processed was at least 2x the number of nodes
                    If intDTACount >= 2 * intNodeCountActive Then
                        strProcessingMsg = "Error: only " & intDTAProcessingStatCount & " host" & CheckForPlurality(intDTAProcessingStatCount) & " processed DTAs"
                        If blnLogToConsole Then Console.WriteLine(strProcessingMsg)
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, strProcessingMsg)
                        m_EvalMessage &= "; " & strProcessingMsg
                        m_EvalCode = m_EvalCode Or ERROR_CODE_C
                    End If
                End If

                ' See if any of the hosts processed far fewer or far more spectra than the other hosts
                ' When comparing hosts, we need to scale by the number of active nodes on each host
                ' We'll populate intHostProcessingRate() with the number of DTAs processed per node on each host

                Const LOW_THRESHOLD_MULTIPLIER As Single = 0.33
                Const HIGH_THRESHOLD_MULTIPLIER As Single = 2

                Dim sngProcessingRateMedian As Single
                Dim intMidpoint As Integer
                Dim sngThresholdRate As Single
                Dim intWarningCount As Integer

                ReDim sngHostProcessingRate(intDTAProcessingStatCount - 1)

                objEnum = htHosts.GetEnumerator
                Do While objEnum.MoveNext
                    objHostIndex = objEnum.Value

                    If Not objHostIndex Is Nothing Then
                        intHostIndex = CInt(objHostIndex)

                        intNodeCountThisHost = CInt(htHostNodeCount(objEnum.Key))
                        If intNodeCountThisHost < 1 Then intNodeCountThisHost = 1

                        sngHostProcessingRate(intHostIndex) = CSng(intDTAProcessingStats(intHostIndex) / intNodeCountThisHost)
                    End If
                Loop

                ' Determine the median number of spectra processed
                ' First duplicate sngHostProcessingRate so that we can sort it

                ReDim sngHostProcessingRateSorted(sngHostProcessingRate.Length - 1)

                Array.Copy(sngHostProcessingRate, sngHostProcessingRateSorted, sngHostProcessingRate.Length)

                ' Now sort sngHostProcessingRateSorted
                Array.Sort(sngHostProcessingRateSorted, 0, sngHostProcessingRateSorted.Length)

                If sngHostProcessingRateSorted.Length <= 2 Then
                    intMidpoint = 0
                Else
                    intMidpoint = CInt(Math.Floor(sngHostProcessingRateSorted.Length / 2))
                End If

                sngProcessingRateMedian = sngHostProcessingRateSorted(intMidpoint)

                ' Count the number of hosts that had a processing rate fewer than LOW_THRESHOLD_MULTIPLIER times the the median value
                intWarningCount = 0
                sngThresholdRate = CSng(LOW_THRESHOLD_MULTIPLIER * sngProcessingRateMedian)

                For intIndex = 0 To sngHostProcessingRate.Length - 1
                    If sngHostProcessingRate(intIndex) < sngThresholdRate Then
                        intWarningCount += 1
                    End If
                Next

                If intWarningCount > 0 Then
                    strProcessingMsg = "Warning: " & intWarningCount & " host" & CheckForPlurality(intWarningCount) & " processed fewer than " & sngThresholdRate.ToString("0.0") & " DTAs/node, which is " & LOW_THRESHOLD_MULTIPLIER & " times the median value of " & sngProcessingRateMedian.ToString("0.0")
                    If blnLogToConsole Then Console.WriteLine(strProcessingMsg)
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, strProcessingMsg)

                    m_EvalMessage &= "; " & strProcessingMsg
                    m_EvalCode = m_EvalCode Or ERROR_CODE_D
                    blnShowDetailedRates = True
                End If

                ' Count the number of nodes that had a processing rate more than HIGH_THRESHOLD_MULTIPLIER times the median value 
                ' When comparing hosts, have to scale by the number of active nodes on each host
                intWarningCount = 0
                sngThresholdRate = CSng(HIGH_THRESHOLD_MULTIPLIER * sngProcessingRateMedian)

                For intIndex = 0 To sngHostProcessingRate.Length - 1
                    If sngHostProcessingRate(intIndex) > sngThresholdRate Then
                        intWarningCount += 1
                    End If
                Next

                If intWarningCount > 0 Then
                    strProcessingMsg = "Warning: " & intWarningCount & " host" & CheckForPlurality(intWarningCount) & " processed more than " & sngThresholdRate.ToString("0.0") & " DTAs/node, which is " & HIGH_THRESHOLD_MULTIPLIER & " times the median value of " & sngProcessingRateMedian.ToString("0.0")
                    If blnLogToConsole Then Console.WriteLine(strProcessingMsg)
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, strProcessingMsg)

                    m_EvalMessage &= "; " & strProcessingMsg
                    m_EvalCode = m_EvalCode Or ERROR_CODE_E
                    blnShowDetailedRates = True
                End If

                If m_DebugLevel >= 2 OrElse blnShowDetailedRates Then
                    ' Log the number of DTAs processed by each host
                    Dim strHostNames() As String

                    If htHosts.Count > 0 Then
                        ' Copy the key names into a string array so that we can sort them alphabetically

                        ReDim strHostNames(htHosts.Count - 1)
                        htHosts.Keys.CopyTo(strHostNames, 0)

                        Array.Sort(strHostNames, 0, htHosts.Count)

                        For intIndex = 0 To strHostNames.Length - 1
                            objHostIndex = htHosts(strHostNames(intIndex))

                            If Not objHostIndex Is Nothing Then
                                intHostIndex = CInt(objHostIndex)
                                intNodeCountThisHost = CInt(htHostNodeCount(strHostNames(intIndex)))

                                strProcessingMsg = "Host " & strHostNames(intIndex) & " processed " & intDTAProcessingStats(intHostIndex) & " DTA" & CheckForPlurality(intDTAProcessingStats(intHostIndex)) & _
                                                   " using " & intNodeCountThisHost & " node" & CheckForPlurality(intNodeCountThisHost) & _
                                                   " (" & sngHostProcessingRate(intIndex).ToString("0.0") & " DTAs/node)"
                                If blnLogToConsole Then Console.WriteLine(strProcessingMsg)
                                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, strProcessingMsg)
                            End If
                        Next
                    End If
                End If

            Catch ex As Exception
                ' Error occurred

                strProcessingMsg = "Error in validating the stats in ValidateSequestNodeCount" & ex.Message
                If blnLogToConsole Then
                    Console.WriteLine("====================================================================")
                    Console.WriteLine(strProcessingMsg)
                    Console.WriteLine("====================================================================")
                End If

                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, strProcessingMsg)
                Return False
            End Try

        Catch ex As Exception
            ' Error occurred

            strProcessingMsg = "Error parsing Sequest.log file in ValidateSequestNodeCount" & ex.Message
            If blnLogToConsole Then
                Console.WriteLine("====================================================================")
                Console.WriteLine(strProcessingMsg)
                Console.WriteLine("====================================================================")
            End If

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, strProcessingMsg)
            Return False
        End Try

        Return True

    End Function

    Private Function CheckForPlurality(ByVal intValue As Integer) As String
        If intValue = 1 Then
            Return ""
        Else
            Return "s"
        End If
    End Function

    ''' <summary>
    ''' Zips the concatenated .out file
    ''' </summary>
    ''' <param name="WorkDir">Working directory</param>
    ''' <param name="ZipperLoc">Location of file zipping program</param>
    ''' <param name="JobNum">Job number</param>
    ''' <returns>TRUE for success; FALSE for failure</returns>
    ''' <remarks></remarks>
    Protected Overridable Function ZipConcatOutFile(ByVal WorkDir As String, ByVal ZipperLoc As String, ByVal JobNum As String) As Boolean

        Dim OutFileName As String = m_jobParams.GetParam("datasetNum") & "_out.txt"

        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Zipping concatenated output file, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step"))

        'Verify file exists
        If Not File.Exists(Path.Combine(m_WorkDir, OutFileName)) Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Unable to find concatenated .out file")
            Return False
        End If

        clsGlobal.FilesToDelete.Add(OutFileName)

        Try
            'Zip the file
            Dim Zipper As New ZipTools(m_WorkDir, ZipperLoc)
            Dim ZipFileName As String = Path.Combine(m_WorkDir, Path.GetFileNameWithoutExtension(OutFileName)) & ".zip"
            If Not Zipper.MakeZipFile("-fast", ZipFileName, OutFileName) Then
                Dim Msg As String = "Error zipping concat out file, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step")
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, Msg)
                Return False
            End If
        Catch ex As Exception
            Dim Msg As String = "Exception zipping concat out file, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step") & _
             ": " & ex.Message & "; " & clsGlobal.GetExceptionStackTrace(ex)
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, Msg)
            Return False
        End Try

        If m_DebugLevel > 0 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerSeqBase.ZipConcatOutFile(), concatenated outfile zipping successful")
        End If

        Return True

    End Function
#End Region

End Class
