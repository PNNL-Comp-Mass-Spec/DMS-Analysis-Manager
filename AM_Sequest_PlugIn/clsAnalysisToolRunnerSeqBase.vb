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

        'Run Sequest
        CalculateNewStatus()
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

		Dim FileArray() As String
		Dim OutFileCount As Integer

		'Get DTA count
		m_WorkDir = CheckTerminator(m_WorkDir)
		FileArray = Directory.GetFiles(m_WorkDir, "*.dta")
		m_DtaCount = FileArray.GetLength(0)

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

            CalculateNewStatus()
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
