'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2006, Battelle Memorial Institute
' Created 06/07/2006
'
' Last modified 01/18/2008
'*********************************************************************************************************

Imports System.IO
Imports PRISM.Files
Imports PRISM.Files.clsFileTools
Imports System.Text.RegularExpressions
Imports PRISM.Logging
Imports AnalysisManagerBase
Imports AnalysisManagerBase.clsGlobal
Imports AnalysisManagerMSMSBase

Public MustInherit Class clsAnalysisToolRunnerSeqBase
	Inherits clsAnalysisToolRunnerMSMS

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
	''' <param name="logger">Logging object</param>
	''' <param name="StatusTools">Object for updating status file as job progresses</param>
	''' <remarks></remarks>
	Public Overrides Sub Setup(ByVal mgrParams As IMgrParams, ByVal jobParams As IJobParams, _
	  ByVal logger As ILogger, ByVal StatusTools As IStatusFile)

		MyBase.Setup(mgrParams, jobParams, logger, StatusTools)

		If m_DebugLevel > 3 Then
			m_logger.PostEntry("clsAnalysisToolRunnerSeqBase.Setup()", ILogger.logMsgType.logDebug, True)
		End If
	End Sub

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
		Dim NumProcessors As Integer = CInt(m_mgrParams.GetParam("numberofprocessors"))

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
			RunProgs(ProcIndx) = New PRISM.Processes.clsProgRunner
			RunProgs(ProcIndx).Name = "Seq" & ProcIndx.ToString
			RunProgs(ProcIndx).CreateNoWindow = CBool(m_mgrParams.GetParam("createnowindow"))
			RunProgs(ProcIndx).Program = m_mgrParams.GetParam("seqprogloc")
			RunProgs(ProcIndx).Arguments = CmdStr & DumStr
			RunProgs(ProcIndx).WorkDir = m_WorkDir
			Textfiles(ProcIndx) = New StreamWriter(DumStr, False)
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
				m_logger.PostEntry("clsAnalysisToolRunnerSeqBase.MakeOutFiles: Closing FileList" & ProcIndx, _
				 ILogger.logMsgType.logDebug, True)
			End If
			Try
				Textfiles(ProcIndx).Close()
				Textfiles(ProcIndx) = Nothing
			Catch Err As Exception
				m_logger.PostEntry("clsAnalysisToolRunnerSeqBase.MakeOutFiles: " & Err.Message, _
				 ILogger.logMsgType.logError, True)
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
			CalculateNewStatus()
			m_StatusTools.UpdateAndWrite(m_progress)
			For ProcIndx = 0 To RunProgs.GetUpperBound(0)
				If m_DebugLevel > 4 Then
					m_logger.PostEntry("clsAnalysisToolRunnerSeqBase.MakeOutFiles(): RunProgs(" & ProcIndx.ToString & ").State = " & _
					 RunProgs(ProcIndx).State.ToString, ILogger.logMsgType.logDebug, True)
				End If
				If (RunProgs(ProcIndx).State <> 0) Then
					If m_DebugLevel > 4 Then
						m_logger.PostEntry("clsAnalysisToolRunnerSeqBase.MakeOutFiles()_2: RunProgs(" & ProcIndx.ToString & ").State = " & _
						 RunProgs(ProcIndx).State.ToString, ILogger.logMsgType.logDebug, True)
					End If
					If (RunProgs(ProcIndx).State <> 10) Then
						If m_DebugLevel > 4 Then
							m_logger.PostEntry("clsAnalysisToolRunnerSeqBase.MakeOutFiles()_3: RunProgs(" & ProcIndx.ToString & ").State = " & _
							 RunProgs(ProcIndx).State.ToString, ILogger.logMsgType.logDebug, True)
						End If
						StillRunning = True
						Exit For
					Else
						If m_DebugLevel > 0 Then
							m_logger.PostEntry("clsAnalysisToolRunnerSeqBase.MakeOutFiles()_4: RunProgs(" & ProcIndx.ToString & ").State = " & _
							 RunProgs(ProcIndx).State.ToString, ILogger.logMsgType.logDebug, True)
						End If
					End If
				End If
			Next
		End While

		'Clean up our object references
		If m_DebugLevel > 0 Then
			m_logger.PostEntry("clsAnalysisToolRunnerSeqBase.MakeOutFiles(), cleaning up runprog object references", _
			 ILogger.logMsgType.logDebug, True)
		End If
		For ProcIndx = 0 To RunProgs.GetUpperBound(0)
			RunProgs(ProcIndx) = Nothing
			If m_DebugLevel > 0 Then
				m_logger.PostEntry("Set RunProgs(" & ProcIndx.ToString & ") to Nothing", _
				 ILogger.logMsgType.logDebug, True)
			End If
		Next

		'Make sure objects are released
		GC.Collect()
		GC.WaitForPendingFinalizers()
		System.Threading.Thread.Sleep(20000)		'20 second delay -- Move this to before GC after troubleshooting complete

		'Verify out file creation
		If m_DebugLevel > 0 Then
			m_logger.PostEntry("clsAnalysisToolRunnerSeqBase.MakeOutFiles(), verifying out file creation", _
			 ILogger.logMsgType.logDebug, True)
		End If
		DtaFiles = Directory.GetFiles(m_WorkDir, "*.out")
		If DtaFiles.GetLength(0) < 1 Then
			m_logger.PostEntry("No OUT files created, job " & m_JobNum, ILogger.logMsgType.logError, LOG_DATABASE)
			m_message = AppendToComment(m_message, "No OUT files created")
			Return IJobParams.CloseOutType.CLOSEOUT_NO_OUT_FILES
		End If

		'Package out files into concatenated text files 
		If Not ConcatOutFiles(m_WorkDir, m_jobParams.GetParam("datasetNum"), m_JobNum) Then
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		'Zip concatenated .out files
		If Not ZipConcatOutFile(m_WorkDir, m_mgrParams.GetParam("zipprogram"), m_JobNum) Then
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		'If we got here, everything worked
		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

	''' <summary>
	''' Deletes stray files (*.csv, lcq*.txt) left behind by Sequest
	''' </summary>
	''' <returns>CloseOutType enum indicating success or failur</returns>
	''' <remarks></remarks>
	Protected Overrides Function DeleteTempAnalFiles() As IJobParams.CloseOutType

		Dim FileList() As String
		Dim TmpFile As String

		Try
			'Delete stray csv files
			FileList = Directory.GetFiles(m_WorkDir, "*.csv")
			For Each TmpFile In FileList
				DeleteFileWithRetries(TmpFile)
			Next
			'Delete the text files
			FileList = Directory.GetFiles(m_WorkDir, "File*.txt")
			For Each TmpFile In FileList
				DeleteFileWithRetries(TmpFile)
			Next
		Catch Err As Exception
			m_logger.PostError("Error cleaning up working directory, job " & m_JobNum, Err, LOG_DATABASE)
			m_message = AppendToComment(m_message, "Error cleaning up working directory")
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

	''' <summary>
	''' Overrides base class method to run Sequest analysis tool
	''' </summary>
	''' <returns>CloseOutType enum indicating success or failur</returns>
	''' <remarks></remarks>
	Public Overrides Function OperateAnalysisTool() As IJobParams.CloseOutType
		Dim StepResult As IJobParams.CloseOutType

		CalculateNewStatus()
		m_StatusTools.UpdateAndWrite(IStatusFile.JobStatus.STATUS_RUNNING, m_progress, m_DtaCount)

		'Make the .out files
		m_logger.PostEntry("Making OUT files, job " & m_JobNum, ILogger.logMsgType.logNormal, LOG_LOCAL_ONLY)
		Try
			StepResult = MakeOUTFiles()
			If StepResult <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
				Return StepResult
			End If
		Catch Err As Exception
			m_logger.PostEntry("clsAnalysisToolRunnerSeqBase.RunTool(), Exception making OUT files, " & Err.Message, _
			 ILogger.logMsgType.logError, True)
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

	End Function

	''' <summary>
	''' Cleans up working directory prior to packaging analysis results
	''' </summary>
	''' <returns>CloseOutType enum indicating success or failur</returns>
	''' <remarks></remarks>
	Public Overrides Function DispositionResults() As IJobParams.CloseOutType
		Dim FileList() As String
		Dim TmpFile As String

		'Delete unzipped concatenated out files
		FileList = Directory.GetFiles(m_WorkDir, "*_out.txt")
		For Each TmpFile In FileList
			Try
				DeleteFileWithRetries(TmpFile)
			Catch ex As Exception
				m_logger.PostEntry("Error: " & ex.Message & " deleting concatenated out file, job " & m_JobNum, ILogger.logMsgType.logError, LOG_DATABASE)
				m_message = AppendToComment(m_message, "Error packaging results")
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End Try
		Next

		'Delete .out  files
		Try
			FileList = Directory.GetFiles(m_WorkDir, "*.out")
			For Each TmpFile In FileList
				DeleteFileWithRetries(TmpFile)
			Next
		Catch Err As Exception
			m_logger.PostError("Error deleting .out files, job " & m_JobNum, Err, LOG_DATABASE)
			m_message = AppendToComment(m_message, "Error deleting .out files")
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

		' do the common stuff (note that this call is last because it included making the results folder)
		MyBase.DispositionResults()

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
			m_logger.PostEntry("clsAnalysisToolRunnerSeqCluster.ConcatOutFiles(), concatenating .out files", _
			  ILogger.logMsgType.logDebug, True)
		End If

		If ConcatTools.ConcatenateFiles(clsConcatToolWrapper.ConcatFileTypes.CONCAT_OUT, DSName) Then
			If m_DebugLevel > 0 Then
				m_logger.PostEntry("clsAnalysisToolRunnerSeqCluster.ConcatOutFiles(), out file concatenation succeeded", _
				  ILogger.logMsgType.logDebug, True)
			End If
			Return True
		Else
			m_logger.PostEntry(ConcatTools.ErrMsg & ", job " & JobNum, ILogger.logMsgType.logError, LOG_DATABASE)
			m_message = AppendToComment(m_message, "Error concatenating out files")
			Return False
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

		m_logger.PostEntry("Zipping concatenated output file, job " & m_JobNum, ILogger.logMsgType.logNormal, LOG_LOCAL_ONLY)

		'Verify file exists
		If Not File.Exists(Path.Combine(m_WorkDir, OutFileName)) Then
			m_logger.PostEntry("Unable to find concatenated .out file", ILogger.logMsgType.logError, True)
			Return False
		End If

		Try
			'Zip the file
			Dim Zipper As New ZipTools(m_WorkDir, ZipperLoc)
			Dim ZipFileName As String = Path.Combine(m_WorkDir, Path.GetFileNameWithoutExtension(OutFileName)) & ".zip"
			If Not Zipper.MakeZipFile("-fast", ZipFileName, OutFileName) Then
				Dim Msg As String = "Error zipping concat out file, job " & m_JobNum
				m_logger.PostEntry(Msg, ILogger.logMsgType.logError, LOG_DATABASE)
				Return False
			End If
		Catch ex As Exception
			Dim Msg As String = "Exception zipping concat out file, job " & m_JobNum & ": " & ex.Message
			m_logger.PostEntry(Msg, ILogger.logMsgType.logError, LOG_DATABASE)
			Return False
		End Try

		'm_CmdRunner = New clsRunDosProgram(m_logger, m_WorkDir)

		'Dim CmdStr As String
		'Dim FileList() As String
		'Dim TmpFile As String
		'Dim CmdRunner As New clsRunDosProgram(m_logger, m_workdir)

		'If m_DebugLevel > 0 Then
		'	m_logger.PostEntry("clsAnalysisToolRunnerSeqCluster.ZipConcatOutFile(), zipping concatenated out file", _
		'	 ILogger.logMsgType.logDebug, True)
		'End If

		'FileList = Directory.GetFiles(WorkDir, "*_out.txt")
		'For Each TmpFile In FileList
		'	'Set up a program runner to zip the file
		'	CmdStr = "-add -fast " & Path.Combine(WorkDir, Path.GetFileNameWithoutExtension(TmpFile)) & ".zip " & TmpFile
		'	If Not CmdRunner.RunProgram(ZipperLoc, CmdStr, "Zipper", True) Then
		'		m_logger.PostEntry("Error zipping output files, job " & JobNum, ILogger.logMsgType.logError, LOG_DATABASE)
		'		m_message = AppendToComment(m_message, "Error zipping concatenated out files")
		'		Return False
		'	End If
		'Next

		If m_DebugLevel > 0 Then
			m_logger.PostEntry("clsAnalysisToolRunnerSeqCluster.ZipConcatOutFile(), concatenated outfile zipping successful", _
			 ILogger.logMsgType.logDebug, True)
		End If

		Return True

	End Function
#End Region

End Class
