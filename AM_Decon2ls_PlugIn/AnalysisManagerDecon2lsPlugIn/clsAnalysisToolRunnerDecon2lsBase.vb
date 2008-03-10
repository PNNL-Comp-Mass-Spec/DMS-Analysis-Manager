'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2006, Battelle Memorial Institute
' Created 09/14/2006
'
' Last modified 10/30/2006
'*********************************************************************************************************

Imports System.IO
Imports PRISM.Logging
'Imports PRISM.Processes
Imports AnalysisManagerBase
'Imports AnalysisManagerBase.clsGlobal
Imports Decon2LS.Readers
Imports System.Threading
'Imports System.Runtime.Remoting
'Imports System.Runtime.Remoting.Channels
'Imports System.Runtime.Remoting.Channels.tcp
Imports Decon2LSRemoter

Public MustInherit Class clsAnalysisToolRunnerDecon2lsBase
	Inherits clsAnalysisToolRunnerBase

	'*********************************************************************************************************
	'Base class for Decon2LS-specific tasks. Handles tasks common to using Decon2LS for deisotoping and TIC 
	'generation
	'
	'This version uses .Net remoting to communicate with a separate process that runs Decon2LS. The separate
	'process is required due to Decon2LS' use of a Finnigan library that puts a lock on the raw data file, preventing
	'cleanup of the working directory after a job completes. Killing the Decon2LS process will
	'release the file lock.
	'*********************************************************************************************************

#Region "Module variables"
	Protected m_ToolObj As clsDecon2LSRemoter	 'Remote class for execution of Decon2LS via .Net remoting
	Protected m_AnalysisType As String
	Protected m_RemotingTools As clsRemotingTools
	Protected m_ServerRunning As Boolean = False
#End Region

#Region "Methods"
	Public Sub New()

	End Sub

	Public Overrides Function RunTool() As IJobParams.CloseOutType

		'Runs the Decon2LS analysis tool. The actual tool version details (deconvolute or TIC) will be handled by a subclass

		Dim RawDataType As String = m_jobParams.GetParam("RawDataType")
		Dim TcpPort As Integer = CInt(m_mgrParams.GetParam("tcpport"))

		If m_DebugLevel > 3 Then
			m_logger.PostEntry("clsAnalysisToolRunnerDecon2LSBase.RunTool()", ILogger.logMsgType.logDebug, True)
		End If

		''Start the remoting server
		'If Not m_RemotingTools.StartSvr Then
		'	m_logger.PostEntry("clsAnalysisToolRunnerDecon2lsBase.RunTool(), Remoting server startup failed", _
		'	 ILogger.logMsgType.logError, True)
		'	m_message = "Remoting server startup problem"
		'	Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		'End If

		''Delay 5 seconds to allow server to start up
		'System.Threading.Thread.Sleep(5000)

		'Get the setup file by running the base class method
		If Not MyBase.RunTool = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
			'Error message is generated in base class, so just exit with error
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		'	Get file type
		Dim FileType As Decon2LS.Readers.FileType = GetInputFileType(RawDataType)
		If FileType = FileType.UNDEFINED Then
			m_logger.PostEntry("clsAnalysisToolRunnerDecon2lsBase.RunTool(), Invalid data file type specifed while getting file type: " & RawDataType, _
			 ILogger.logMsgType.logError, True)
			m_message = "Invalid raw data type specified"
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		'	Specify output file name
		Dim OutFileName As String = Path.Combine(m_workdir, m_jobParams.GetParam("datasetNum"))

		'	Specify Input file or folder
		Dim InpFileName As String = SpecifyInputFileName(RawDataType)
		If InpFileName = "" Then
			m_logger.PostEntry("clsAnalysisToolRunnerDecon2lsBase.RunTool(), Invalid data file type specifed while input file name: " & RawDataType, _
			 ILogger.logMsgType.logError, True)
			m_message = "Invalid raw data type specified"
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		'Create an object to handle the .Net Remoting tasks
		m_RemotingTools = New clsRemotingTools(m_logger, m_DebugLevel, TcpPort)

		'Start the remoting server
		If Not m_RemotingTools.StartSvr Then
			m_logger.PostEntry("clsAnalysisToolRunnerDecon2lsBase.RunTool(), Remoting server startup failed", _
			 ILogger.logMsgType.logError, True)
			m_message = "Remoting server startup problem"
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		'Delay 5 seconds to allow server to start up
		System.Threading.Thread.Sleep(5000)

		'Init the Decon2LS wrapper
		Try
			'Instantiate the remote object
			m_ToolObj = New clsDecon2LSRemoter
			With m_ToolObj
				.ResetState()
				.DataFile = InpFileName
				.DeconFileType = FileType
				.OutFile = OutFileName
				.ParamFile = Path.Combine(m_workdir, m_jobParams.GetParam("parmFileName"))
			End With
		Catch ex As System.Exception
			m_logger.PostEntry("clsAnalysisToolRunnerDecon2lsBase.RunTool(), Error initializing Decon2LS: " & ex.Message, _
			  ILogger.logMsgType.logError, True)
			m_message = "Error initializing Decon2LS"
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

		'Start Decon2LS via the subclass in a separate thread
		Dim Decon2LSThread As New Thread(AddressOf StartDecon2LS)
		Decon2LSThread.Start()

		'Wait for Decon2LS to finish
		System.Threading.Thread.Sleep(1000)		  'Pause to ensure Decon2LS has adequate time to start
		WaitForDecon2LSFinish()

		'Stop the analysis timer
		m_StopTime = Now

		If m_DebugLevel > 3 Then
			m_logger.PostEntry("clsAnalysisToolRunnerDecon2lsBase.RunTool(), Decon2LS finished", ILogger.logMsgType.logDebug, True)
		End If

		'Determine reason for Decon2LS finish
		Select Case m_ToolObj.DeconState
			Case DMSDecon2LS.DeconState.DONE
				'This is normal, do nothing else
			Case DMSDecon2LS.DeconState.ERROR
				m_logger.PostEntry("clsAnalysisToolRunnerDecon2lsBase.RunTool(), Decon2LS error: " & m_ToolObj.ErrMsg, _
				 ILogger.logMsgType.logError, True)
				m_message = "Decon2LS error"
				KillDecon2LSObject()
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			Case DMSDecon2LS.DeconState.IDLE
				'Shouldn't ever get here
				m_logger.PostEntry("clsAnalysisToolRunnerDecon2lsBase.RunTool(), Decon2LS invalid state: IDLE", _
				  ILogger.logMsgType.logError, True)
				m_message = "Decon2LS error"
				KillDecon2LSObject()
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Select

		'Delay to allow Decon2LS a chance to close all files
		System.Threading.Thread.Sleep(5000)		 '5 seconds

		'Kill the Decon2LS object and stop the remoting server
		KillDecon2LSObject()

		'Delete the raw data files
		If m_DebugLevel > 3 Then
			m_logger.PostEntry("clsAnalysisToolRunnerDecon2lsBase.RunTool(), Deleting raw data file", ILogger.logMsgType.logDebug, True)
		End If
		If DeleteRawDataFiles(m_jobparams.GetParam("RawDataType")) <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
			m_logger.PostEntry("clsAnalysisToolRunnerDecon2lsBase.RunTool(), Problem deleting raw data files: " & m_message, _
			 ILogger.logMsgType.logError, True)
			m_message = "Error deleting raw data files"
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		'Update the job summary file
		If m_DebugLevel > 3 Then
			m_logger.PostEntry("clsAnalysisToolRunnerDecon2lsBase.RunTool(), Updating summary file", ILogger.logMsgType.logDebug, True)
		End If
		UpdateSummaryFile()

		'Make the results folder
		If m_DebugLevel > 3 Then
			m_logger.PostEntry("clsAnalysisToolRunnerDecon2lsBase.RunTool(), Making results folder", ILogger.logMsgType.logDebug, True)
		End If
		If MakeResultsFolder(m_AnalysisType) <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
			'MakeResultsFolder handles posting to local log, so set database error message and exit
			m_message = "Error making results folder"
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

	End Function

	Protected MustOverride Sub StartDecon2LS()	 'Uses overrides in subclasses to handle details of starting Decon2LS

	Protected Overridable Sub CalculateNewStatus()

		'Get the percent complete status from Decon2LS
		m_progress = CSng(m_ToolObj.PercentDone)

	End Sub

	Protected Function GetInputFileType(ByVal RawDataType As String) As Decon2LS.Readers.FileType

		'Gets the Decon2LS file type based on the input data type
		Select Case RawDataType.ToLower
			Case "dot_raw_files"
				Return FileType.FINNIGAN
			Case "dot_wiff_files"
				Return FileType.AGILENT_TOF
			Case "dot_raw_folder"
				Return FileType.MICROMASSRAWDATA
			Case "zipped_s_folders"
				If m_jobParams.GetParam("instClass").ToLower = "brukerftms" Then
					'Data off of Bruker FTICR
					Return FileType.ICR2LSRAWDATA
				ElseIf m_jobParams.GetParam("instClass").ToLower = "finnigan_fticr" Then
					'Data from old Finnigan FTICR
					Return FileType.SUNEXTREL
				Else
					'Should never get here
					Return FileType.UNDEFINED
				End If
			Case Else
				'Should never get this value
				Return FileType.UNDEFINED
		End Select

	End Function

	Protected Function SpecifyInputFileName(ByVal RawDataType As String) As String

		'Based on the raw data type, assembles a string telling Decon2LS the name of the input file or folder
		Select Case RawDataType.ToLower
			Case "dot_raw_files"
				Return Path.Combine(m_workdir, m_jobParams.GetParam("datasetNum") & ".raw")
			Case "dot_wiff_files"
				Return Path.Combine(m_workdir, m_jobParams.GetParam("datasetNum") & ".wiff")
			Case "dot_raw_folder"
				Return Path.Combine(m_workdir, m_jobParams.GetParam("datasetNum")) & ".raw/_FUNC001.DAT"
			Case "zipped_s_folders"
				Return Path.Combine(m_workdir, m_jobParams.GetParam("datasetNum"))
			Case Else
				'Should never get this value
				Return ""
		End Select

	End Function

	Protected Sub WaitForDecon2LSFinish()

		'Loops while waiting for Decon2LS to finish running

		Dim CurState As DMSDecon2LS.DeconState = m_ToolObj.DeconState
		While (CurState = DMSDecon2LS.DeconState.RUNNING_DECON) Or (CurState = DMSDecon2LS.DeconState.RUNNING_TIC)
			'Update the % completion
			CalculateNewStatus()
			'Update the status file
			m_StatusTools.UpdateAndWrite(m_progress)
			'Wait 5 seconds, then get a new Decon2LS state
			System.Threading.Thread.Sleep(5000)
			CurState = m_ToolObj.DeconState
			Debug.WriteLine("Current Scan: " & m_ToolObj.CurrentScan)
			If m_DebugLevel > 4 Then
				m_logger.PostEntry("clsAnalysisToolRunnerDecon2lsBase.WaitForDecon2LSFinish(), Scan " & m_ToolObj.CurrentScan, _
				 ILogger.logMsgType.logDebug, True)
			End If
		End While

	End Sub

	Protected Function DeleteRawDataFiles(ByVal RawDataType As String) As IJobParams.CloseOutType

		'Deletes the raw data files/folders from the working directory
		Dim IsFile As Boolean = True
		Dim FileOrFolderName As String

		Select Case RawDataType.ToLower
			Case "dot_raw_files"
				FileOrFolderName = Path.Combine(m_workdir, m_jobParams.GetParam("datasetNum") & ".raw")
				IsFile = True
			Case "dot_wiff_files"
				FileOrFolderName = Path.Combine(m_workdir, m_jobParams.GetParam("datasetNum") & ".wiff")
				IsFile = True
			Case "dot_raw_folder"
				FileOrFolderName = Path.Combine(m_workdir, m_jobParams.GetParam("datasetNum") & ".raw")
				IsFile = False
			Case "zipped_s_folders"
				FileOrFolderName = Path.Combine(m_workdir, m_jobParams.GetParam("datasetNum"))
				IsFile = False
			Case Else
				'Should never get this value
				m_message = "DeleteRawDataFiles, Invalid RawDataType specified: " & RawDataType
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Select

		If IsFile Then
			'Data is a file, so use file deletion tools
			If DeleteFileWithRetries(FileOrFolderName) Then
				Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
			Else
				m_message = "Error deleting raw data file " & FileOrFolderName
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If
		Else
			'Use folder deletion tools
			Try
				Directory.Delete(FileOrFolderName, True)
				Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
			Catch ex As System.Exception
				m_message = "Exception deleting raw data folder " & FileOrFolderName & ": " & ex.Message
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End Try
		End If

	End Function

	Protected Sub KillDecon2LSObject()

		'Removes the Decon2LS object
		If Not IsNothing(m_ToolObj) Then
			m_ToolObj.Dispose()
			m_ToolObj = Nothing
		End If

		'Stop remoting server
		m_RemotingTools.StopSvr()		 'At present, no action other than logging is being taken if there is a problem stopping the server.

	End Sub
#End Region

End Class
