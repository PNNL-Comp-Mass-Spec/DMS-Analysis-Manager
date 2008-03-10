'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2007, Battelle Memorial Institute
' Created 12/19/2007
'
' Last modified 01/17/2008
'*********************************************************************************************************

Imports PRISM.Logging
Imports System.IO
Imports PRISM.Files.clsFileTools
Imports AnalysisManagerBase.clsGlobal

Namespace AnalysisManagerBase

	Public Class clsAnalysisResults

		'*********************************************************************************************************
		'Analysis job results handling class
		'*********************************************************************************************************

#Region "Module variables"
		' access to the job parameters
		Private m_jobParams As IJobParams

		' access to the logger
		Private m_logger As ILogger

		' access to mgr parameters
		Private m_mgrParams As IMgrParams

		' for posting a general explanation for external consumption
		Protected m_message As String
#End Region

#Region "Properties"
		' explanation of what happened to last operation this class performed
		Public ReadOnly Property Message() As String
			Get
				Return m_message
			End Get
		End Property
#End Region

#Region "Methods"
		''' <summary>
		''' Constructor
		''' </summary>
		''' <param name="mgrParams">Manager parameter object</param>
		''' <param name="jobParams">Job parameter object</param>
		''' <param name="logger">Logging object</param>
		''' <remarks></remarks>
		Public Sub New(ByVal mgrParams As IMgrParams, ByVal jobParams As IJobParams, ByVal logger As ILogger)
			m_mgrParams = mgrParams
			m_logger = logger
			m_jobParams = jobParams
		End Sub

		''' <summary>
		''' Copies the results folder to the transfer directory
		''' </summary>
		''' <param name="ResultsFolderName">Name of results folder</param>
		''' <returns>CloseOutType enum indicating success or failure</returns>
		''' <remarks></remarks>
		Public Function DeliverResults(ByVal ResultsFolderName As String) As IJobParams.CloseOutType

			Dim XferDir As String = CheckTerminator(m_jobParams.GetParam("transferFolderPath"))
			Dim WorkDir As String = m_mgrParams.GetParam("workdir")

			'Verify xfer directory exists
			If Not Directory.Exists(XferDir) Then
				m_logger.PostEntry("Transfer folder not found, job " & m_jobParams.GetParam("jobNum"), _
				ILogger.logMsgType.logError, LOG_DATABASE)
				m_message = "Transfer folder not found"
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			'Verify results folder exists
			If Not Directory.Exists(Path.Combine(WorkDir, ResultsFolderName)) Then
				m_logger.PostEntry("Results folder not found, job " & m_jobParams.GetParam("jobNum"), _
				ILogger.logMsgType.logError, LOG_DATABASE)
				m_message = "Results folder not found"
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			'Append machine name to xfer directory, if specified, and create directory if it doesn't exist
			If IsNothing(m_mgrParams.GetParam("AppendMgrNameToXferFolder")) Then
				XferDir = XferDir & m_mgrParams.GetParam("MgrName")
			ElseIf CBool(m_mgrParams.GetParam("AppendMgrNameToXferFolder")) Then
				XferDir = XferDir & m_mgrParams.GetParam("MgrName")
			End If
			If Not Directory.Exists(XferDir) Then
				Try
					Directory.CreateDirectory(XferDir)
				Catch err As Exception
					m_logger.PostError("Unable to create server results folder, job " & m_jobParams.GetParam("jobNum"), _
					 err, LOG_DATABASE)
					m_message = "Unable to create server results folder"
					Return IJobParams.CloseOutType.CLOSEOUT_FAILED
				End Try
			End If

			'Copy results folder to xfer directory
			Try
				CopyDirectory(Path.Combine(WorkDir, ResultsFolderName), Path.Combine(XferDir, ResultsFolderName), True)
			Catch err As Exception
				m_logger.PostError("Error copying results folder, job " & m_jobParams.GetParam("jobNum"), _
				 err, LOG_DATABASE)
				m_message = "Unable to create server results folder"
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End Try

			'Everything must be OK if we got to here
			Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

		End Function
#End Region

	End Class

End Namespace
