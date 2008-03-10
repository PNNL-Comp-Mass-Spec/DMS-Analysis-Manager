'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2007, Battelle Memorial Institute
' Created 12/18/2007
'
' Last modified 01/16/2008
'*********************************************************************************************************

Imports System.Collections.Specialized
Imports System.Data.SqlClient
Imports PRISM.Logging
Imports AnalysisManagerBase.clsGlobal
Imports AnalysisManagerBase.clsAnalysisMgrSettings

Namespace AnalysisManagerBase

	Public Class clsAnalysisJob
		Inherits clsDBTask
		Implements IJobParams

		'*********************************************************************************************************
		'Provides DB access and tools for one analysis job
		'*********************************************************************************************************

#Region "Constants"
		Protected Const SP_NAME_SET_COMPLETE As String = "SetAnalysisTaskComplete"
		Protected Const SP_NAME_REQUEST_TASK As String = "RequestAnalysisTask"
		Protected Const SP_NAME_REQUEST_TASK_PARAMS As String = "RequestAnalysisTaskParams"
#End Region

#Region "Module variables"
		Protected m_JobParams As New StringDictionary
		Protected m_JobId As Integer
#End Region

#Region "Methods"
		''' <summary>
		''' Constructor
		''' </summary>
		''' <param name="mgrParams">IMgrParams object containing manager parameters</param>
		''' <param name="Logger">ILogger object for logging</param>
		''' <remarks></remarks>
		Public Sub New(ByVal mgrParams As IMgrParams, ByVal Logger As ILogger, ByVal DebugLvl As Integer)

			MyBase.New(mgrParams, Logger, DebugLvl)

			'Ensure job parameters collection has been cleared
			m_JobParams.Clear()

		End Sub

		''' <summary>
		''' Gets a task parameter from the task params class
		''' </summary>
		''' <param name="Name">Key name for parameter</param>
		''' <returns>Value for specified parameter</returns>
		''' <remarks></remarks>
		Public Function GetParam(ByVal Name As String) As String Implements IJobParams.GetParam

			Return m_JobParams(Name)

		End Function

		''' <summary>
		''' Adds a parameter to the class
		''' </summary>
		''' <param name="ParamName">Name of parameter</param>
		''' <param name="ParamValue">Value for parameter</param>
		''' <returns>TRUE for success, FALSE for error</returns>
		''' <remarks></remarks>
		Public Function AddAdditionalParameter(ByVal ParamName As String, ByVal ParamValue As String) As Boolean _
		 Implements IJobParams.AddAdditionalParameter

			Try
				m_JobParams.Add(ParamName, ParamValue)
				Return True
			Catch ex As Exception
				Dim Msg As String = "Exception adding parameter: " & ParamName & " Value: " & ParamValue
				m_Logger.PostEntry(Msg, ILogger.logMsgType.logError, LOG_LOCAL_ONLY)
				Return False
			End Try

		End Function

		''' <summary>
		''' Requests a task from the database
		''' </summary>
		''' <returns>Enum indicating if task was found</returns>
		''' <remarks></remarks>
		Public Overrides Function RequestTask() As clsDBTask.RequestTaskResult

			Dim RetVal As RequestTaskResult

			RetVal = RequestAnalysisJob()
			Select Case RetVal
				Case clsDBTask.RequestTaskResult.NoTaskFound
					m_TaskWasAssigned = False
				Case clsDBTask.RequestTaskResult.TaskFound
					m_TaskWasAssigned = True
				Case Else
					m_TaskWasAssigned = False
			End Select
			Return RetVal

		End Function

		''' <summary>
		''' Requests a single analysis job
		''' </summary>
		''' <returns></returns>
		''' <remarks></remarks>
		Private Function RequestAnalysisJob() As clsDBTask.RequestTaskResult

			Dim MyCmd As New SqlCommand
			Dim Outcome As clsDBTask.RequestTaskResult = RequestTaskResult.NoTaskFound
			Dim RetVal As Integer

			Try
				'Set up the command object prior to SP execution
				With MyCmd
					.CommandType = CommandType.StoredProcedure
					.CommandText = SP_NAME_REQUEST_TASK
					.Parameters.Add(New SqlClient.SqlParameter("@Return", SqlDbType.Int))
					.Parameters.Item("@Return").Direction = ParameterDirection.ReturnValue
					.Parameters.Add(New SqlClient.SqlParameter("@processorName", SqlDbType.VarChar, 128))
					.Parameters.Item("@processorName").Direction = ParameterDirection.Input
					.Parameters.Item("@processorName").Value = m_MgrParams.GetParam("MgrName")
					.Parameters.Add(New SqlClient.SqlParameter("@jobNum", SqlDbType.Int))
					.Parameters.Item("@jobNum").Direction = ParameterDirection.Output
					.Parameters.Add(New SqlClient.SqlParameter("@message", SqlDbType.VarChar, 512))
					.Parameters.Item("@message").Direction = ParameterDirection.InputOutput
					.Parameters.Item("@message").Value = ""
					.Parameters.Add(New SqlClient.SqlParameter("@infoOnly", SqlDbType.TinyInt))
					.Parameters.Item("@infoOnly").Direction = ParameterDirection.Input
					.Parameters.Item("@infoOnly").Value = 0
				End With

				If m_DebugLevel > 4 Then
					Dim MyMsg As String = "clsAnalysisJob.RequestAnalysisJob(), connection string: " & m_BrokerConnStr
					m_Logger.PostEntry(MyMsg, ILogger.logMsgType.logDebug, True)
					MyMsg = "clsAnalysisJob.RequestAnalysisJob(), printing param list"
					m_Logger.PostEntry(MyMsg, ILogger.logMsgType.logDebug, True)
					PrintCommandParams(MyCmd)
				End If

				'Execute the SP
				RetVal = ExecuteSP(MyCmd, m_BrokerConnStr)

				Select Case RetVal
					Case RET_VAL_OK
						'No errors found in SP call, so see of any results tasks were found
						m_JobId = CInt(MyCmd.Parameters("@jobNum").Value)
						'Analysis task was found; get the data for it
						If AddTaskParamsToDictionary(m_JobId) Then
							Outcome = clsDBTask.RequestTaskResult.TaskFound
						Else
							Outcome = clsDBTask.RequestTaskResult.ResultError
						End If  'Addition of parameters to dictionary
					Case RET_VAL_TASK_NOT_AVAILABLE
						'No jobs found
						Outcome = clsDBTask.RequestTaskResult.NoTaskFound
					Case Else
						'There was an SP error
						Dim msg As String = "clsAnalysisJob.RequestAnalysisJob(), SP execution error " & RetVal.ToString
						msg &= "; Msg text = " & CStr(MyCmd.Parameters("@message").Value)
						m_Logger.PostEntry(msg, ILogger.logMsgType.logError, True)
						Outcome = RequestTaskResult.ResultError
				End Select

			Catch ex As System.Exception
				m_Logger.PostError("Exception requesting analysis job: ", ex, True)
				Outcome = RequestTaskResult.ResultError
			End Try

			Return Outcome

		End Function

		''' <summary>
		''' Loads task parameters into the parameter dictionary
		''' </summary>
		''' <param name="TaskID">Task ID to obtain parameters for</param>
		''' <returns>TRUE for success, FALSE for failure</returns>
		''' <remarks></remarks>
		Private Function AddTaskParamsToDictionary(ByVal TaskID As Integer) As Boolean

			'Finds the transfer task parameters for the specified job number and loads them into the parameters dictionary
			Dim Msg As String

			m_JobParams = GetTaskParams(SP_NAME_REQUEST_TASK_PARAMS, TaskID, m_ConnStr)
			If m_JobParams IsNot Nothing Then
				Return True
			Else
				'There was an error
				Msg = "clsAnalysisJob.AddTaskParamsToDictionary(), Unable to obtain job data"
				m_Logger.PostEntry(Msg, ILogger.logMsgType.logError, True)
				Return False
			End If

		End Function

		''' <summary>
		''' Dummy sub to meet MustOverride requirement of base class
		''' </summary>
		''' <param name="Success"></param>
		''' <remarks></remarks>
		Public Overrides Sub CloseTask(ByVal Success As Boolean)

			'Do nothing

		End Sub

		''' <summary>
		''' Closes an analysis job
		''' </summary>
		''' <param name="CloseOut">IJobParams enum specifying close out type</param>
		''' <param name="resultsFolderName">Name of results folder for job</param>
		''' <param name="Comment">Comment to be added to database upon closeout</param>
		''' <remarks>Overloads the CloseTask sub inherited from base class</remarks>
		Public Overloads Sub CloseTask(ByVal CloseOut As IJobParams.CloseOutType, _
		  ByVal resultsFolderName As String, ByVal Comment As String)

			'NOTE: This sub actually overrides and overloads sub CloseTask in base class

			Dim MsgStr As String
			Dim CompCode As Integer

			CompCode = CInt(CloseOut)
			If Not SetAnalysisJobComplete(SP_NAME_SET_COMPLETE, CompCode, resultsFolderName, Comment, m_ConnStr) Then
				MsgStr = "Error setting job complete in database, job " & m_JobId
				m_Logger.PostEntry(MsgStr, ILogger.logMsgType.logError, LOG_LOCAL_ONLY)
			End If

		End Sub

		''' <summary>
		''' Communicates with database to perform job closeout
		''' </summary>
		''' <param name="SpName">Name of SP in database to call for closeout</param>
		''' <param name="CompletionCode">Integer version of ITaskParams specifying closeout type</param>
		''' <param name="resultsFolderName">Name of results folder for job</param>
		''' <param name="Comment">Comment to insert in database</param>
		''' <param name="ConnStr">Database connection string</param>
		''' <returns>True for success, False for failure</returns>
		''' <remarks></remarks>
		Protected Function SetAnalysisJobComplete(ByVal SpName As String, ByVal CompletionCode As Integer, _
		 ByVal ResultsFolderName As String, ByVal Comment As String, ByVal ConnStr As String) As Boolean

			Dim Outcome As Boolean = False
			Dim ResCode As Integer

			'Setup for execution of the stored procedure
			Dim MyCmd As New SqlCommand
			With MyCmd
				.CommandType = CommandType.StoredProcedure
				.CommandText = SpName
				.Parameters.Add(New SqlClient.SqlParameter("@Return", SqlDbType.Int))
				.Parameters.Item("@Return").Direction = ParameterDirection.ReturnValue
				.Parameters.Add(New SqlClient.SqlParameter("@jobNum", SqlDbType.Int))
				.Parameters.Item("@jobNum").Direction = ParameterDirection.Input
				.Parameters.Item("@jobNum").Value = m_JobId
				.Parameters.Add(New SqlClient.SqlParameter("@completionCode", SqlDbType.Int))
				.Parameters.Item("@completionCode").Direction = ParameterDirection.Input
				.Parameters.Item("@completionCode").Value = CompletionCode
				.Parameters.Add(New SqlClient.SqlParameter("@resultsFolderName", SqlDbType.VarChar, 64))
				.Parameters.Item("@resultsFolderName").Direction = ParameterDirection.Input
				.Parameters.Item("@resultsFolderName").Value = ResultsFolderName
				.Parameters.Add(New SqlClient.SqlParameter("@comment", SqlDbType.VarChar, 255))
				.Parameters.Item("@comment").Direction = ParameterDirection.Input
				.Parameters.Item("@comment").Value = Comment
				.Parameters.Add(New SqlClient.SqlParameter("@organismDBName", SqlDbType.VarChar, 64))
				.Parameters.Item("@organismDBName").Direction = ParameterDirection.Input
				.Parameters.Item("@organismDBName").Value = CStr(IIf(m_JobParams.ContainsKey("generatedFastaName"), _
				 m_JobParams.Item("generatedFastaName"), ""))
			End With

			'Execute the SP
			ResCode = ExecuteSP(MyCmd, ConnStr)

			If ResCode = 0 Then
				Outcome = True
			Else
				Dim Msg As String = "Error " & ResCode.ToString & " setting analysis job complete"
				'			Msg &= "; Message = " & CStr(MyCmd.Parameters("@message").Value)
				m_Logger.PostEntry(Msg, ILogger.logMsgType.logError, True)
				Outcome = False
			End If

			Return Outcome

		End Function

		Protected Overrides Function GetTaskParams(ByVal SpName As String, ByVal EntityKey As Integer, _
		ByVal ConnStr As String) As StringDictionary

			Dim RetDict As StringDictionary = MyBase.GetTaskParams(SpName, EntityKey, ConnStr)
			If RetDict Is Nothing Then
				'Something went wrong, we need to fail the job. Errors were logged in base class
				Me.CloseTask(AnalysisManagerBase.IJobParams.CloseOutType.CLOSEOUT_FAILED, "", "")
			End If

			Return RetDict

		End Function
#End Region

	End Class

End Namespace
