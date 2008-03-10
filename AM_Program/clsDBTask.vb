'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2007, Battelle Memorial Institute
' Created 10/26/2007
'
' Last modified 02/14/2008
'*********************************************************************************************************

Imports System.Collections.Specialized
Imports System.Data.SqlClient
Imports PRISM.Logging
Imports AnalysisManagerBase.clsAnalysisMgrSettings

Namespace AnalysisManagerBase

	Public MustInherit Class clsDBTask

		'*********************************************************************************************************
		'Base class for handling task-related data
		'*********************************************************************************************************

#Region "Enums"
		Public Enum RequestTaskResult
			TaskFound = 0
			NoTaskFound = 1
			ResultError = 2
		End Enum
#End Region

#Region "Constants"
		Protected Const RET_VAL_OK As Integer = 0
		Protected Const RET_VAL_TASK_NOT_AVAILABLE As Integer = 53000
#End Region

#Region "Module variables"
		'Logging
		Protected m_Logger As ILogger

		'Manager parameters
		Protected m_MgrParams As IMgrParams
		Protected m_ConnStr As String
		Protected m_BrokerConnStr As String
		Protected m_ErrorList As New StringCollection
		Protected m_DebugLevel As Integer

		'Job status
		Protected m_TaskWasAssigned As Boolean = False
#End Region

#Region "Properties"
		''' <summary>
		''' Value showing if a transfer task was assigned
		''' </summary>
		''' <value></value>
		''' <returns>TRUE if task was assigned; otherwise false</returns>
		''' <remarks></remarks>
		Public ReadOnly Property TaskWasAssigned() As Boolean
			Get
				Return m_TaskWasAssigned
			End Get
		End Property

		''' <summary>
		''' Debug level
		''' </summary>
		''' <value></value>
		''' <returns></returns>
		''' <remarks>Values from 0 (minimum output) to 5 (max detail)</remarks>
		Public Property DebugLevel() As Integer
			Get
				Return m_DebugLevel
			End Get
			Set(ByVal value As Integer)
				m_DebugLevel = value
			End Set
		End Property
#End Region

#Region "Methods"
		''' <summary>
		''' Constructor
		''' </summary>
		''' <param name="MgrParams">An IMgrParams object containing manager parameters</param>
		''' <param name="Logger">An ILogger object to handle logging</param>
		''' <remarks></remarks>
		Protected Sub New(ByVal MgrParams As IMgrParams, ByVal Logger As ILogger, ByVal DebugLvl As Integer)

			m_MgrParams = MgrParams
			m_Logger = Logger
			m_ConnStr = m_MgrParams.GetParam("ConnectionString")
			m_BrokerConnStr = m_MgrParams.GetParam("brokerconnectionstring")
			m_DebugLevel = DebugLvl

		End Sub

		''' <summary>
		''' Requests a task
		''' </summary>
		''' <returns></returns>
		''' <remarks></remarks>
		Public MustOverride Function RequestTask() As RequestTaskResult

		''' <summary>
		''' Closes a task
		''' </summary>
		''' <param name="Success"></param>
		''' <remarks></remarks>
		Public MustOverride Sub CloseTask(ByVal Success As Boolean)

		''' <summary>
		''' Reports database errors to local log
		''' </summary>
		''' <remarks></remarks>
		Protected Sub LogErrorEvents()
			If m_ErrorList.Count > 0 Then
				m_Logger.PostEntry("Warning messages were posted to local log", ILogger.logMsgType.logWarning, True)
			End If
			Dim s As String
			For Each s In m_ErrorList
				m_Logger.PostEntry(s, ILogger.logMsgType.logWarning, True)
			Next
		End Sub

		''' <summary>
		''' Event handler for InfoMessage event
		''' </summary>
		''' <param name="sender"></param>
		''' <param name="args"></param>
		''' <remarks>Errors and warnings from SQL Server are caught here</remarks>
		Private Sub OnInfoMessage(ByVal sender As Object, ByVal args As SqlInfoMessageEventArgs)

			Dim err As SqlError
			Dim s As String
			For Each err In args.Errors
				s = ""
				s &= "Message: " & err.Message
				s &= ", Source: " & err.Source
				s &= ", Class: " & err.Class
				s &= ", State: " & err.State
				s &= ", Number: " & err.Number
				s &= ", LineNumber: " & err.LineNumber
				s &= ", Procedure:" & err.Procedure
				s &= ", Server: " & err.Server
				m_ErrorList.Add(s)
			Next

		End Sub

		''' <summary>
		''' Requests task parameters from database
		''' </summary>
		''' <param name="SpName">Stored procedure to use for task request</param>
		''' <param name="EntityKey">Job number or datasetID, depending on task type</param>
		''' <param name="ConnStr">Connection string for database to be used</param>
		''' <returns>String dictionary containing task parameters if successful. NOTHING on failure</returns>
		''' <remarks></remarks>
		Protected Overridable Function GetTaskParams(ByVal SpName As String, ByVal EntityKey As Integer, _
		  ByVal ConnStr As String) As StringDictionary

			Dim ErrMsg As String
			Dim ResCode As Integer
			Dim Dt As New DataTable

			'Get a data table holding the parameters for one job
			Dim MyCmd As New SqlCommand
			With MyCmd
				.CommandType = CommandType.StoredProcedure
				.CommandText = SpName
				.Parameters.Add(New SqlClient.SqlParameter("@Return", SqlDbType.Int))
				.Parameters.Item("@Return").Direction = ParameterDirection.ReturnValue
				.Parameters.Add(New SqlClient.SqlParameter("@EntityId", SqlDbType.Int))
				.Parameters.Item("@EntityId").Direction = ParameterDirection.Input
				.Parameters.Item("@EntityId").Value = EntityKey
				.Parameters.Add(New SqlClient.SqlParameter("@message", SqlDbType.VarChar, 512))
				.Parameters.Item("@message").Direction = ParameterDirection.InputOutput
				.Parameters.Item("@message").Value = ""
			End With

			If m_DebugLevel > 4 Then
				Dim MyMsg As String = "clsDbTask.GetTaskParams(), connection string: " & ConnStr
				m_Logger.PostEntry(MyMsg, ILogger.logMsgType.logDebug, True)
				MyMsg = "clsDbTask.GetTaskParams(), printing param list"
				m_Logger.PostEntry(MyMsg, ILogger.logMsgType.logDebug, True)
				PrintCommandParams(MyCmd)
			End If

			ResCode = ExecuteSP(MyCmd, Dt, ConnStr)

			'Check for SP errors
			If ResCode <> 0 Then
				'Check for SP errors
				ErrMsg = "clsDBTask.GetTaskParams(), error retrieving job params: " & ResCode.ToString
				m_Logger.PostEntry(ErrMsg, ILogger.logMsgType.logError, True)
				Return Nothing
			End If

			'Verify exactly one row returned
			If Dt.Rows.Count <> 1 Then
				'Wrong number of rows returned
				ErrMsg = "clsDBTask.GetTaskParams(), Incorrect row count retrieving parameters: " & Dt.Rows.Count.ToString
				m_Logger.PostEntry(ErrMsg, ILogger.logMsgType.logError, True)
				Return Nothing
			End If

			'Fill a string dictionary with the job parameters that have been found
			Dim MyRow As DataRow = Dt.Rows(0)
			Dim MyCols As DataColumnCollection = Dt.Columns
			Dim CurCol As DataColumn
			Dim RetDict As New StringDictionary
			Try
				For Each CurCol In MyCols
					'Add the column heading and value to the dictionary
					RetDict.Add(CurCol.ColumnName, DbCStr(MyRow(Dt.Columns(CurCol.ColumnName))))
				Next
				Return RetDict
			Catch ex As System.Exception
				ErrMsg = "clsDBTask.GetTaskParams(), exception filling dictionary; " & ex.Message
				m_Logger.PostEntry(ErrMsg, ILogger.logMsgType.logError, True)
				Return Nothing
			End Try

		End Function

		''' <summary>
		''' Method for executing a db stored procedure, assuming no data table is returned
		''' </summary>
		''' <param name="SpCmd">SQL command object containing stored procedure params</param>
		''' <param name="ConnStr">Db connection string</param>
		''' <returns>Result code returned by SP; -1 if unable to execute SP</returns>
		''' <remarks></remarks>
		Protected Overloads Function ExecuteSP(ByRef SpCmd As SqlCommand, ByVal ConnStr As String) As Integer

			Return ExecuteSP(SpCmd, Nothing, ConnStr)

		End Function

		''' <summary>
		''' Method for executing a db stored procedure if a data table is to be returned
		''' </summary>
		''' <param name="SpCmd">SQL command object containing stored procedure params</param>
		''' <param name="OutTable">NOTHING when called; if SP successful, contains data table on return</param>
		''' <param name="ConnStr">Db connection string</param>
		''' <returns>Result code returned by SP; -1 if unable to execute SP</returns>
		''' <remarks></remarks>
		Protected Overloads Function ExecuteSP(ByRef SpCmd As SqlCommand, ByRef OutTable As DataTable, ByVal ConnStr As String) As Integer

			Dim ResCode As Integer = -9999	'If this value is in error msg, then exception occurred before ResCode was set
			Dim ErrMsg As String
			Dim MyTimer As New System.Diagnostics.Stopwatch
			Dim RetryCount As Integer = 3

			m_ErrorList.Clear()
			While RetryCount > 0	'Multiple retry loop for handling SP execution failures
				Try
					Using Cn As SqlConnection = New SqlConnection(ConnStr)
						AddHandler Cn.InfoMessage, New SqlInfoMessageEventHandler(AddressOf OnInfoMessage)
						Using Da As SqlDataAdapter = New SqlDataAdapter(), Ds As DataSet = New DataSet
							'NOTE: The connection has to be added here because it didn't exist at the time the command object was created
							SpCmd.Connection = Cn
							'Change command timeout from 30 second default in attempt to reduce SP execution timeout errors
							SpCmd.CommandTimeout = CInt(m_MgrParams.GetParam("cmdtimeout"))
							Da.SelectCommand = SpCmd
							MyTimer.Start()
							Da.Fill(Ds)
							MyTimer.Stop()
							ResCode = CInt(Da.SelectCommand.Parameters("@Return").Value)
							If OutTable IsNot Nothing Then OutTable = Ds.Tables(0)
						End Using  'Ds
						RemoveHandler Cn.InfoMessage, AddressOf OnInfoMessage
					End Using  'Cn
					LogErrorEvents()
					Exit While
				Catch ex As System.Exception
					MyTimer.Stop()
					RetryCount -= 1
					ErrMsg = "clsDBTask.ExecuteSP(), exception filling data adapter, " & ex.Message
					ErrMsg &= ". ResCode = " & ResCode.ToString & ". Retry count = " & RetryCount.ToString
					m_Logger.PostEntry(ErrMsg, ILogger.logMsgType.logError, True)
				Finally
					If m_DebugLevel > 1 Then
						ErrMsg = "SP execution time: " & (CDbl(MyTimer.ElapsedMilliseconds) / 1000.0#).ToString("##0.000") & " seconds "
						ErrMsg &= "for SP " & SpCmd.CommandText
						m_Logger.PostEntry(ErrMsg, ILogger.logMsgType.logDebug, True)
					End If
					MyTimer.Reset()
				End Try
				System.Threading.Thread.Sleep(10000)	'Wait 10 seconds before retrying
			End While

			If RetryCount < 1 Then
				'Too many retries, log and return error
				ErrMsg = "Excessive retries executing SP " & SpCmd.CommandText
				m_Logger.PostEntry(ErrMsg, ILogger.logMsgType.logError, True)
				Return -1
			End If

			Return ResCode

		End Function

		''' <summary>
		''' Debugging routine for printing SP calling params
		''' </summary>
		''' <param name="InpCmd">SQL command object containing params</param>
		''' <remarks></remarks>
		Protected Sub PrintCommandParams(ByVal InpCmd As SqlCommand)

			'Verify there really are command paramters
			If InpCmd Is Nothing Then Exit Sub
			If InpCmd.Parameters.Count < 1 Then Exit Sub

			Dim MyMsg As String = ""

			For Each MyParam As SqlParameter In InpCmd.Parameters
				MyMsg &= vbCrLf & "Name= " & MyParam.ParameterName & vbTab & ", Value= " & DbCStr(MyParam.Value)
			Next

			m_Logger.PostEntry("Parameter list:" & MyMsg, ILogger.logMsgType.logDebug, True)

		End Sub

		Protected Function DbCStr(ByVal InpObj As Object) As String

			'If input object is DbNull, returns "", otherwise returns String representation of object
			If InpObj Is DBNull.Value Then
				Return ""
			Else
				Return CStr(InpObj)
			End If

		End Function

		Protected Function DbCSng(ByVal InpObj As Object) As Single

			'If input object is DbNull, returns 0.0, otherwise returns Single representation of object
			If InpObj Is DBNull.Value Then
				Return 0.0
			Else
				Return CSng(InpObj)
			End If

		End Function

		Protected Function DbCDbl(ByVal InpObj As Object) As Double

			'If input object is DbNull, returns 0.0, otherwise returns Double representation of object
			If InpObj Is DBNull.Value Then
				Return 0.0
			Else
				Return CDbl(InpObj)
			End If

		End Function

		Protected Function DbCInt(ByVal InpObj As Object) As Integer

			'If input object is DbNull, returns 0, otherwise returns Integer representation of object
			If InpObj Is DBNull.Value Then
				Return 0
			Else
				Return CInt(InpObj)
			End If

		End Function

		Protected Function DbCLng(ByVal InpObj As Object) As Long

			'If input object is DbNull, returns 0, otherwise returns Integer representation of object
			If InpObj Is DBNull.Value Then
				Return 0
			Else
				Return CLng(InpObj)
			End If

		End Function

		Protected Function DbCDec(ByVal InpObj As Object) As Decimal

			'If input object is DbNull, returns 0, otherwise returns Decimal representation of object
			If InpObj Is DBNull.Value Then
				Return 0
			Else
				Return CDec(InpObj)
			End If

		End Function

		Protected Function DbCShort(ByVal InpObj As Object) As Short

			'If input object is DbNull, returns 0, otherwise returns Short representation of object
			If InpObj Is DBNull.Value Then
				Return 0
			Else
				Return CShort(InpObj)
			End If

		End Function
#End Region

	End Class

End Namespace