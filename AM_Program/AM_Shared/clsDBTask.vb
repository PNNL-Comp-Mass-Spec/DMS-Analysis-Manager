'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2007, Battelle Memorial Institute
' Created 10/26/2007
'
' Last modified 06/11/2009 JDS - Added logging using log4net
'*********************************************************************************************************

Option Strict On

Imports System.Data.SqlClient
Imports System.Xml.XPath
Imports System.Xml
Imports System.IO

Public MustInherit Class clsDBTask

	'*********************************************************************************************************
	'Base class for handling task-related data
	'*********************************************************************************************************

#Region "Enums"
	Public Enum RequestTaskResult
		TaskFound = 0
		NoTaskFound = 1
		ResultError = 2
		TooManyRetries = 3
		Deadlock = 4
	End Enum
#End Region

#Region "Constants"
	Public Const RET_VAL_OK As Integer = 0
	Public Const RET_VAL_EXCESSIVE_RETRIES As Integer = -5			 ' Timeout expired
	Public Const RET_VAL_DEADLOCK As Integer = -4					 ' Transaction (Process ID 143) was deadlocked on lock resources with another process and has been chosen as the deadlock victim
	Public Const RET_VAL_TASK_NOT_AVAILABLE As Integer = 53000
	Public Const DEFAULT_SP_RETRY_COUNT As Integer = 3
#End Region

#Region "Module variables"

	'Manager parameters
	Protected m_MgrParams As IMgrParams
	Protected m_ConnStr As String
	Protected m_BrokerConnStr As String
	Protected m_ErrorList As New Generic.List(Of String)
	Protected m_DebugLevel As Integer

	'Job status
	Protected m_TaskWasAssigned As Boolean = False

	Protected m_Xml_Text As String
#End Region

#Region "Structures"
	Public Structure udtParameterInfoType
		Public Section As String
		Public ParamName As String
		Public Value As String
	End Structure
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
	''' <remarks></remarks>
	Protected Sub New(ByVal MgrParams As IMgrParams, ByVal DebugLvl As Integer)

		m_MgrParams = MgrParams
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
	''' Closes out a task
	''' </summary>
	''' <param name="CloseOut"></param>
	''' <param name="CompMsg"></param>
	''' <remarks></remarks>
	Public MustOverride Sub CloseTask(ByVal CloseOut As IJobParams.CloseOutType, ByVal CompMsg As String)

	''' <summary>
	''' Closes out a task (includes EvalCode and EvalMessgae)
	''' </summary>
	''' <param name="CloseOut"></param>
	''' <param name="CompMsg"></param>
	''' <param name="EvalCode">Evaluation code (0 if no special evaulation message)</param>
	''' <param name="EvalMessage">Evaluation message ("" if no special message)</param>
	''' <remarks></remarks>
	Public MustOverride Sub CloseTask(ByVal CloseOut As IJobParams.CloseOutType, ByVal CompMsg As String, ByVal EvalCode As Integer, ByVal EvalMessage As String)

	''' <summary>
	''' Reports database errors to local log
	''' </summary>
	''' <remarks></remarks>
	Protected Sub LogErrorEvents()
		If m_ErrorList.Count > 0 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Errors reported when calling stored procedure")
		End If
		Dim s As String
		For Each s In m_ErrorList
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, s)
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

	Protected Function FillParamDictXml(ByVal InpXml As String) As Generic.List(Of udtParameterInfoType)

		Dim ErrMsg As String

		Try
			' Read XML string into XPathDocument object 
			' and set up navigation objects to traverse it

			Dim xReader As XmlReader = New XmlTextReader(New StringReader(InpXml))
			Dim xdoc As New XPathDocument(xReader)
			Dim xpn As XPathNavigator = xdoc.CreateNavigator()
			Dim nodes As XPathNodeIterator = xpn.Select("//item")

			Dim dctParameters As New Generic.List(Of udtParameterInfoType)
			Dim udtParamInfo As udtParameterInfoType

			' Traverse the parsed XML document and extract the key and value for each item
			While nodes.MoveNext()
				' Extract section, key, and value from XML element and append entry to dctParameterInfo
				udtParamInfo.ParamName = nodes.Current.GetAttribute("key", "")
				udtParamInfo.Value = nodes.Current.GetAttribute("value", "")

				' Extract the section name for the current item and dump it to output
				Dim nav2 As Xml.XPath.XPathNavigator = nodes.Current.Clone
				nav2.MoveToParent()
				udtParamInfo.Section = nav2.GetAttribute("name", "")

				dctParameters.Add(udtParamInfo)

			End While

			Return dctParameters

		Catch ex As System.Exception
			ErrMsg = "clsDBTask.FillParamDict(), exception filling dictionary; " & ex.Message & "; " & clsGlobal.GetExceptionStackTrace(ex)
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, ErrMsg)
			Return Nothing
		End Try

	End Function

	''' <summary>
	''' Method for executing a db stored procedure, assuming no data table is returned; will retry the call to the procedure up to DEFAULT_SP_RETRY_COUNT=3 times
	''' </summary>
	''' <param name="SpCmd">SQL command object containing stored procedure params</param>
	''' <param name="ConnStr">Db connection string</param>
	''' <returns>Result code returned by SP; -1 if unable to execute SP</returns>
	''' <remarks></remarks>
	Public Function ExecuteSP(ByRef SpCmd As SqlCommand, ByVal ConnStr As String) As Integer

		Return ExecuteSP(SpCmd, Nothing, ConnStr, DEFAULT_SP_RETRY_COUNT)

	End Function

	''' <summary>
	''' Method for executing a db stored procedure, assuming no data table is returned
	''' </summary>
	''' <param name="SpCmd">SQL command object containing stored procedure params</param>
	''' <param name="ConnStr">Db connection string</param>
	''' <param name="MaxRetryCount">Maximum number of times to attempt to call the stored procedure</param>
	''' <returns>Result code returned by SP; -1 if unable to execute SP</returns>
	''' <remarks></remarks>
	Public Function ExecuteSP(ByRef SpCmd As SqlCommand, ByVal ConnStr As String, ByVal MaxRetryCount As Integer) As Integer

		Return ExecuteSP(SpCmd, Nothing, ConnStr, MaxRetryCount)

	End Function

	''' <summary>
	''' Method for executing a db stored procedure if a data table is to be returned; will retry the call to the procedure up to DEFAULT_SP_RETRY_COUNT=3 times
	''' </summary>
	''' <param name="SpCmd">SQL command object containing stored procedure params</param>
	''' <param name="OutTable">NOTHING when called; if SP successful, contains data table on return</param>
	''' <param name="ConnStr">Db connection string</param>
	''' <returns>Result code returned by SP; -1 if unable to execute SP</returns>
	''' <remarks></remarks>
	Public Function ExecuteSP(ByRef SpCmd As SqlCommand, ByRef OutTable As DataTable, ByVal ConnStr As String) As Integer
		Return ExecuteSP(SpCmd, OutTable, ConnStr, DEFAULT_SP_RETRY_COUNT)
	End Function

	''' <summary>
	''' Method for executing a db stored procedure if a data table is to be returned
	''' </summary>
	''' <param name="SpCmd">SQL command object containing stored procedure params</param>
	''' <param name="OutTable">NOTHING when called; if SP successful, contains data table on return</param>
	''' <param name="ConnStr">Db connection string</param>
	''' <param name="MaxRetryCount">Maximum number of times to attempt to call the stored procedure</param>
	''' <returns>Result code returned by SP; -1 if unable to execute SP</returns>
	''' <remarks></remarks>
	Public Function ExecuteSP(ByRef SpCmd As SqlCommand, ByRef OutTable As DataTable, ByVal ConnStr As String, ByVal MaxRetryCount As Integer) As Integer

		Dim ResCode As Integer = -9999	'If this value is in error msg, then exception occurred before ResCode was set
		Dim ErrMsg As String
		Dim MyTimer As New System.Diagnostics.Stopwatch
		Dim RetryCount As Integer = MaxRetryCount
		Dim blnDeadlockOccurred As Boolean

		Dim intTimeoutSeconds As Integer

		If RetryCount < 1 Then
			RetryCount = 1
		End If

		If Not Int32.TryParse(m_MgrParams.GetParam("cmdtimeout"), intTimeoutSeconds) Then
			intTimeoutSeconds = 30
		End If

		If intTimeoutSeconds = 0 Then
			intTimeoutSeconds = 30
		ElseIf intTimeoutSeconds < 10 Then
			intTimeoutSeconds = 10
		End If

		m_ErrorList.Clear()
		While RetryCount > 0	'Multiple retry loop for handling SP execution failures
			blnDeadlockOccurred = False
			Try
				Using Cn As SqlConnection = New SqlConnection(ConnStr)
					AddHandler Cn.InfoMessage, New SqlInfoMessageEventHandler(AddressOf OnInfoMessage)
					Using Da As SqlDataAdapter = New SqlDataAdapter(), Ds As DataSet = New DataSet
						'NOTE: The connection has to be added here because it didn't exist at the time the command object was created
						SpCmd.Connection = Cn
						'Change command timeout from 30 second default in attempt to reduce SP execution timeout errors
						SpCmd.CommandTimeout = intTimeoutSeconds
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
				ErrMsg = "clsDBTask.ExecuteSP(), exception filling data adapter for " & SpCmd.CommandText & ", " & ex.Message
				ErrMsg &= ". ResCode = " & ResCode.ToString & ". Retry count = " & RetryCount.ToString
				ErrMsg &= "; " & clsGlobal.GetExceptionStackTrace(ex)
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, ErrMsg)
				If ex.Message.StartsWith("Could not find stored procedure " & SpCmd.CommandText) Then
					Exit While
				ElseIf ex.Message.Contains("was deadlocked") Then
					blnDeadlockOccurred = True
				End If
			Finally
				If m_DebugLevel > 1 Then
					ErrMsg = "SP execution time: " & (CDbl(MyTimer.ElapsedMilliseconds) / 1000.0#).ToString("##0.000") & " seconds "
					ErrMsg &= "for SP " & SpCmd.CommandText
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, ErrMsg)
				End If
				MyTimer.Reset()
			End Try

			If RetryCount > 0 Then
				System.Threading.Thread.Sleep(20000)	'Wait 20 seconds before retrying
			End If
		End While

		If RetryCount < 1 Then
			'Too many retries, log and return error
			ErrMsg = "Excessive retries"
			If blnDeadlockOccurred Then
				ErrMsg &= " (including deadlock)"
			End If
			ErrMsg &= " executing SP " & SpCmd.CommandText
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, ErrMsg)
			If blnDeadlockOccurred Then
				Return RET_VAL_DEADLOCK
			Else
				Return RET_VAL_EXCESSIVE_RETRIES
			End If
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
			MyMsg &= System.Environment.NewLine & "Name= " & MyParam.ParameterName & ControlChars.Tab & ", Value= " & clsGlobal.DbCStr(MyParam.Value)
		Next

		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Parameter list:" & MyMsg)

	End Sub
#End Region

End Class

