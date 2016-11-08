'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2007, Battelle Memorial Institute
' Created 10/26/2007
'
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
    Public Const RET_VAL_EXCESSIVE_RETRIES As Integer = -5           ' Timeout expired
    Public Const RET_VAL_DEADLOCK As Integer = -4                    ' Transaction (Process ID 143) was deadlocked on lock resources with another process and has been chosen as the deadlock victim
    Public Const RET_VAL_TASK_NOT_AVAILABLE As Integer = 53000
    Public Const DEFAULT_SP_RETRY_COUNT As Integer = 3
#End Region

#Region "Module variables"

    'Manager parameters
    Protected m_MgrParams As IMgrParams
    Protected m_ConnStr As String
    Protected m_BrokerConnStr As String

    ''' <summary>
    ''' Debug level
    ''' Values from 0 (minimum output) to 5 (max detail)
    ''' </summary>
    Protected m_DebugLevel As Integer

    'Job status
    Protected m_TaskWasAssigned As Boolean = False

    Protected m_Xml_Text As String

    Public WithEvents DMSProcedureExecutor As PRISM.DataBase.clsExecuteDatabaseSP
    Public WithEvents PipelineDBProcedureExecutor As PRISM.DataBase.clsExecuteDatabaseSP

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
        Set(value As Integer)
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
    Protected Sub New(mgrParams As IMgrParams, debugLvl As Integer)

        m_MgrParams = mgrParams
        m_ConnStr = m_MgrParams.GetParam("ConnectionString")               ' Gigasax.DMS5
        m_BrokerConnStr = m_MgrParams.GetParam("brokerconnectionstring")   ' Gigasax.DMS_Pipeline
        m_DebugLevel = debugLvl

        DMSProcedureExecutor = New PRISM.DataBase.clsExecuteDatabaseSP(m_ConnStr)
        PipelineDBProcedureExecutor = New PRISM.DataBase.clsExecuteDatabaseSP(m_BrokerConnStr)

        If m_DebugLevel > 1 Then
            DMSProcedureExecutor.DebugMessagesEnabled = True
            PipelineDBProcedureExecutor.DebugMessagesEnabled = True
        End If

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
    Public MustOverride Sub CloseTask(CloseOut As IJobParams.CloseOutType, CompMsg As String)

    ''' <summary>
    ''' Closes out a task (includes EvalCode and EvalMessgae)
    ''' </summary>
    ''' <param name="CloseOut"></param>
    ''' <param name="CompMsg"></param>
    ''' <param name="EvalCode">Evaluation code (0 if no special evaulation message)</param>
    ''' <param name="EvalMessage">Evaluation message ("" if no special message)</param>
    ''' <remarks></remarks>
    Public MustOverride Sub CloseTask(CloseOut As IJobParams.CloseOutType, CompMsg As String, EvalCode As Integer, EvalMessage As String)

    Protected Function FillParamDictXml(InpXml As String) As IEnumerable(Of udtParameterInfoType)

        Try
            ' Read XML string into XPathDocument object 
            ' and set up navigation objects to traverse it

            Dim xReader As XmlReader = New XmlTextReader(New StringReader(InpXml))
            Dim xdoc As New XPathDocument(xReader)
            Dim xpn As XPathNavigator = xdoc.CreateNavigator()
            Dim nodes As XPathNodeIterator = xpn.Select("//item")

            Dim dctParameters As New List(Of udtParameterInfoType)
            Dim udtParamInfo As udtParameterInfoType

            ' Traverse the parsed XML document and extract the key and value for each item
            While nodes.MoveNext()
                ' Extract section, key, and value from XML element and append entry to dctParameterInfo
                udtParamInfo.ParamName = nodes.Current.GetAttribute("key", "")
                udtParamInfo.Value = nodes.Current.GetAttribute("value", "")

                ' Extract the section name for the current item and dump it to output
                Dim nav2 As XPathNavigator = nodes.Current.Clone
                nav2.MoveToParent()
                udtParamInfo.Section = nav2.GetAttribute("name", "")

                dctParameters.Add(udtParamInfo)

            End While

            Return dctParameters

        Catch ex As Exception
            LogError("clsDBTask.FillParamDict(), exception filling dictionary", ex)
            Return Nothing
        End Try

    End Function

    ''' <summary>
    ''' Log an error message
    ''' </summary>
    ''' <param name="errorMessage">Error message</param>
    Protected Sub LogError(errorMessage As String)
        Console.ForegroundColor = ConsoleColor.Red
        Console.WriteLine(errorMessage)
        Console.ResetColor()
        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, errorMessage)
    End Sub

    ''' <summary>
    ''' Log an error message and exception
    ''' </summary>
    ''' <param name="errorMessage">Error message</param>
    ''' <param name="ex">Exception to log</param>
    Protected Sub LogError(errorMessage As String, ex As Exception)
        ReportStatus(errorMessage, ex)
    End Sub

    ''' <summary>
    ''' Debugging routine for printing SP calling params
    ''' </summary>
    ''' <param name="InpCmd">SQL command object containing params</param>
    ''' <remarks></remarks>
    Protected Sub PrintCommandParams(InpCmd As SqlCommand)

        'Verify there really are command paramters
        If InpCmd Is Nothing Then Exit Sub
        If InpCmd.Parameters.Count < 1 Then Exit Sub

        Dim MyMsg As String = ""

        For Each MyParam As SqlParameter In InpCmd.Parameters
            MyMsg &= Environment.NewLine & "Name= " & MyParam.ParameterName & ControlChars.Tab & ", Value= " & clsGlobal.DbCStr(MyParam.Value)
        Next

        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Parameter list:" & MyMsg)

    End Sub

    ''' <summary>
    ''' Shows information about an exception at the console and in the log file
    ''' </summary>
    ''' <param name="errorMessage">Error message (do not include ex.message)</param>
    ''' <param name="ex">Exception</param>
    Protected Sub ReportStatus(errorMessage As String, ex As Exception)
        Dim formattedError As String
        If errorMessage.EndsWith(ex.Message) Then
            formattedError = errorMessage
        Else
            formattedError = errorMessage & ": " & ex.Message
        End If
        Console.ForegroundColor = ConsoleColor.Red
        Console.WriteLine(formattedError)
        Console.ForegroundColor = ConsoleColor.Cyan
        Console.WriteLine(clsGlobal.GetExceptionStackTrace(ex, True))
        Console.ResetColor()
        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, formattedError, ex)
    End Sub

    ''' <summary>
    ''' Show a status message at the console and optionally include in the log file
    ''' </summary>
    ''' <param name="statusMessage">Status message</param>
    ''' <param name="logFileDebugLevel">
    ''' Log level for whether to log to disk: 
    ''' 0 to always log
    ''' 1 to log if m_DebugLevel is >= 1
    ''' 2 to log if m_DebugLevel is >= 2
    ''' 10 to not log to disk
    ''' </param>
    ''' <param name="isError">True if this is an error</param>
    Protected Sub ReportStatus(statusMessage As String, Optional logFileDebugLevel As Integer = 0, Optional isError As Boolean = False)
        If isError Then
            Console.ForegroundColor = ConsoleColor.Red
            Console.WriteLine(statusMessage)
            Console.ResetColor()
        Else
            Console.WriteLine(statusMessage)
        End If
        If logFileDebugLevel < 10 AndAlso (logFileDebugLevel = 0 OrElse logFileDebugLevel <= m_DebugLevel) Then
            If isError Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, statusMessage)
            Else
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, statusMessage)
            End If
        End If
    End Sub

#End Region

#Region "Event Handlers"

    Private Sub m_ExecuteSP_DebugEvent(message As String) Handles DMSProcedureExecutor.DebugEvent, PipelineDBProcedureExecutor.DebugEvent
        ReportStatus(message, clsLogTools.LogLevels.DEBUG)
    End Sub

    Private Sub m_ExecuteSP_DBErrorEvent(message As String) Handles DMSProcedureExecutor.DBErrorEvent, PipelineDBProcedureExecutor.DBErrorEvent
        If Message.Contains("permission was denied") Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, message)
        End If
        LogError(message)
    End Sub

#End Region

End Class

