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
        m_ConnStr = m_MgrParams.GetParam("ConnectionString")               ' Gigasax.DMS5
        m_BrokerConnStr = m_MgrParams.GetParam("brokerconnectionstring")   ' Gigasax.DMS_Pipeline
        m_DebugLevel = DebugLvl

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

    Protected Function FillParamDictXml(ByVal InpXml As String) As IEnumerable(Of udtParameterInfoType)

        Dim ErrMsg As String

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
            ErrMsg = "clsDBTask.FillParamDict(), exception filling dictionary; " & ex.Message & "; " & clsGlobal.GetExceptionStackTrace(ex)
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, ErrMsg)
            Return Nothing
        End Try

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
            MyMsg &= Environment.NewLine & "Name= " & MyParam.ParameterName & ControlChars.Tab & ", Value= " & clsGlobal.DbCStr(MyParam.Value)
        Next

        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Parameter list:" & MyMsg)

    End Sub
#End Region

#Region "Event Handlers"

    Private Sub m_ExecuteSP_DebugEvent(Message As String) Handles DMSProcedureExecutor.DebugEvent, PipelineDBProcedureExecutor.DebugEvent
        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, Message)
    End Sub

    Private Sub m_ExecuteSP_DBErrorEvent(Message As String) Handles DMSProcedureExecutor.DBErrorEvent, PipelineDBProcedureExecutor.DBErrorEvent
        If Message.Contains("permission was denied") Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, Message)
        Else
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Message)
        End If
    End Sub

#End Region

End Class

