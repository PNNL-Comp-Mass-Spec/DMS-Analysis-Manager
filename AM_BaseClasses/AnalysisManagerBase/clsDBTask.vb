Imports System.Collections.Specialized
Imports System.Data.SqlClient
Imports PRISM.Logging

Public MustInherit Class clsDBTask

#Region "Member Variables"

  ' access to the logger
  Protected m_logger As ILogger

  ' access to mgr parameters
  Protected m_mgrParams As IMgrParams

  ' DB access
  Protected m_connection_str As String
  Protected m_DBCn As SqlConnection
  Protected m_error_list As New StringCollection()

  ' job status
  Protected m_TaskWasAssigned As Boolean = False

#End Region

  ' constructor
  Public Sub New(ByVal mgrParams As IMgrParams, ByVal logger As ILogger)
    m_mgrParams = mgrParams
    m_logger = logger
    m_connection_str = m_mgrParams.GetParam("DatabaseSettings", "ConnectionString")
  End Sub

  Public ReadOnly Property TaskWasAssigned() As Boolean
    Get
      Return m_TaskWasAssigned
    End Get
  End Property

  'Public MustOverride Function RequestTask() As Boolean

  'Public MustOverride Sub CloseTask(Optional ByVal success As Boolean = True)

  '------[for DB access]-----------------------------------------------------------

  Protected Sub OpenConnection()
    Dim retryCount As Integer = 3
    While retryCount > 0
      Try
        m_DBCn = New SqlConnection(m_connection_str)
        AddHandler m_DBCn.InfoMessage, New SqlInfoMessageEventHandler(AddressOf OnInfoMessage)
        m_DBCn.Open()
        retryCount = 0
      Catch e As SqlException
        retryCount -= 1
        m_DBCn.Close()
        m_logger.PostError("Connection problem: ", e, True)
        System.Threading.Thread.Sleep(300)
      End Try
    End While
  End Sub

  Protected Sub CLoseConnection()
    If Not m_DBCn Is Nothing Then
      m_DBCn.Close()
    End If
  End Sub

  Protected Sub LogErrorEvents()
    If m_error_list.Count > 0 Then
            m_logger.PostEntry("Warning messages were posted to local log", ILogger.logMsgType.logWarning, True)
    End If
    Dim s As String
    For Each s In m_error_list
            m_logger.PostEntry(s, ILogger.logMsgType.logWarning, True)
    Next
  End Sub

  ' event handler for InfoMessage event
  ' errors and warnings sent from the SQL server are caught here
  '
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
      m_error_list.Add(s)
    Next
  End Sub

End Class

