Option Strict On

Imports Apache.NMS

''' <summary>
''' Sends messages to ActiveMQ message broker using NMS client library
''' </summary>
''' <remarks></remarks>
Class clsMessageSender

    Private ReadOnly topicName As String
    Private ReadOnly brokerUri As String
    Private ReadOnly processorName As String

    Private connection As IConnection
    Private session As ISession
    Private producer As IMessageProducer

    Private isDisposed As Boolean = False
    Private hasConnection As Boolean = False

    Public Sub New(brokerUri As String, topicName As String, processorName As String)
        Me.topicName = topicName
        Me.brokerUri = brokerUri
        Me.processorName = processorName
    End Sub

    ''' <summary>
    '''  Send the message using NMS connection objects
    ''' </summary>
    ''' <param name="message"></param>
    ''' <remarks>
    ''' If connection does not exist, make it
    ''' If connection objects don't work, erase them and make another set
    ''' </remarks>
    Public Sub SendMessage(message As String)
        If Me.isDisposed Then
            Exit Sub
        End If
        If Not hasConnection Then
            CreateConnection()
        End If
        If hasConnection Then
            Try
                Dim textMessage As ITextMessage = Me.session.CreateTextMessage(message)
                textMessage.Properties.SetString("ProcessorName", Me.processorName)
                Me.producer.Send(textMessage)
            Catch e As Exception
                ' something went wrong trying to send message,
                ' get rid of current set of connection object - they'll never work again anyway
                DestroyConnection()
                ' temp debug
                '+ e.Message
                '                Console.WriteLine("=== Error sending ===" & Environment.NewLine)
            End Try
        End If
    End Sub

    ''' <summary>
    ''' Create set of NMS connection objects necessary to talk to the ActiveMQ broker
    ''' </summary>
    ''' <param name="retryCount"></param>
    ''' <param name="timeoutSeconds"></param>
    ''' <remarks></remarks>
    Protected Sub CreateConnection(Optional ByVal retryCount As Integer = 2, Optional ByVal timeoutSeconds As Integer = 15)
        If hasConnection Then
            Exit Sub
        End If

        If retryCount < 0 Then
            retryCount = 0
        End If

        Dim retriesRemaining As Integer = retryCount

        If timeoutSeconds < 5 Then
            timeoutSeconds = 5
        End If

        Dim errorList = New List(Of String)()

        While retriesRemaining >= 0
            Try
                Dim connectionFactory As IConnectionFactory = New ActiveMQ.ConnectionFactory(Me.brokerUri)
                Me.connection = connectionFactory.CreateConnection()
                Me.connection.RequestTimeout = New TimeSpan(0, 0, timeoutSeconds)
                Me.connection.Start()

                Me.session = connection.CreateSession()

                Me.producer = Me.session.CreateProducer(New ActiveMQ.Commands.ActiveMQTopic(Me.topicName))
                Me.hasConnection = True

                Return
            Catch ex As Exception
                ' Connection failed
                If Not errorList.Contains(ex.Message) Then
                    errorList.Add(ex.Message)
                End If

                ' Sleep for 3 seconds
                System.Threading.Thread.Sleep(3000)
            End Try

            retriesRemaining -= 1
        End While

        ' If we get here, we never could connect to the message broker

        Dim msg = "Exception creating broker connection"
        If retryCount > 0 Then
            msg += " after " & (retryCount + 1) & " attempts"
        End If

        msg += ": " + String.Join("; ", errorList)
        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg)

        Console.WriteLine("=== Error creating Activemq connection ===" & Environment.NewLine & msg)
   
    End Sub

    Protected Sub DestroyConnection()
        Try
            If hasConnection Then
                If Not Me.producer Is Nothing Then Me.producer.Dispose()
                If Not Me.session Is Nothing Then Me.session.Dispose()
                If Not Me.connection Is Nothing Then Me.connection.Dispose()
                Me.hasConnection = False
            End If
        Catch ex As Exception
            ' Ignore errors here
        End Try
    End Sub

    Public Sub Dispose()
        If Not Me.isDisposed Then
            Me.isDisposed = True
            Try
                Me.DestroyConnection()
            Catch ex As Exception
                ' Ignore errors here
            End Try
        End If
    End Sub
End Class
