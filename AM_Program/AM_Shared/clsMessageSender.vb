Option Strict On

Imports Apache.NMS

' sends messages to ActiveMQ message broker using NMS client library
Class clsMessageSender
    '    Implements IDisposable

    Private topicName As String = Nothing
    Private brokerUri As String = Nothing
    Private processorName As String = Nothing

    Private connection As IConnection
    Private session As ISession
    Private producer As IMessageProducer

    Private isDisposed As Boolean = False
    Private hasConnection As Boolean = False

    Public Sub New(ByVal brokerUri As String, ByVal topicName As String, ByVal processorName As String)
        Me.topicName = topicName
        Me.brokerUri = brokerUri
        Me.processorName = processorName
    End Sub

    ' send the message using NMS connection objects
    ' If connection does not exist, make it..
    ' If connection objects don't work, erase them and make another set
    Public Sub SendMessage(ByVal message As String)
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

    ' create set of NMS connection objects necessary to talk to the ActiveMQ broker
    Protected Sub CreateConnection()
        If hasConnection Then
            Exit Sub
        End If
        Try
			Dim connectionFactory As IConnectionFactory = New ActiveMQ.ConnectionFactory(Me.brokerUri)
			Me.connection = connectionFactory.CreateConnection()
			Me.connection.RequestTimeout = New System.TimeSpan(0, 0, 15)
			Me.connection.Start()

            Me.session = connection.CreateSession()

			Me.producer = Me.session.CreateProducer(New ActiveMQ.Commands.ActiveMQTopic(Me.topicName))
            Me.producer.Persistent = False
            Me.hasConnection = True
            ' temp debug
            '+ e.ToString()
            '            Console.WriteLine("--- New connection made ---" & Environment.NewLine)
        Catch e As Exception
            ' we couldn't make a viable set of connection objects 
            ' - this has "long day" written all over it,
            ' but we don't have to do anything specific at this point (except eat the exception)

            '+ e.ToString() // temp debug
			Console.WriteLine("=== Error creating Activemq connection ===" & Environment.NewLine & e.Message)
        End Try
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
