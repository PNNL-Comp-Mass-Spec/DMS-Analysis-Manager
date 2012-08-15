Option Strict On

Imports System
Imports System.Threading
Imports System.Collections.Generic

' delegate that does the eventual posting
Public Delegate Sub MessageSenderDelegate(ByVal message As String)

Class clsMessageQueueLogger
	' actual delegate registers here
	Public Event Sender As MessageSenderDelegate

	' the worker thread that pulls messages off the queue
	' and posts them
	Private worker As Thread

	' synchronization and signalling stuff to coordinate
	' with worker thread
	Private waitHandle As EventWaitHandle = New AutoResetEvent(False)
	Private locker As New Object()

	' local queue that contains messages to be sent
	Private m_statusMessages As New Queue(Of String)()

	Public Sub New()
		worker = New Thread(AddressOf PostalWorker)
		worker.Start()
	End Sub

	' stuff status message onto the local status message queue 
	' to be sent off by the PostalWorker thread
	Public Sub LogStatusMessage(ByVal statusMessage As String)
		SyncLock locker
			m_statusMessages.Enqueue(statusMessage)
		End SyncLock
		waitHandle.[Set]()
	End Sub

	' worker that runs in the worker thread
	' It pulls messages off the queue and posts
	' them via the delegate.  When no more messages
	' it waits until signalled by new message added to queue
	Private Sub PostalWorker()
		While True
			Dim statusMessage As String = Nothing
			SyncLock locker
				If m_statusMessages.Count > 0 Then
					statusMessage = m_statusMessages.Dequeue()
					If statusMessage Is Nothing Then
						Exit Sub
					End If
				End If
			End SyncLock
			If statusMessage IsNot Nothing Then
				' we have work to do
				' use our delegates (if we have any)
				RaiseEvent Sender(statusMessage)
			Else
				waitHandle.WaitOne()
				' No more m_statusMessages - wait for a signal
			End If
		End While
	End Sub

	Public Sub Dispose()
		LogStatusMessage(Nothing)
		' Signal the worker to exit.
		' Wait a maximum of 15 seconds
		If Not worker.Join(15000) Then
			' Still no response, so kill the thread
			worker.Abort()
		Else
			' Wait for the consumer's thread to finish.
			' Release any OS resources.
			waitHandle.Close()
		End If
	End Sub

End Class
