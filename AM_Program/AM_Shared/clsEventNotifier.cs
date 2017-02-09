Public MustInherit Class clsEventNotifier

#Region "Events and Event Handlers"

    Public Event ErrorEvent(strMessage As String, ex As Exception)
    Public Event StatusEvent(strMessage As String)
    Public Event WarningEvent(strMessage As String)
    Public Event ProgressUpdate(progressMessage As String, percentComplete As Single)

    ''' <summary>
    ''' Progress udpate
    ''' </summary>
    ''' <param name="progressMessage">Progress message</param>
    ''' <param name="percentComplete">Value between 0 and 100</param>
    Protected Sub OnProgressUpdate(progressMessage As String, percentComplete As Single)
        RaiseEvent ProgressUpdate(progressMessage, percentComplete)
    End Sub

    ''' <summary>
    ''' Report an error
    ''' </summary>
    ''' <param name="strMessage"></param>
    Protected Sub OnErrorEvent(strMessage As String)
        RaiseEvent ErrorEvent(strMessage, Nothing)
    End Sub

    ''' <summary>
    ''' Report an error
    ''' </summary>
    ''' <param name="strMessage"></param>
    ''' <param name="ex">Exception (allowed to be nothing)</param>
    Protected Sub OnErrorEvent(strMessage As String, ex As Exception)
        RaiseEvent ErrorEvent(strMessage, ex)
    End Sub

    ''' <summary>
    ''' Report an error
    ''' </summary>
    ''' <param name="strMessage"></param>
    Protected Sub OnStatusEvent(strMessage As String)
        RaiseEvent StatusEvent(strMessage)
    End Sub

    ''' <summary>
    ''' report a warning
    ''' </summary>
    ''' <param name="strMessage"></param>
    Protected Sub OnWarningEvent(strMessage As String)
        RaiseEvent WarningEvent(strMessage)
    End Sub

#End Region

End Class
