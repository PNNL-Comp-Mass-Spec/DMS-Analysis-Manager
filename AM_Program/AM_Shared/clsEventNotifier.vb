Public MustInherit Class clsEventNotifier

#Region "Events and Event Handlers"

    Public Event ErrorEvent(strMessage As String)
    Public Event WarningEvent(strMessage As String)

    Public Event ProgressUpdate(progressMessage As String, percentComplete As Integer)

    ''' <summary>
    ''' Progress udpate
    ''' </summary>
    ''' <param name="progressMessage">Progress message</param>
    ''' <param name="percentComplete">Value between 0 and 100</param>
    Protected Sub OnProgressUpdate(progressMessage As String, percentComplete As Integer)
        RaiseEvent ProgressUpdate(progressMessage, percentComplete)
    End Sub

    ''' <summary>
    ''' Report an error
    ''' </summary>
    ''' <param name="errorMessageNew"></param>
    Protected Sub OnErrorEvent(errorMessageNew As String)
        RaiseEvent ErrorEvent(errorMessageNew)
    End Sub

    ''' <summary>
    ''' report a warning
    ''' </summary>
    ''' <param name="warningMessageNew"></param>
    Protected Sub OnWarningEvent(warningMessageNew As String)
        RaiseEvent WarningEvent(warningMessageNew)
    End Sub

#End Region

End Class
