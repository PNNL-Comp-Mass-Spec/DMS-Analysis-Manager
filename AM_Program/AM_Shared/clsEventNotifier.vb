Public MustInherit Class clsEventNotifier

#Region "Events and Event Handlers"

    Public Event ErrorEvent(strMessage As String)
    Public Event WarningEvent(strMessage As String)

    Public Event ProgressUpdate(progressMessage As String, percentComplete As Integer)

    Protected Sub OnProgressUpdate(progressMessage As String, percentComplete As Integer)
        RaiseEvent ProgressUpdate(progressMessage, percentComplete)
    End Sub

    Protected Sub OnErrorEvent(errorMessageNew As String)
        RaiseEvent ErrorEvent(errorMessageNew)
    End Sub

    Protected Sub OnWarningEvent(warningMessageNew As String)
        RaiseEvent WarningEvent(warningMessageNew)
    End Sub

#End Region

End Class
