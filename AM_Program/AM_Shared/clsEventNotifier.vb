Public MustInherit Class clsEventNotifier

#Region "Events and Event Handlers"

    Public Event ErrorEvent(ByVal strMessage As String)
    Public Event WarningEvent(ByVal strMessage As String)

    Public Event ProgressUpdate(ByVal progressMessage As String, ByVal percentComplete As Integer)

    Protected Sub OnProgressUpdate(ByVal progressMessage As String, ByVal percentComplete As Integer)
        RaiseEvent ProgressUpdate(progressMessage, percentComplete)
    End Sub

    Protected Sub OnErrorEvent(ByVal errorMessageNew As String)
        RaiseEvent ErrorEvent(errorMessageNew)
    End Sub

    Protected Sub OnWarningEvent(ByVal warningMessageNew As String)
        RaiseEvent WarningEvent(warningMessageNew)
    End Sub

#End Region

End Class
