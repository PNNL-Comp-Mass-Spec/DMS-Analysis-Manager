'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2006, Battelle Memorial Institute
' Created 04/30/2007
'
' Last modified 01/16/2008
'*********************************************************************************************************

Option Strict On

Namespace AnalysisManagerBase

    Public Class clsEmergencyLog

        '*********************************************************************************************************
        'Class for logging of problems prior to manager's full logging capability being available
        '*********************************************************************************************************

#Region "Methods"
        ''' <summary>
        ''' Writes a message to a custom event log, which is used if standard log file not available
        ''' </summary>
        ''' <param name="SourceName">Name of source (program) using log</param>
        ''' <param name="LogName">Name of log</param>
        ''' <param name="ErrMsg">Message to write to log</param>
        ''' <remarks></remarks>
        Public Shared Sub WriteToLog(ByVal SourceName As String, ByVal LogName As String, ByVal ErrMsg As String)

            'If custom event log doesn't exist yet, create it
            If Not EventLog.SourceExists(SourceName) Then
                Dim SourceData As EventSourceCreationData = New EventSourceCreationData(SourceName, LogName)
                EventLog.CreateEventSource(SourceData)
            End If

            'Create custom event logging object and write to log
            Dim ELog As New EventLog
            ELog.Log = LogName
            ELog.Source = SourceName
            ELog.MaximumKilobytes = 1024
            ELog.ModifyOverflowPolicy(OverflowAction.OverwriteAsNeeded, 90)
            EventLog.WriteEntry(SourceName, ErrMsg, EventLogEntryType.Error)

        End Sub
#End Region

    End Class

End Namespace