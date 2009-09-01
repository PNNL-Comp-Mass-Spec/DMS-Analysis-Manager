'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2009, Battelle Memorial Institute
' Created 02/09/2009
'
'*********************************************************************************************************

Option Strict On

Namespace AnalysisManagerBase

    Public Class clsMemoryUsageLogger

#Region "Module variables"

        'Status file name and location
        Private m_LogFolderPath As String

        ' The minimum interval between appending a new memory usage entry to the log
        Private m_MinimumMemoryUsageLogIntervalMinutes As Single = 1

        'Used to determine the amount of free memory
        Private m_PerfCounterFreeMemory As System.Diagnostics.PerformanceCounter
        Private m_PerfCounterPoolPagedBytes As System.Diagnostics.PerformanceCounter
        Private m_PerfCounterPoolNonpagedBytes As System.Diagnostics.PerformanceCounter

        Private m_PerfCountersIntitialized As Boolean = False
#End Region

#Region "Properties"
        
        Public ReadOnly Property LogFolderPath() As String
            Get
                Return m_LogFolderPath
            End Get
        End Property

        Public Property MinimumLogIntervalMinutes() As Single
            Get
                Return m_MinimumMemoryUsageLogIntervalMinutes
            End Get
            Set(ByVal value As Single)
                If value < 0 Then value = 0
                m_MinimumMemoryUsageLogIntervalMinutes = value
            End Set
        End Property
#End Region

#Region "Methods"

        ''' <summary>
        ''' Constructor
        ''' </summary>
        ''' <param name="LogFolderPath">Folder in which to write the memory log file(s) </param>
        ''' <remarks></remarks>
        Public Sub New(ByVal LogFolderPath As String, ByVal sngMinimumLogIntervalMinutes As Single)
            If LogFolderPath Is Nothing Then LogFolderPath = String.Empty
            m_LogFolderPath = LogFolderPath

            Me.MinimumLogIntervalMinutes = sngMinimumLogIntervalMinutes
        End Sub

        ''' <summary>
        ''' Returns the amount of free memory on the current machine
        ''' </summary>
        ''' <returns>Free memory, in MB</returns>
        ''' <remarks></remarks>
        Protected Function GetFreeMemoryMB() As Single
            Try
                If m_PerfCounterFreeMemory Is Nothing Then
                    Return 0
                Else
                    Return m_PerfCounterFreeMemory.NextValue()
                End If
            Catch ex As Exception
                Return -1
            End Try
        End Function

        ''' <summary>
        ''' Returns the amount of pool nonpaged memory on the current machine
        ''' </summary>
        ''' <returns>Pool Nonpaged memory, in MB</returns>
        ''' <remarks></remarks>
        Protected Function GetPoolNonpagedMemory() As Single
            Try
                If m_PerfCounterPoolNonpagedBytes Is Nothing Then
                    Return 0
                Else
                    Return CSng(m_PerfCounterPoolNonpagedBytes.NextValue() / 1024.0 / 1024)
                End If
            Catch ex As Exception
                Return -1
            End Try           
        End Function

        ''' <summary>
        ''' Returns the amount of pool paged memory on the current machine
        ''' </summary>
        ''' <returns>Pool Paged memory, in MB</returns>
        ''' <remarks></remarks>
        Protected Function GetPoolPagedMemory() As Single
            Try
                If m_PerfCounterPoolPagedBytes Is Nothing Then
                    Return 0
                Else
                    Return CSng(m_PerfCounterPoolPagedBytes.NextValue() / 1024.0 / 1024)
                End If
            Catch ex As Exception
                Return -1
            End Try
        End Function

        ''' <summary>
        ''' Returns the amount of memory that the currently running process is using
        ''' </summary>
        ''' <returns>Memory usage, in MB</returns>
        ''' <remarks></remarks>
        Public Shared Function GetProcessMemoryUsageMB() As Single
            Try
                ' Obtain a handle to the current process
                Dim objProcess As System.Diagnostics.Process
                objProcess = System.Diagnostics.Process.GetCurrentProcess()

                ' The WorkingSet is the total physical memory usage 
                Return CSng(objProcess.WorkingSet64 / 1024.0 / 1024)
            Catch ex As Exception
                Return 0
            End Try

        End Function

        ''' <summary>
        ''' Initializes the performance counters
        ''' </summary>
        ''' <returns>Any errors that occur; empty string if no errors</returns>
        ''' <remarks></remarks>
        Public Function InitializePerfCounters() As String
            Dim msgErrors As String = String.Empty

            Try
                m_PerfCounterFreeMemory = New System.Diagnostics.PerformanceCounter("Memory", "Available MBytes")
                m_PerfCounterFreeMemory.ReadOnly = True
            Catch ex As Exception
                If msgErrors.Length > 0 Then msgErrors &= "; "
                msgErrors &= "Error instantiating the Memory: 'Available MBytes' performance counter: " & ex.Message
            End Try

            Try
                m_PerfCounterPoolPagedBytes = New System.Diagnostics.PerformanceCounter("Memory", "Pool Paged Bytes")
                m_PerfCounterPoolPagedBytes.ReadOnly = True
            Catch ex As Exception
                If msgErrors.Length > 0 Then msgErrors &= "; "
                msgErrors &= "Error instantiating the Memory: 'Pool Paged Bytes' performance counter: " & ex.Message
            End Try

            Try
                m_PerfCounterPoolNonpagedBytes = New System.Diagnostics.PerformanceCounter("Memory", "Pool NonPaged Bytes")
                m_PerfCounterPoolNonpagedBytes.ReadOnly = True
            Catch ex As Exception
                If msgErrors.Length > 0 Then msgErrors &= "; "
                msgErrors &= "Error instantiating the Memory: 'Pool NonPaged Bytes' performance counter: " & ex.Message
            End Try

            m_PerfCountersIntitialized = True

            Return msgErrors

        End Function

        Public Sub WriteMemoryUsageLogEntry()
            Static dtLastWriteTime As System.DateTime = System.DateTime.MinValue

            'Writes a status file for external monitor to read
            Dim swOutFile As System.IO.StreamWriter

            Dim strLogFileName As String
            Dim strLogFilePath As String
            Dim blnWriteHeader As Boolean = False

            Try
                If System.DateTime.Now.Subtract(dtLastWriteTime).TotalMinutes < m_MinimumMemoryUsageLogIntervalMinutes Then
                    ' Not enough time has elapsed since the last write; exit sub
                    Exit Sub
                End If
                dtLastWriteTime = System.DateTime.Now

                ' We're creating a new log file each month
                strLogFileName = "MemoryUsageLog_" & System.DateTime.Now.ToString("yyyy-MM") & ".txt"

                If Not m_LogFolderPath Is Nothing AndAlso m_LogFolderPath.Length > 0 Then
                    strLogFilePath = System.IO.Path.Combine(m_LogFolderPath, strLogFileName)
                Else
                    strLogFilePath = String.Copy(strLogFileName)
                End If

                If Not System.IO.File.Exists(strLogFilePath) Then
                    blnWriteHeader = True
                End If

                swOutFile = New System.IO.StreamWriter(New System.IO.FileStream(strLogFilePath, IO.FileMode.Append, IO.FileAccess.Write, IO.FileShare.Read))

                If Not m_PerfCountersIntitialized Then
                    InitializePerfCounters()
                    m_PerfCountersIntitialized = True
                End If

                If blnWriteHeader Then
                    swOutFile.WriteLine("Date" & ControlChars.Tab & _
                                        "Time" & ControlChars.Tab & _
                                        "ProcessMemoryUsage_MB" & ControlChars.Tab & _
                                        "FreeMemory_MB" & ControlChars.Tab & _
                                        "PoolPaged_MB" & ControlChars.Tab & _
                                        "PoolNonpaged_MB" & ControlChars.Tab)
                End If

                swOutFile.WriteLine(System.DateTime.Now.ToString("yyyy-MM-dd") & ControlChars.Tab & _
                                    System.DateTime.Now.ToString("hh:mm:ss tt") & ControlChars.Tab & _
                                    GetProcessMemoryUsageMB.ToString("0.0") & ControlChars.Tab & _
                                    GetFreeMemoryMB.ToString("0.0") & ControlChars.Tab & _
                                    GetPoolPagedMemory.ToString("0.0") & ControlChars.Tab & _
                                    GetPoolNonpagedMemory.ToString("0.0") & ControlChars.Tab)

                If Not swOutFile Is Nothing Then
                    swOutFile.Close()
                End If

            Catch
                ' Ignore errors here
            End Try

        End Sub
#End Region

    End Class

End Namespace
