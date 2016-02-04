'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Created 02/09/2009
' Last updated 02/03/2016
'*********************************************************************************************************

Option Strict On

Imports System.IO

Public Class clsMemoryUsageLogger

#Region "Module variables"

    Private Const COL_SEP As Char = ControlChars.Tab

	'Status file name and location
	Private ReadOnly m_LogFolderPath As String

	' The minimum interval between appending a new memory usage entry to the log
	Private m_MinimumMemoryUsageLogIntervalMinutes As Single = 1

	'Used to determine the amount of free memory
	Private m_PerfCounterFreeMemory As PerformanceCounter
	Private m_PerfCounterPoolPagedBytes As PerformanceCounter
	Private m_PerfCounterPoolNonpagedBytes As PerformanceCounter

	Private m_PerfCountersIntitialized As Boolean = False
#End Region

#Region "Properties"

    ''' <summary>
    ''' Output folder for the log file
    ''' </summary>
    ''' <value></value>
    ''' <returns></returns>
    ''' <remarks>If this is an empty string, the log file is created in the working directory</remarks>
	Public ReadOnly Property LogFolderPath() As String
		Get
			Return m_LogFolderPath
		End Get
	End Property

    ''' <summary>
    ''' The minimum interval between appending a new memory usage entry to the log
    ''' </summary>
    ''' <value></value>
    ''' <returns></returns>
    ''' <remarks></remarks>
	Public Property MinimumLogIntervalMinutes() As Single
		Get
			Return m_MinimumMemoryUsageLogIntervalMinutes
		End Get
        Set(value As Single)
            If value < 0 Then value = 0
            m_MinimumMemoryUsageLogIntervalMinutes = value
        End Set
    End Property
#End Region

#Region "Methods"

    ''' <summary>
    ''' Constructor
    ''' </summary>
    ''' <param name="logFolderPath">Folder in which to write the memory log file(s); if this is an empty string, the log file is created in the working directory</param>
    ''' <param name="minLogIntervalMinutes">Minimum log interval, in minutes</param>
    ''' <remarks>
    ''' Use WriteMemoryUsageLogEntry to append an entry to the log file.
    ''' Alternatively use GetMemoryUsageSummary() to retrieve the memory usage as a string</remarks>
    Public Sub New(logFolderPath As String, Optional minLogIntervalMinutes As Single = 5)
        If String.IsNullOrWhiteSpace(logFolderPath) Then
            m_LogFolderPath = String.Empty
        Else
            m_LogFolderPath = logFolderPath
        End If

        Me.MinimumLogIntervalMinutes = minLogIntervalMinutes
    End Sub

    ''' <summary>
    ''' Returns the amount of free memory on the current machine
    ''' </summary>
    ''' <returns>Free memory, in MB</returns>
    ''' <remarks></remarks>
    Public Function GetFreeMemoryMB() As Single
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

    Public Function GetMemoryUsageHeader() As String
        Return "Date" & COL_SEP &
                "Time" & COL_SEP &
                "ProcessMemoryUsage_MB" & COL_SEP &
                "FreeMemory_MB" & COL_SEP &
                "PoolPaged_MB" & COL_SEP &
                "PoolNonpaged_MB"
    End Function

    Public Function GetMemoryUsageSummary() As String

        If Not m_PerfCountersIntitialized Then
            InitializePerfCounters()
        End If

        Dim currentTime = DateTime.Now()

        Return currentTime.ToString("yyyy-MM-dd") & COL_SEP &
               currentTime.ToString("hh:mm:ss tt") & COL_SEP &
               GetProcessMemoryUsageMB.ToString("0.0") & COL_SEP &
               GetFreeMemoryMB.ToString("0.0") & COL_SEP &
               GetPoolPagedMemory.ToString("0.0") & COL_SEP &
               GetPoolNonpagedMemory.ToString("0.0")

    End Function

    ''' <summary>
    ''' Returns the amount of pool nonpaged memory on the current machine
    ''' </summary>
    ''' <returns>Pool Nonpaged memory, in MB</returns>
    ''' <remarks></remarks>
    Public Function GetPoolNonpagedMemory() As Single
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
    Public Function GetPoolPagedMemory() As Single
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
            Dim objProcess As Process
            objProcess = Process.GetCurrentProcess()

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
            m_PerfCounterFreeMemory = New PerformanceCounter("Memory", "Available MBytes")
            m_PerfCounterFreeMemory.ReadOnly = True
        Catch ex As Exception
            If msgErrors.Length > 0 Then msgErrors &= "; "
            msgErrors &= "Error instantiating the Memory: 'Available MBytes' performance counter: " & ex.Message
        End Try

        Try
            m_PerfCounterPoolPagedBytes = New PerformanceCounter("Memory", "Pool Paged Bytes")
            m_PerfCounterPoolPagedBytes.ReadOnly = True
        Catch ex As Exception
            If msgErrors.Length > 0 Then msgErrors &= "; "
            msgErrors &= "Error instantiating the Memory: 'Pool Paged Bytes' performance counter: " & ex.Message
        End Try

        Try
            m_PerfCounterPoolNonpagedBytes = New PerformanceCounter("Memory", "Pool NonPaged Bytes")
            m_PerfCounterPoolNonpagedBytes.ReadOnly = True
        Catch ex As Exception
            If msgErrors.Length > 0 Then msgErrors &= "; "
            msgErrors &= "Error instantiating the Memory: 'Pool NonPaged Bytes' performance counter: " & ex.Message
        End Try

        m_PerfCountersIntitialized = True

        Return msgErrors

    End Function

    ''' <summary>
    ''' Writes a status file tracking memory usage
    ''' </summary>
    ''' <remarks></remarks>
    Public Sub WriteMemoryUsageLogEntry()
        Static dtLastWriteTime As DateTime = DateTime.UtcNow.Subtract(New TimeSpan(1, 0, 0))

        Dim strLogFileName As String
        Dim strLogFilePath As String

        Try
            If DateTime.UtcNow.Subtract(dtLastWriteTime).TotalMinutes < m_MinimumMemoryUsageLogIntervalMinutes Then
                ' Not enough time has elapsed since the last write; exit sub
                Exit Sub
            End If
            dtLastWriteTime = DateTime.UtcNow

            ' We're creating a new log file each month
            strLogFileName = "MemoryUsageLog_" & DateTime.Now.ToString("yyyy-MM") & ".txt"

            If Not String.IsNullOrWhiteSpace(m_LogFolderPath) Then
                strLogFilePath = Path.Combine(m_LogFolderPath, strLogFileName)
            Else
                strLogFilePath = String.Copy(strLogFileName)
            End If

            Dim blnWriteHeader = Not File.Exists(strLogFilePath)

            Using swOutFile = New StreamWriter(New FileStream(strLogFilePath, FileMode.Append, FileAccess.Write, FileShare.Read))

                If blnWriteHeader Then
                    GetMemoryUsageHeader()
                End If

                swOutFile.WriteLine(GetMemoryUsageSummary())

            End Using


        Catch
            ' Ignore errors here
        End Try

    End Sub

#End Region

End Class
