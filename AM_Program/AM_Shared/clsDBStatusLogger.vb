'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2009, Battelle Memorial Institute
' Created 02/09/2009
'
'*********************************************************************************************************

Option Strict On

Public Class clsDBStatusLogger

#Region "Structures"

	Public Structure udtStatusInfoType
		Public MgrName As String
		Public MgrStatus As IStatusFile.EnumMgrStatus
		Public LastUpdate As DateTime
        Public LastStartTime As DateTime

        ''' <summary>
        ''' Overall CPU utilization of all threads
        ''' </summary>
        ''' <remarks></remarks>
        Public CPUUtilization As Single

        ''' <summary>
        ''' System-wide free memory
        ''' </summary>
        ''' <remarks></remarks>
        Public FreeMemoryMB As Single

        ''' <summary>
        ''' Return the ProcessID of the Analysis manager
        ''' </summary>
        ''' <remarks></remarks>
        Public ProcessID As Integer

        ''' <summary>
        ''' ProcessID of an externally spawned process
        ''' </summary>
        ''' <remarks>0 if no external process running</remarks>
        Public ProgRunnerProcessID As Integer

        ''' <summary>
        ''' Number of cores in use by an externally spawned process
        ''' </summary>
        ''' <remarks></remarks>
        Public ProgRunnerCoreUsage As Single

		Public MostRecentErrorMessage As String
		Public Task As udtTaskInfoType
	End Structure

	Public Structure udtTaskInfoType
		Public Tool As String
		Public Status As IStatusFile.EnumTaskStatus
		Public DurationHours As Single		 ' Task duration, in hours
		Public Progress As Single		' Percent complete, value between 0 and 100
		Public CurrentOperation As String
		Public TaskDetails As udtTaskDetailsType
	End Structure

	Public Structure udtTaskDetailsType
		Public Status As IStatusFile.EnumTaskStatusDetail
		Public Job As Integer
		Public JobStep As Integer
		Public Dataset As String
		Public MostRecentLogMessage As String
		Public MostRecentJobInfo As String
		Public SpectrumCount As Integer
	End Structure

#End Region

#Region "Module variables"

    ''' <summary>
    ''' Stored procedure that could be used to report manager status; typically not used
    ''' </summary>
    ''' <remarks>This stored procedure is valid, but the primary way that we track status is when WriteStatusFile calls LogStatusToMessageQueue</remarks>
	Private Const SP_NAME_UPDATE_MANAGER_STATUS As String = "UpdateManagerAndTaskStatus"

	'Status file name and location
	Private ReadOnly m_DBConnectionString As String

	' The minimum interval between updating the manager status in the database
	Private m_DBStatusUpdateIntervalMinutes As Single = 1

#End Region

#Region "Properties"

	Public ReadOnly Property DBConnectionString() As String
		Get
			Return m_DBConnectionString
		End Get
	End Property

	Public Property DBStatusUpdateIntervalMinutes() As Single
		Get
			Return m_DBStatusUpdateIntervalMinutes
		End Get
        Set(value As Single)
            If value < 0 Then value = 0
            m_DBStatusUpdateIntervalMinutes = value
        End Set
    End Property
#End Region

#Region "Methods"

    ''' <summary>
    ''' Constructor
    ''' </summary>
    ''' <param name="strDBConnectionString">Database connection string</param>
    ''' <param name="sngDBStatusUpdateIntervalMinutes">Minimum interval between updating the manager status in the database</param>
    ''' <remarks></remarks>
    Public Sub New(strDBConnectionString As String, sngDBStatusUpdateIntervalMinutes As Single)
        If strDBConnectionString Is Nothing Then strDBConnectionString = String.Empty
        m_DBConnectionString = strDBConnectionString
        m_DBStatusUpdateIntervalMinutes = sngDBStatusUpdateIntervalMinutes
    End Sub

    ''' <summary>
    ''' Send status information to the database
    ''' </summary>
    ''' <param name="udtStatusInfo"></param>
    ''' <param name="blnForceLogToDB"></param>
    ''' <remarks>This function is valid, but the primary way that we track status is when WriteStatusFile calls LogStatusToMessageQueue</remarks>
    Public Sub LogStatus(udtStatusInfo As udtStatusInfoType, blnForceLogToDB As Boolean)

        Static dtLastWriteTime As DateTime = Date.UtcNow.Subtract(New TimeSpan(1, 0, 0))

        Dim MyConnection As SqlClient.SqlConnection
        Dim myCmd As New SqlClient.SqlCommand

        Try
            If String.IsNullOrEmpty(m_DBConnectionString) Then
                ' Connection string not defined; unable to continue
                Exit Sub
            End If

            If Not blnForceLogToDB AndAlso Date.UtcNow.Subtract(dtLastWriteTime).TotalMinutes < m_DBStatusUpdateIntervalMinutes Then
                ' Not enough time has elapsed since the last write; exit sub
                Exit Sub
            End If
            dtLastWriteTime = Date.UtcNow


            MyConnection = New SqlClient.SqlConnection(m_DBConnectionString)
            MyConnection.Open()

            'Set up the command object prior to SP execution
            With myCmd

                .CommandType = CommandType.StoredProcedure
                .CommandText = SP_NAME_UPDATE_MANAGER_STATUS
                .Connection = MyConnection

                .Parameters.Add(New SqlClient.SqlParameter("@Return", SqlDbType.Int))
                .Parameters.Item("@Return").Direction = ParameterDirection.ReturnValue


                ' Manager items
                AddSPParameter(.Parameters, "@MgrName", udtStatusInfo.MgrName, 128)
                AddSPParameter(.Parameters, "@MgrStatusCode", udtStatusInfo.MgrStatus)

                AddSPParameter(.Parameters, "@LastUpdate", udtStatusInfo.LastUpdate.ToLocalTime())
                AddSPParameter(.Parameters, "@LastStartTime", udtStatusInfo.LastStartTime.ToLocalTime())
                AddSPParameter(.Parameters, "@CPUUtilization", udtStatusInfo.CPUUtilization)
                AddSPParameter(.Parameters, "@FreeMemoryMB", udtStatusInfo.FreeMemoryMB)
                AddSPParameter(.Parameters, "@ProcessID", udtStatusInfo.ProcessID)
                AddSPParameter(.Parameters, "@ProgRunnerProcessID", udtStatusInfo.ProgRunnerProcessID)
                AddSPParameter(.Parameters, "@ProgRunnerCoreUsage", udtStatusInfo.ProgRunnerCoreUsage)

                AddSPParameter(.Parameters, "@MostRecentErrorMessage", udtStatusInfo.MostRecentErrorMessage, 1024)

                ' Task items
                AddSPParameter(.Parameters, "@StepTool", udtStatusInfo.Task.Tool, 128)
                AddSPParameter(.Parameters, "@TaskStatusCode", udtStatusInfo.Task.Status)
                AddSPParameter(.Parameters, "@DurationHours", udtStatusInfo.Task.DurationHours)
                AddSPParameter(.Parameters, "@Progress", udtStatusInfo.Task.Progress)
                AddSPParameter(.Parameters, "@CurrentOperation", udtStatusInfo.Task.CurrentOperation, 256)

                ' Task detail items
                AddSPParameter(.Parameters, "@TaskDetailStatusCode", udtStatusInfo.Task.TaskDetails.Status)
                AddSPParameter(.Parameters, "@Job", udtStatusInfo.Task.TaskDetails.Job)
                AddSPParameter(.Parameters, "@JobStep", udtStatusInfo.Task.TaskDetails.JobStep)
                AddSPParameter(.Parameters, "@Dataset", udtStatusInfo.Task.TaskDetails.Dataset, 256)
                AddSPParameter(.Parameters, "@MostRecentLogMessage", udtStatusInfo.Task.TaskDetails.MostRecentLogMessage, 1024)
                AddSPParameter(.Parameters, "@MostRecentJobInfo", udtStatusInfo.Task.TaskDetails.MostRecentJobInfo, 256)
                AddSPParameter(.Parameters, "@SpectrumCount", udtStatusInfo.Task.TaskDetails.SpectrumCount)

                AddSPParameterOutput(.Parameters, "@message", String.Empty, 512)

            End With

            'Execute the SP
            myCmd.ExecuteNonQuery()

        Catch ex As Exception
            ' Ignore errors here
            Console.WriteLine("Error in clsDBStatusLogger.LogStatus: " & ex.Message)
        End Try

    End Sub

    Private Sub AddSPParameter(ByRef objParameters As SqlClient.SqlParameterCollection, strParamName As String, strValue As String, intVarCharLength As Integer)
        ' Make sure the parameter starts with an @ sign
        If Not strParamName.StartsWith("@") Then
            strParamName = "@" & strParamName
        End If

        objParameters.Add(New SqlClient.SqlParameter(strParamName, SqlDbType.VarChar, intVarCharLength))
        objParameters.Item(strParamName).Direction = ParameterDirection.Input
        objParameters.Item(strParamName).Value = strValue
    End Sub

    Private Sub AddSPParameter(ByRef objParameters As SqlClient.SqlParameterCollection, strParamName As String, intValue As Integer)
        ' Make sure the parameter starts with an @ sign
        If Not strParamName.StartsWith("@") Then
            strParamName = "@" & strParamName
        End If

        objParameters.Add(New SqlClient.SqlParameter(strParamName, SqlDbType.Int))
        objParameters.Item(strParamName).Direction = ParameterDirection.Input
        objParameters.Item(strParamName).Value = intValue
    End Sub

    Private Sub AddSPParameter(ByRef objParameters As SqlClient.SqlParameterCollection, strParamName As String, dtValue As DateTime)
        ' Make sure the parameter starts with an @ sign
        If Not strParamName.StartsWith("@") Then
            strParamName = "@" & strParamName
        End If

        objParameters.Add(New SqlClient.SqlParameter(strParamName, SqlDbType.DateTime))
        objParameters.Item(strParamName).Direction = ParameterDirection.Input
        objParameters.Item(strParamName).Value = dtValue
    End Sub

    Private Sub AddSPParameter(ByRef objParameters As SqlClient.SqlParameterCollection, strParamName As String, sngValue As Single)
        ' Make sure the parameter starts with an @ sign
        If Not strParamName.StartsWith("@") Then
            strParamName = "@" & strParamName
        End If

        objParameters.Add(New SqlClient.SqlParameter(strParamName, SqlDbType.Real))
        objParameters.Item(strParamName).Direction = ParameterDirection.Input
        objParameters.Item(strParamName).Value = sngValue
    End Sub

    Private Sub AddSPParameterOutput(ByRef objParameters As SqlClient.SqlParameterCollection, strParamName As String, strValue As String, intVarCharLength As Integer)
        ' Make sure the parameter starts with an @ sign
        If Not strParamName.StartsWith("@") Then
            strParamName = "@" & strParamName
        End If

        objParameters.Add(New SqlClient.SqlParameter(strParamName, SqlDbType.VarChar, intVarCharLength))
        objParameters.Item(strParamName).Direction = ParameterDirection.Output
        objParameters.Item(strParamName).Value = strValue
    End Sub

#End Region

End Class


