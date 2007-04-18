Option Strict On

Imports System.Collections.Specialized
Imports System.Data.SqlClient
Imports PRISM.Logging
Imports AnalysisManagerBase.clsGlobal

Public Class clsAnalysisJob
	Inherits clsDBTask
	Implements IJobParams

	' constructor
	Public Sub New(ByVal mgrParams As IMgrParams, ByVal logger As ILogger)
		MyBase.New(mgrParams, logger)
	End Sub

#Region "Member variables"
	Private m_jobParams As New StringDictionary		' job parameters returned from request
#End Region

	Public Function RequestJob() As Boolean

		'Requests an analysis job. If job is found, fills a string dictionary object with the necessary parameters

		m_connection_str = m_mgrParams.GetParam("DatabaseSettings", "ConnectionString")

		Try
			OpenConnection()
		Catch Err As Exception
			m_logger.PostEntry("clsAnalysisJob.RequestJob(), error opening connection, " & Err.Message, ILogger.logMsgType.logError, True)
			Return False
		End Try

		Dim priority As String
		Try
			m_TaskWasAssigned = RequestAnalysisJob()
		Catch Err As Exception
			m_logger.PostEntry("clsAnalysisJob.RequestJob(), Error running RequestAnalysisJob, " & Err.Message, ILogger.logMsgType.logError, True)
			Return False
		End Try

		Try
			CLoseConnection()
		Catch Err As Exception
			m_logger.PostEntry("clsAnalysisJob.RequestJob(), Error closing connection, " & Err.Message, ILogger.logMsgType.logError, True)
			Return False
		End Try

		If m_TaskWasAssigned Then
			'Get a table containing the associated data for this job
			Dim SqlStr As String = "SELECT * FROM V_RequestAnalysisJobEx5 WHERE JobNum = '" & m_jobParams("jobNum") & "'"
			Dim ResultTable As DataTable = GetJobParamsFromTableWithRetries(SqlStr)
			If ResultTable Is Nothing Then			'There was an error
				m_logger.PostEntry("clsAnalysisJob.RequestJob(), Unable to obtain job data", ILogger.logMsgType.logError, True)
				Return False
			End If
			'Verify exactly 1 row was received
			If ResultTable.Rows.Count <> 1 Then
				m_logger.PostEntry("clsAnalysisJob.RequestJob(), Invalid job data record count: " & ResultTable.Rows.Count.ToString, _
				  ILogger.logMsgType.logError, True)
				Return False
			End If
			'Add job parameters to string dictionary
			Dim ResultRow As DataRow = ResultTable.Rows(0)
			m_jobParams("tool") = CStr(ResultRow(ResultTable.Columns("ToolName")))
			m_jobParams("datasetNum") = CStr(ResultRow(ResultTable.Columns("DatasetNum")))
			m_jobParams("datasetFolderName") = CStr(ResultRow(ResultTable.Columns("DatasetFolderName")))
			m_jobParams("datasetFolderStoragePath") = CStr(ResultRow(ResultTable.Columns("DatasetStoragePath")))			'Samba path in archive
			m_jobParams("transferFolderPath") = CStr(ResultRow(ResultTable.Columns("transferFolderPath")))
			m_jobParams("parmFileName") = CStr(ResultRow(ResultTable.Columns("ParmFileName")))
			m_jobParams("parmFileStoragePath") = CStr(ResultRow(ResultTable.Columns("ParmFileStoragePath")))
			m_jobParams("settingsFileName") = CStr(ResultRow(ResultTable.Columns("SettingsFileName")))
			m_jobParams("settingsFileStoragePath") = CStr(ResultRow(ResultTable.Columns("SettingsFileStoragePath")))
			m_jobParams("legacyFastaFileName") = CStr(ResultRow(ResultTable.Columns("legacyFastaFileName")))
			m_jobParams("instClass") = CStr(ResultRow(ResultTable.Columns("InstClass")))
			m_jobParams("comment") = CStr(ResultRow(ResultTable.Columns("Comment")))
			m_jobParams("RawDataType") = CStr(ResultRow(ResultTable.Columns("RawDataType")))
			m_jobParams("SearchEngineInputFileFormats") = CStr(ResultRow(ResultTable.Columns("SearchEngineInputFileFormats")))
			m_jobParams("OrganismName") = CStr(ResultRow(ResultTable.Columns("OrganismName")))
			m_jobParams("OrgDbReqd") = CStr(ResultRow(ResultTable.Columns("OrgDbReqd")))
			m_jobParams("ProteinCollectionList") = CStr(ResultRow(ResultTable.Columns("ProteinCollectionList")))
			m_jobParams("ProteinOptionsList") = CStr(ResultRow(ResultTable.Columns("ProteinOptions")))
		End If

		Return m_TaskWasAssigned

	End Function

	Public Sub CloseJob(ByVal closeOut As IJobParams.CloseOutType, ByVal resultsFolderName As String, ByVal comment As String)
		If closeOut = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
			FailCount = 0
		Else
			FailCount += 1
		End If
		OpenConnection()
		SetAnalysisJobComplete(GetCompletionCode(closeOut), resultsFolderName, comment)
		CLoseConnection()
	End Sub

	Private Function GetCompletionCode(ByVal closeOut As IJobParams.CloseOutType) As Integer
		Dim code As Integer = 1		  '  0->success, 1->failure, anything else ->no intermediate files
		Select Case closeOut
			Case IJobParams.CloseOutType.CLOSEOUT_SUCCESS
				code = 0
			Case IJobParams.CloseOutType.CLOSEOUT_FAILED
				code = 1
			Case IJobParams.CloseOutType.CLOSEOUT_NO_DTA_FILES
				code = 2
			Case IJobParams.CloseOutType.CLOSEOUT_NO_OUT_FILES
				code = 3
		End Select
		GetCompletionCode = code
	End Function

	Public Function GetParam(ByVal Name As String) As String Implements IJobParams.GetParam
		Return m_jobParams(Name)
	End Function

	Private Function RequestAnalysisJob() As Boolean

		Dim sc As SqlCommand
		Dim Outcome As Boolean = False

		Try
			m_error_list.Clear()

			'Create the command object
			sc = New SqlCommand("RequestAnalysisJob", m_DBCn)
			sc.CommandType = CommandType.StoredProcedure

			'Define parameters for command object
			Dim myParm As SqlParameter

			'Define parameter for stored procedure's return value
			myParm = sc.Parameters.Add("@Return", SqlDbType.Int)
			myParm.Direction = ParameterDirection.ReturnValue

			'Define parameters for the stored procedure's arguments
			myParm = sc.Parameters.Add("@processorName", SqlDbType.VarChar, 64)
			myParm.Direction = ParameterDirection.Input
			myParm.Value = m_mgrParams.GetParam("ProgramControl", "MachName")

			myParm = sc.Parameters.Add("@jobNum", SqlDbType.VarChar, 32)
			myParm.Direction = ParameterDirection.Output

			'Execute the stored procedure
			sc.ExecuteNonQuery()

			'Get return value
			Dim ret As Integer
			ret = CInt(sc.Parameters("@Return").Value)

			If ret = 0 Then
				'Get job number
				m_jobParams("jobNum") = CStr(sc.Parameters("@jobNum").Value)

				'If we made it this far, we succeeded
				Outcome = True
			End If

		Catch ex As System.Exception
			m_logger.PostError("Error requesting task: ", ex, True)
			Outcome = False
		End Try

		LogErrorEvents()

		Return Outcome

	End Function

	Private Function SetAnalysisJobComplete(ByVal completionCode As Int32, ByVal resultsFolderName As String, ByVal comment As String) As Boolean
		Dim sc As SqlCommand
		Dim Outcome As Boolean = False

		Try
			m_error_list.Clear()

			'Create the command object
			sc = New SqlCommand("SetAnalysisJobComplete", m_DBCn)
			sc.CommandType = CommandType.StoredProcedure

			'Define parameters for command object
			Dim myParm As SqlParameter

			'Define parameter for stored procedure's return value
			myParm = sc.Parameters.Add("@Return", SqlDbType.Int)
			myParm.Direction = ParameterDirection.ReturnValue

			'Define parameters for the stored procedure's arguments
			myParm = sc.Parameters.Add("@jobNum", SqlDbType.VarChar, 32)
			myParm.Direction = ParameterDirection.Input
			myParm.Value = m_jobParams("jobNum")

			myParm = sc.Parameters.Add("@processorName", SqlDbType.VarChar, 64)
			myParm.Direction = ParameterDirection.Input
			myParm.Value = m_mgrParams.GetParam("ProgramControl", "MachName")

			myParm = sc.Parameters.Add("@completionCode", SqlDbType.Int)
			myParm.Direction = ParameterDirection.Input
			myParm.Value = completionCode

			myParm = sc.Parameters.Add("@resultsFolderName", SqlDbType.VarChar, 64)
			myParm.Direction = ParameterDirection.Input
			myParm.Value = resultsFolderName

			myParm = sc.Parameters.Add("@comment", SqlDbType.VarChar, 255)
			myParm.Direction = ParameterDirection.Input
			myParm.Value = comment

			myParm = sc.Parameters.Add("@organismDBName", SqlDbType.VarChar, 64)
			myParm.Direction = ParameterDirection.Input
			myParm.Value = CStr(IIf(m_jobParams.ContainsKey("generatedFastaName"), m_jobParams.Item("generatedFastaName"), ""))

			'Execute the stored procedure
			sc.ExecuteNonQuery()

			'Get return value
			Dim ret As Object
			ret = sc.Parameters("@Return").Value

			'If we made it this far, we succeeded
			Outcome = True

		Catch ex As System.Exception
			m_logger.PostError("Error closing task: ", ex, True)
			Outcome = False
		End Try

		LogErrorEvents()

		Return Outcome

	End Function

	Public Function AddAdditionalParameter(ByVal ParamName As String, ByVal ParamValue As String) As Boolean _
	  Implements IJobParams.AddAdditionalParameter

		'Allows other functions to add parameters as necessary

		Try
			m_jobParams.Add(ParamName, ParamValue)
			Return True
		Catch ex As Exception
			m_logger.PostEntry("clsAnalysisJob.AddAdditionalParameters(), " & ex.Message, _
			 ILogger.logMsgType.logError, True)
			Return False
		End Try

	End Function

End Class

