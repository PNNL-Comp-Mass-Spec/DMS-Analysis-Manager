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
	Private m_assignedTool As String	 ' specific tool for the assigned job
#End Region

#Region "parameters for calling stored procedures"
	Private mp_toolName As String
	Private mp_processorName As String
	Private mp_requestedPriority As Int32
	Private mp_requestedMinDuration As Double
	Private mp_requestedMaxDuration As Double
	Private mp_jobNum As String
	Private mp_datasetNum As String
	Private mp_datasetFolderName As String
	Private mp_datasetFolderStoragePath As String
	Private mp_transferFolderPath As String
	Private mp_parmFileName As String
	Private mp_parmFileStoragePath As String
	Private mp_settingsFileName As String
	Private mp_settingsFileStoragePath As String
	Private mp_organismDBName As String
	Private mp_organismDBStoragePath As String
	Private mp_instClass As String
	Private mp_comment As String
	Private mp_legacyFastaFileName As String
#End Region

	Public ReadOnly Property AssignedTool() As String
		Get
			Return m_assignedTool
		End Get
	End Property

	Public Function RequestJob() As Boolean
		Dim tools As String = m_mgrParams.GetParam("ProgramControl", "AnalysisTypes")
		Dim priorities As String = m_mgrParams.GetParam("ProgramControl", "Priorities")

		Dim cpu As String
		Dim minDur As String = m_mgrParams.GetParam("ProgramControl", "minduration")
		Dim maxDur As String = m_mgrParams.GetParam("ProgramControl", "maxduration")


		mp_processorName = m_mgrParams.GetParam("ProgramControl", "MachName")
		mp_requestedMinDuration = Double.Parse(minDur)
		mp_requestedMaxDuration = Double.Parse(maxDur)

		m_connection_str = m_mgrParams.GetParam("DatabaseSettings", "ConnectionString")

		' cycle though combinations of priorities and tools
		' requesting available jobs
		'
		Try
			OpenConnection()
		Catch Err As Exception
			m_logger.PostEntry("clsAnalysisJob.RequestJob(), error opening connection, " & Err.Message, ILogger.logMsgType.logError, True)
			Return False
		End Try

		Dim priority As String
		Try
			For Each priority In Split(priorities, ";")
				mp_requestedPriority = Int32.Parse(priority)
				For Each mp_toolName In Split(tools, ";")
					' call request stored procedure
					m_TaskWasAssigned = RequestAnalysisJobEx5()
					If m_TaskWasAssigned Then Exit For
				Next
				If m_TaskWasAssigned Then Exit For
			Next
		Catch Err As Exception
			m_logger.PostEntry("clsAnalysisJob.RequestJob(), Error running RequestAnalysisJobEx5, " & Err.Message, ILogger.logMsgType.logError, True)
			Return False
		End Try

		Try
			CLoseConnection()
		Catch Err As Exception
			m_logger.PostEntry("clsAnalysisJob.RequestJob(), Error closing connection, " & Err.Message, ILogger.logMsgType.logError, True)
			Return False
		End Try

		If m_TaskWasAssigned Then
			m_assignedTool = mp_toolName
			' set up parameters conveniently in dictionary for "GetParam" method to use
			'
			m_jobParams("jobNum") = mp_jobNum
			m_jobParams("datasetNum") = mp_datasetNum
			m_jobParams("datasetFolderName") = mp_datasetFolderName
			m_jobParams("datasetFolderStoragePath") = mp_datasetFolderStoragePath
			m_jobParams("transferFolderPath") = mp_transferFolderPath
			m_jobParams("parmFileName") = mp_parmFileName
			m_jobParams("parmFileStoragePath") = mp_parmFileStoragePath
			m_jobParams("settingsFileName") = mp_settingsFileName
			m_jobParams("settingsFileStoragePath") = mp_settingsFileStoragePath
			'TODO: Next two parameters may no longer be needed
			m_jobParams("organismDBName") = mp_organismDBName
			m_jobParams("organismDBStoragePath") = mp_organismDBStoragePath
			'Replaces organismDBName (needs to be verified true in all cases)
			m_jobParams("legacyFastaFileName") = mp_legacyFastaFileName
			m_jobParams("instClass") = mp_instClass
			m_jobParams("comment") = mp_comment
			m_jobParams("tool") = mp_toolName
			m_jobParams("priority") = mp_requestedPriority.ToString

			'Obtain additional parameters
			If Not RequestAdditionalJobParamenters(mp_jobNum) Then Return False
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
		SetAnalysisJobCompleteEx5(GetCompletionCode(closeOut), resultsFolderName, comment)
		CLoseConnection()
	End Sub

	Private Function GetCompletionCode(ByVal closeOut As IJobParams.CloseOutType) As Integer
		Dim code As Integer = 1			 '  0->success, 1->failure, anything else ->no intermediate files
		Select Case closeOut
			Case IJobParams.CloseOutType.CLOSEOUT_SUCCESS
				code = 0
			Case IJobParams.CloseOutType.CLOSEOUT_FAILED
				code = 1
			Case IJobParams.CloseOutType.CLOSEOUT_NO_DTA_FILES
				code = 2
			Case IJobParams.CloseOutType.CLOSEOUT_NO_OUT_FILES
				code = 3
				'Case IJobParams.CloseOutType.CLOSEOUT_NO_HTML_FILES
				'	code = 4
		End Select
		GetCompletionCode = code
	End Function

	'-------[for interface IJobParams]----------------------------------------------
	Public Function GetParam(ByVal Name As String) As String Implements IJobParams.GetParam
		Return m_jobParams(Name)
	End Function

	'------[for DB access]-----------------------------------------------------------

	Private Function RequestAnalysisJobEx5() As Boolean

		Dim sc As SqlCommand
		Dim Outcome As Boolean = False
		Dim TempMaxDuration As Single

		Try
			m_error_list.Clear()
			' create the command object
			'
			sc = New SqlCommand("RequestAnalysisJobEx5", m_DBCn)
			sc.CommandType = CommandType.StoredProcedure

			' define parameters for command object
			'
			Dim myParm As SqlParameter
			'
			' define parameter for stored procedure's return value
			'
			myParm = sc.Parameters.Add("@Return", SqlDbType.Int)
			myParm.Direction = ParameterDirection.ReturnValue
			'
			' define parameters for the stored procedure's arguments
			'
			myParm = sc.Parameters.Add("@toolName", SqlDbType.VarChar, 64)
			myParm.Direction = ParameterDirection.Input
			myParm.Value = mp_toolName

			myParm = sc.Parameters.Add("@processorName", SqlDbType.VarChar, 64)
			myParm.Direction = ParameterDirection.Input
			myParm.Value = mp_processorName

			myParm = sc.Parameters.Add("@requestedPriority", SqlDbType.Int)
			myParm.Direction = ParameterDirection.Input
			myParm.Value = mp_requestedPriority

			myParm = sc.Parameters.Add("@requestedMinDuration", SqlDbType.Float)
			myParm.Direction = ParameterDirection.Input
			myParm.Value = mp_requestedMinDuration

			myParm = sc.Parameters.Add("@requestedMaxDuration", SqlDbType.Float)
			myParm.Direction = ParameterDirection.Input
			TempMaxDuration = CSng(IIf(mp_requestedPriority = 1, 99000, mp_requestedMaxDuration))
			myParm.Value = TempMaxDuration

			myParm = sc.Parameters.Add("@jobNum", SqlDbType.VarChar, 32)
			myParm.Direction = ParameterDirection.Output

			myParm = sc.Parameters.Add("@datasetNum", SqlDbType.VarChar, 64)
			myParm.Direction = ParameterDirection.Output

			myParm = sc.Parameters.Add("@datasetFolderName", SqlDbType.VarChar, 128)
			myParm.Direction = ParameterDirection.Output

			myParm = sc.Parameters.Add("@datasetFolderStoragePath", SqlDbType.VarChar, 255)
			myParm.Direction = ParameterDirection.Output

			myParm = sc.Parameters.Add("@transferFolderPath", SqlDbType.VarChar, 255)
			myParm.Direction = ParameterDirection.Output

            myParm = sc.Parameters.Add("@parmFileName", SqlDbType.VarChar, 255)
			myParm.Direction = ParameterDirection.Output

			myParm = sc.Parameters.Add("@parmFileStoragePath", SqlDbType.VarChar, 255)
			myParm.Direction = ParameterDirection.Output

			myParm = sc.Parameters.Add("@settingsFileName", SqlDbType.VarChar, 64)
			myParm.Direction = ParameterDirection.Output

			myParm = sc.Parameters.Add("@settingsFileStoragePath", SqlDbType.VarChar, 255)
			myParm.Direction = ParameterDirection.Output

			myParm = sc.Parameters.Add("@organismDBName", SqlDbType.VarChar, 64)
			myParm.Direction = ParameterDirection.Output

			myParm = sc.Parameters.Add("@organismDBStoragePath", SqlDbType.VarChar, 255)
			myParm.Direction = ParameterDirection.Output

			myParm = sc.Parameters.Add("@instClass", SqlDbType.VarChar, 32)
			myParm.Direction = ParameterDirection.Output

			myParm = sc.Parameters.Add("@comment", SqlDbType.VarChar, 255)
			myParm.Direction = ParameterDirection.Output

			' execute the stored procedure
			'
			sc.ExecuteNonQuery()

			' get return value
			'
			'        Dim ret As Object
			Dim ret As Integer
			ret = CInt(sc.Parameters("@Return").Value)

			If ret = 0 Then
				' get values for output parameters
				'
				mp_jobNum = CStr(sc.Parameters("@jobNum").Value)
				mp_datasetNum = CStr(sc.Parameters("@datasetNum").Value)
				mp_datasetFolderName = CStr(sc.Parameters("@datasetFolderName").Value)
				mp_datasetFolderStoragePath = CStr(sc.Parameters("@datasetFolderStoragePath").Value)
				mp_transferFolderPath = CStr(sc.Parameters("@transferFolderPath").Value)
				mp_parmFileName = CStr(sc.Parameters("@parmFileName").Value)
				mp_parmFileStoragePath = CStr(sc.Parameters("@parmFileStoragePath").Value)
				mp_settingsFileName = CStr(sc.Parameters("@settingsFileName").Value)
				mp_settingsFileStoragePath = CStr(sc.Parameters("@settingsFileStoragePath").Value)
				'TODO: The next two parameters may no longer be needed, but are left in until we know for sure
				mp_organismDBName = CStr(sc.Parameters("@organismDBName").Value)
				mp_organismDBStoragePath = CStr(sc.Parameters("@organismDBStoragePath").Value)
				'This parameter replaces mp_organismDBName for Sequest/XTandem analysis
				mp_legacyFastaFileName = CStr(sc.Parameters("@organismDBName").Value)
				mp_instClass = CStr(sc.Parameters("@instClass").Value)
				mp_comment = CStr(sc.Parameters("@comment").Value)

				' if we made it this far, we succeeded
				'
				Outcome = True
			End If

		Catch ex As System.Exception
			m_logger.PostError("Error requesting task: ", ex, True)
			Outcome = False
		End Try

		LogErrorEvents()

		Return Outcome
	End Function

	Private Function SetAnalysisJobCompleteEx5(ByVal completionCode As Int32, ByVal resultsFolderName As String, ByVal comment As String) As Boolean
		'			Dim m_DBCn As SqlConnection
		Dim sc As SqlCommand
		Dim Outcome As Boolean = False

		Try
			m_error_list.Clear()

			' create the command object
			'
			sc = New SqlCommand("SetAnalysisJobCompleteEx5", m_DBCn)
			sc.CommandType = CommandType.StoredProcedure

			' define parameters for command object
			'
			Dim myParm As SqlParameter
			'
			' define parameter for stored procedure's return value
			'
			myParm = sc.Parameters.Add("@Return", SqlDbType.Int)
			myParm.Direction = ParameterDirection.ReturnValue
			'
			' define parameters for the stored procedure's arguments
			'
			myParm = sc.Parameters.Add("@jobNum", SqlDbType.VarChar, 32)
			myParm.Direction = ParameterDirection.Input
			myParm.Value = mp_jobNum

			myParm = sc.Parameters.Add("@processorName", SqlDbType.VarChar, 64)
			myParm.Direction = ParameterDirection.Input
			myParm.Value = mp_processorName

			myParm = sc.Parameters.Add("@completionCode", SqlDbType.Int)
			myParm.Direction = ParameterDirection.Input
			myParm.Value = completionCode

			myParm = sc.Parameters.Add("@resultsFolderName", SqlDbType.VarChar, 64)
			myParm.Direction = ParameterDirection.Input
			myParm.Value = resultsFolderName

			myParm = sc.Parameters.Add("@comment", SqlDbType.VarChar, 255)
			myParm.Direction = ParameterDirection.Input
			myParm.Value = comment


			' execute the stored procedure
			'
			sc.ExecuteNonQuery()

			' get return value
			'
			Dim ret As Object
			ret = sc.Parameters("@Return").Value

			' get values for output parameters
			'

			' if we made it this far, we succeeded
			'
			Outcome = True

		Catch ex As System.Exception
			m_logger.PostError("Error closing task: ", ex, True)
			Outcome = False
		End Try

		LogErrorEvents()

		Return Outcome

	End Function

	Private Function RequestAdditionalJobParamenters(ByVal JobNum As String) As Boolean

		'Requests additional job parameters from database and adds them to the m_jobParams string dictionary
		Dim SQL As String = "SELECT * FROM V_Analysis_Job_Additional_Parameters WHERE Job = " & JobNum

		'Get a list of all records in database (hopefully just one) matching the job number
		Dim Cn As New SqlConnection(m_connection_str)
		Dim Da As New SqlDataAdapter(SQL, Cn)
		Dim Ds As DataSet = New DataSet

		Try
			Da.Fill(Ds)
		Catch ex As Exception
			m_logger.PostEntry("clsAnalysisJob.RequestAdditionalParameters(), Filling data adapter, " & ex.Message, _
				ILogger.logMsgType.logError, True)
			Return False
		End Try

		Dim Dt As DataTable = Ds.Tables(0)
		If Dt.Rows.Count <> 1 Then
			m_logger.PostEntry("clsAnalysisJob.RequestAdditionalParameters(), invalid row count: " & Dt.Rows.Count.ToString, _
				ILogger.logMsgType.logError, True)
			Return False
		End If

		'Read the extra parameters into a collection
		Dim MyRow As DataRow = Dt.Rows(0)
		Dim cols As DataColumnCollection = Dt.Columns
		Dim col As DataColumn
		Try
			'Add the raw data type to the string dictionary
			For Each col In cols
				m_jobParams.Add(col.ColumnName, CStr(MyRow(Dt.Columns(col.ColumnName))))
				'm_jobParams.Add(col.ColumnName, DbCStr(MyRow(Dt.Columns(col.ColumnName))))
			Next
		Catch ex As Exception
			m_logger.PostEntry("clsAnalysisJob.RequestAdditionalParameters(), Filling additional parameter collection, " _
			  & ex.Message, ILogger.logMsgType.logError, True)
			Return False
		End Try

		Return True

	End Function

	'-------[for interface IJobParams]----------------------------------------------
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

