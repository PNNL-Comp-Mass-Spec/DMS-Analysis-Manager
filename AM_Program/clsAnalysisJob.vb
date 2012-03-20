'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2007, Battelle Memorial Institute
' Created 12/18/2007
'
' Last modified 06/11/2009 JDS - Added logging using log4net
'*********************************************************************************************************

Option Strict On

Imports System.Data.SqlClient
Imports AnalysisManagerBase.clsGlobal
Imports AnalysisManagerBase.clsAnalysisMgrSettings
Imports System.IO

Namespace AnalysisManagerBase

	Public Class clsAnalysisJob
		Inherits clsDBTask
		Implements IJobParams

		'*********************************************************************************************************
		'Provides DB access and tools for one analysis job
		'*********************************************************************************************************

#Region "Constants"
		Protected Const SP_NAME_SET_COMPLETE As String = "SetStepTaskComplete"
		Protected Const SP_NAME_REQUEST_TASK As String = "RequestStepTaskXML" '"RequestStepTask"
#End Region

#Region "Module variables"
		' The outer dictionary tracks section names, then the inner dictionary tracks key/value pairs within each section
		Protected m_JobParams As System.Collections.Generic.Dictionary(Of String, System.Collections.Generic.Dictionary(Of String, String))

		Protected m_JobId As Integer
		Protected m_TaskWasClosed As Boolean
#End Region

#Region "Methods"
		''' <summary>
		''' Constructor
		''' </summary>
		''' <param name="mgrParams">IMgrParams object containing manager parameters</param>
		''' <remarks></remarks>
		Public Sub New(ByVal mgrParams As IMgrParams, ByVal DebugLvl As Integer)

			MyBase.New(mgrParams, DebugLvl)

			InitializeJobParams()

			m_TaskWasClosed = False

		End Sub

		''' <summary>
		''' Initializes the job parameters dictionary 
		''' </summary>
		''' <remarks></remarks>
		Protected Sub InitializeJobParams()

			If m_JobParams Is Nothing Then
				m_JobParams = New System.Collections.Generic.Dictionary(Of String, System.Collections.Generic.Dictionary(Of String, String))(StringComparer.CurrentCultureIgnoreCase)
			End If

			m_JobParams.Clear()

		End Sub

		''' <summary>
		''' Gets a job parameter with the given name (in any parameter section)
		''' </summary>
		''' <param name="Name">Key name for parameter</param>
		''' <returns>Value for specified parameter; empty string if not found</returns>
		''' <remarks></remarks>
		Public Function GetParam(ByVal Name As String) As String Implements IJobParams.GetParam

			Dim strValue As String = String.Empty

			If TryGetParam(Name, strValue) Then
				Return strValue
			Else
				Return String.Empty
			End If

		End Function

		''' <summary>
		''' Gets a job parameter with the given name, preferentially using the specified parameter section
		''' </summary>
		''' <param name="Section">Section name for parameter</param>
		''' <param name="Name">Key name for parameter</param>
		''' <returns>Value for specified parameter; empty string if not found</returns>
		''' <remarks></remarks>
		Public Function GetParam(ByVal Section As String, ByVal Name As String) As String Implements IJobParams.GetParam

			Dim strValue As String = String.Empty

			If TryGetParam(Section, Name, strValue) Then
				Return strValue
			Else
				Return String.Empty
			End If

		End Function

		'' The two-parameter form of this function is no longer supported
		'' Public Sub SetParam(ByVal ParamName As String, ByVal ParamValue As String) Implements IJobParams.SetParam

		''' <summary>
		''' Add/updates the value for the given parameter
		''' </summary>
		''' <param name="Section">Section name</param>
		''' <param name="ParamName">Parameter name</param>
		''' <param name="ParamValue">Parameter value</param>
		''' <remarks></remarks>
		Public Sub SetParam(ByVal Section As String, ByVal ParamName As String, ByVal ParamValue As String) Implements IJobParams.SetParam

			Dim oParams As System.Collections.Generic.Dictionary(Of String, String) = Nothing

			If m_JobParams Is Nothing Then InitializeJobParams()

			If Not m_JobParams.TryGetValue(Section, oParams) Then
				' Need to add a section with a blank name
				oParams = New System.Collections.Generic.Dictionary(Of String, String)(StringComparer.CurrentCultureIgnoreCase)
				m_JobParams.Add(Section, oParams)
			End If

			If ParamValue Is Nothing Then ParamValue = String.Empty

			If oParams.ContainsKey(ParamName) Then
				oParams(ParamName) = ParamValue
			Else
				oParams.Add(ParamName, ParamValue)
			End If

		End Sub

		''' <summary>
		''' Attempts to retrieve the specified parameter (looks in all parameter sections)
		''' </summary>
		''' <param name="ParamName">Parameter Name</param>
		''' <param name="ParamValue">Output: parameter value</param>
		''' <returns>True if success, False if not found</returns>
		''' <remarks></remarks>
		Public Function TryGetParam(ByVal ParamName As String, ByRef ParamValue As String) As Boolean

			ParamValue = String.Empty

			If Not m_JobParams Is Nothing Then
				For Each oEntry As System.Collections.Generic.KeyValuePair(Of String, System.Collections.Generic.Dictionary(Of String, String)) In m_JobParams
					If oEntry.Value.TryGetValue(ParamName, ParamValue) Then
						If String.IsNullOrWhiteSpace(ParamValue) Then
							ParamValue = String.Empty
						End If
						Return True
					End If
				Next
			End If

			Return False

		End Function

		''' <summary>
		''' Attempts to retrieve the specified parameter in the specified parameter section
		''' </summary>
		''' <param name="Section">Section Name</param>
		''' <param name="ParamName">Parameter Name</param>
		''' <param name="ParamValue">Output: parameter value</param>
		''' <returns>True if success, False if not found</returns>
		''' <remarks></remarks>
		Public Function TryGetParam(ByVal Section As String, ByVal ParamName As String, ByRef ParamValue As String) As Boolean
			Return TryGetParam(Section, ParamName, ParamValue, True)
		End Function

		''' <summary>
		''' Attempts to retrieve the specified parameter in the specified parameter section
		''' </summary>
		''' <param name="Section">Section Name</param>
		''' <param name="ParamName">Parameter Name</param>
		''' <param name="ParamValue">Output: parameter value</param>
		''' <param name="SearchAllSectionsIfNotFound">If True, then searches other sections for the parameter if not found in the specified section</param>
		''' <returns>True if success, False if not found</returns>
		''' <remarks></remarks>
		Public Function TryGetParam(ByVal Section As String, ByVal ParamName As String, ByRef ParamValue As String, ByVal SearchAllSectionsIfNotFound As Boolean) As Boolean

			Dim oParams As System.Collections.Generic.Dictionary(Of String, String) = Nothing
			ParamValue = String.Empty

			If Not m_JobParams Is Nothing Then
				If m_JobParams.TryGetValue(Section, oParams) Then
					If oParams.TryGetValue(ParamName, ParamValue) Then
						If String.IsNullOrWhiteSpace(ParamValue) Then
							ParamValue = String.Empty
						End If
						Return True
					End If
				End If
			End If

			If SearchAllSectionsIfNotFound Then
				' Parameter not found in the specified section
				' Search for the entry in other sections
				Return TryGetParam(ParamName, ParamValue)
			Else
				Return False
			End If

		End Function


		'' The two-parameter version of this function is no longer supported
		'' Public Function AddAdditionalParameter(ByVal ParamName As String, ByVal ParamValue As String) As Boolean Implements IJobParams.AddAdditionalParameter

		''' <summary>
		''' Adds a parameter to the class
		''' </summary>
		''' <param name="SectionName">Section name for parameter</param>
		''' <param name="ParamName">Name of parameter</param>
		''' <param name="ParamValue">Value for parameter</param>
		''' <returns>True if success, False if an error</returns>
		''' <remarks></remarks>
		Public Function AddAdditionalParameter(ByVal SectionName As String, ByVal ParamName As String, ByVal ParamValue As String) As Boolean _
		 Implements IJobParams.AddAdditionalParameter

			Try
				If ParamValue Is Nothing Then ParamValue = String.Empty

				Me.SetParam(SectionName, ParamName, ParamValue)

				Return True
			Catch ex As Exception
				Dim Msg As String = "Exception adding parameter: " & ParamName & " Value: " & ParamValue & "; " & clsGlobal.GetExceptionStackTrace(ex)
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
				Return False
			End Try

		End Function

		''' <summary>
		''' Requests a task from the database
		''' </summary>
		''' <returns>Enum indicating if task was found</returns>
		''' <remarks></remarks>
		Public Overrides Function RequestTask() As clsDBTask.RequestTaskResult

			Dim RetVal As clsDBTask.RequestTaskResult

			RetVal = RequestAnalysisJob()
			Select Case RetVal
				Case clsDBTask.RequestTaskResult.NoTaskFound
					m_TaskWasAssigned = False
				Case clsDBTask.RequestTaskResult.TaskFound
					m_TaskWasAssigned = True
				Case Else
					m_TaskWasAssigned = False
			End Select
			Return RetVal

		End Function

		''' <summary>
		''' Requests a single analysis job
		''' </summary>
		''' <returns></returns>
		''' <remarks></remarks>
		Private Function RequestAnalysisJob() As clsDBTask.RequestTaskResult

			Dim MyCmd As New SqlCommand
			Dim Outcome As clsDBTask.RequestTaskResult = RequestTaskResult.NoTaskFound
			Dim RetVal As Integer
			Dim Dt As New DataTable
			Dim paramXml As String

			Dim strProductVersion As String = Application.ProductVersion
			If strProductVersion Is Nothing Then strProductVersion = "??"
			m_TaskWasClosed = False

			InitializeJobParams()

			Try
				'Set up the command object prior to SP execution
				With MyCmd
					.CommandType = CommandType.StoredProcedure
					.CommandText = SP_NAME_REQUEST_TASK
					.Parameters.Add(New SqlClient.SqlParameter("@Return", SqlDbType.Int))
					.Parameters.Item("@Return").Direction = ParameterDirection.ReturnValue

					.Parameters.Add(New SqlClient.SqlParameter("@processorName", SqlDbType.VarChar, 128))
					.Parameters.Item("@processorName").Direction = ParameterDirection.Input
					.Parameters.Item("@processorName").Value = m_MgrParams.GetParam("MgrName")

					.Parameters.Add(New SqlClient.SqlParameter("@jobNumber", SqlDbType.Int))
					.Parameters.Item("@jobNumber").Direction = ParameterDirection.Output

					.Parameters.Add(New SqlClient.SqlParameter("@parameters", SqlDbType.VarChar, 8000))
					.Parameters.Item("@parameters").Direction = ParameterDirection.Output
					.Parameters.Item("@parameters").Value = ""

					.Parameters.Add(New SqlClient.SqlParameter("@message", SqlDbType.VarChar, 512))
					.Parameters.Item("@message").Direction = ParameterDirection.Output
					.Parameters.Item("@message").Value = ""

					.Parameters.Add(New SqlClient.SqlParameter("@infoOnly", SqlDbType.TinyInt))
					.Parameters.Item("@infoOnly").Direction = ParameterDirection.Input
					.Parameters.Item("@infoOnly").Value = 0

					.Parameters.Add(New SqlClient.SqlParameter("@AnalysisManagerVersion", SqlDbType.VarChar, 128))
					.Parameters.Item("@AnalysisManagerVersion").Direction = ParameterDirection.Input
					.Parameters.Item("@AnalysisManagerVersion").Value = strProductVersion
				End With

				If m_DebugLevel > 4 Then
					Dim MyMsg As String = "clsAnalysisJob.RequestAnalysisJob(), connection string: " & m_BrokerConnStr
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, MyMsg)
					MyMsg = "clsAnalysisJob.RequestAnalysisJob(), printing param list"
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, MyMsg)
					PrintCommandParams(MyCmd)
				End If

				'Execute the SP
				RetVal = ExecuteSP(MyCmd, m_BrokerConnStr, 1)

				Select Case RetVal
					Case RET_VAL_OK
						'No errors found in SP call, so see if any step tasks were found
						m_JobId = CInt(MyCmd.Parameters("@jobNumber").Value)
						paramXml = CStr(MyCmd.Parameters("@parameters").Value)

						'Step task was found; get the data for it
						Dim dctParameters As New System.Collections.Generic.List(Of clsDBTask.udtParameterInfoType)
						dctParameters = FillParamDictXml(paramXml)

						If dctParameters IsNot Nothing Then

							For Each udtParamInfo As clsDBTask.udtParameterInfoType In dctParameters
								Me.SetParam(udtParamInfo.Section, udtParamInfo.ParamName, udtParamInfo.Value)
							Next

							SaveJobParameters(m_MgrParams.GetParam("WorkDir"), paramXml, CStr(MyCmd.Parameters("@jobNumber").Value))
							Outcome = clsDBTask.RequestTaskResult.TaskFound
						Else
							'There was an error
							Dim Msg As String = "clsAnalysisJob.AddTaskParamsToDictionary(), Unable to obtain job data"
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
							Outcome = RequestTaskResult.ResultError
						End If
					Case RET_VAL_TASK_NOT_AVAILABLE
						'No jobs found
						Outcome = clsDBTask.RequestTaskResult.NoTaskFound
					Case RET_VAL_EXCESSIVE_RETRIES
						' Too many retries
						Outcome = clsDBTask.RequestTaskResult.TooManyRetries
					Case RET_VAL_DEADLOCK
						' Transaction was deadlocked on lock resources with another process and has been chosen as the deadlock victim
						Outcome = clsDBTask.RequestTaskResult.Deadlock
					Case Else
						'There was an SP error
						Dim msg As String = "clsAnalysisJob.RequestAnalysisJob(), SP execution error " & RetVal.ToString
						msg &= "; Msg text = " & CStr(MyCmd.Parameters("@message").Value)
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg)
						Outcome = RequestTaskResult.ResultError
				End Select

			Catch ex As System.Exception
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception requesting analysis job: " & ex.Message)
				Outcome = RequestTaskResult.ResultError
			End Try

			Return Outcome

		End Function

		''' <summary>
		''' Saves job Parameters to an XML File
		''' </summary>
		''' <param name="WorkDir">Full path to work directory</param>
		''' <param name="paramXml">Contains the xml for all the job parameters</param>
		''' <param name="jobNum">Contains the job number</param>
		Private Function SaveJobParameters(ByVal WorkDir As String, ByVal paramXml As String, ByVal jobNum As String) As Boolean

			Dim xmlWriter As New clsFormattedXMLWriter
			Dim xmlParameterFilename As String = String.Empty
			Dim xmlParameterFilePath As String = String.Empty

			Try
				xmlParameterFilename = XML_FILENAME_PREFIX & jobNum & "." & XML_FILENAME_EXTENSION
				xmlParameterFilePath = Path.Combine(WorkDir, xmlParameterFilename)

				xmlWriter.WriteXMLToFile(paramXml, xmlParameterFilePath)

				If Not AddAdditionalParameter("JobParameters", "genJobParamsFilename", xmlParameterFilename) Then Return False

				Dim Msg As String = "Job Parameters successfully saved to file: " & xmlParameterFilePath

				' Copy the Job Parameter file to the Analysis Manager folder so that we can inspect it if the job fails
				Dim FInfo As FileInfo = New FileInfo(Application.ExecutablePath)
				clsGlobal.CopyAndRenameFileWithBackup(xmlParameterFilePath, FInfo.DirectoryName, "RecentJobParameters.xml", 5)

				If m_DebugLevel >= 2 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, Msg)
				End If

			Catch ex As System.Exception
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception saving analysis job parameters to " & xmlParameterFilePath & ": " & ex.Message)
				Return False
			End Try

			Return True

		End Function

		''' <summary>
		''' Closes an analysis job
		''' </summary>
		''' <param name="CloseOut">IJobParams enum specifying close out type</param>
		''' <param name="CompMsg">Completion message to be added to database upon closeout</param>
		Public Overrides Sub CloseTask(ByVal CloseOut As IJobParams.CloseOutType, ByVal CompMsg As String)
			CloseTask(CloseOut, CompMsg, 0, String.Empty)
		End Sub

		''' <summary>
		''' Closes an analysis job
		''' </summary>
		''' <param name="CloseOut">IJobParams enum specifying close out type</param>
		''' <param name="CompMsg">Completion message to be added to database upon closeout</param>
		''' <param name="EvalCode">Evaluation code (0 if no special evaulation message)</param>
		''' <param name="EvalMessage">Evaluation message ("" if no special message)</param>
		Public Overrides Sub CloseTask(ByVal CloseOut As IJobParams.CloseOutType, ByVal CompMsg As String, ByVal EvalCode As Integer, ByVal EvalMessage As String)

			Dim MsgStr As String
			Dim CompCode As Integer

			CompCode = CInt(CloseOut)

			If EvalMessage Is Nothing Then EvalMessage = String.Empty

			If m_TaskWasClosed Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Job " & m_JobId & " has already been closed; will not call " & SP_NAME_SET_COMPLETE & " again")
			Else
				m_TaskWasClosed = True
				If Not SetAnalysisJobComplete(SP_NAME_SET_COMPLETE, CompCode, CompMsg, EvalCode, EvalMessage, m_BrokerConnStr) Then
					MsgStr = "Error setting job complete in database, job " & m_JobId
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, MsgStr)
				End If
			End If

		End Sub

		''' <summary>
		''' Communicates with database to perform job closeout
		''' </summary>
		''' <param name="SpName">Name of SP in database to call for closeout</param>
		''' <param name="CompCode">Integer version of ITaskParams specifying closeout type</param>
		''' <param name="CompMsg">Comment to insert in database</param>
		''' <param name="EvalCode">Integer results evaluation code</param>
		''' <param name="EvalMsg">Message describing evaluation results</param>
		''' <param name="ConnStr">Database connection string</param>
		''' <returns>True for success, False for failure</returns>
		''' <remarks>EvalCode and EvalMsg not presently used</remarks>
		Protected Function SetAnalysisJobComplete(ByVal SpName As String,
		   ByVal CompCode As Integer, ByVal CompMsg As String, _
		   ByVal EvalCode As Integer, ByVal EvalMsg As String, _
		   ByVal ConnStr As String) As Boolean

			Dim Outcome As Boolean = False
			Dim ResCode As Integer

			'Setup for execution of the stored procedure
			Dim MyCmd As New SqlCommand
			With MyCmd
				.CommandType = CommandType.StoredProcedure
				.CommandText = SpName

				.Parameters.Add(New SqlClient.SqlParameter("@Return", SqlDbType.Int))
				.Parameters.Item("@Return").Direction = ParameterDirection.ReturnValue
				.Parameters.Add(New SqlClient.SqlParameter("@job", SqlDbType.Int))

				.Parameters.Item("@job").Direction = ParameterDirection.Input
				.Parameters.Item("@job").Value = CInt(Me.GetParam("StepParameters", "Job"))

				.Parameters.Add(New SqlClient.SqlParameter("@step", SqlDbType.Int))
				.Parameters.Item("@step").Direction = ParameterDirection.Input
				.Parameters.Item("@step").Value = CInt(Me.GetParam("StepParameters", "Step"))

				.Parameters.Add(New SqlClient.SqlParameter("@completionCode", SqlDbType.Int))
				.Parameters.Item("@completionCode").Direction = ParameterDirection.Input
				.Parameters.Item("@completionCode").Value = CompCode

				.Parameters.Add(New SqlClient.SqlParameter("@completionMessage", SqlDbType.VarChar, 256))
				.Parameters.Item("@completionMessage").Direction = ParameterDirection.Input
				.Parameters.Item("@completionMessage").Value = CompMsg

				.Parameters.Add(New SqlClient.SqlParameter("@evaluationCode", SqlDbType.Int))
				.Parameters.Item("@evaluationCode").Direction = ParameterDirection.Input
				.Parameters.Item("@evaluationCode").Value = EvalCode

				.Parameters.Add(New SqlClient.SqlParameter("@evaluationMessage", SqlDbType.VarChar, 256))
				.Parameters.Item("@evaluationMessage").Direction = ParameterDirection.Input
				.Parameters.Item("@evaluationMessage").Value = EvalMsg

				.Parameters.Add(New SqlClient.SqlParameter("@organismDBName", SqlDbType.VarChar, 64))
				.Parameters.Item("@organismDBName").Direction = ParameterDirection.Input

				Dim strValue As String = String.Empty
				If Me.TryGetParam("PeptideSearch", "generatedFastaName", strValue) Then
					.Parameters.Item("@organismDBName").Value = strValue
				Else
					.Parameters.Item("@organismDBName").Value = String.Empty
				End If

			End With

			'Execute the SP (retry the call up to 20 times)
			ResCode = ExecuteSP(MyCmd, ConnStr, 20)

			If ResCode = 0 Then
				Outcome = True
			Else
				Dim Msg As String = "Error " & ResCode.ToString & " setting analysis job complete"
				'			Msg &= "; Message = " & CStr(MyCmd.Parameters("@message").Value)
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
				Outcome = False
			End If

			Return Outcome

		End Function

		''' <summary>
		''' Uses the "ToolName" and "StepTool" entries in m_JobParamsTable to generate the tool name for the current analysis job
		''' Example tool names are "Sequest" or "DTA_Gen (Sequest)" or "DataExtractor (XTandem)"
		''' </summary>
		''' <returns>Tool name</returns>
		''' <remarks></remarks>
		Public Function GetCurrentJobToolDescription() As String Implements IJobParams.GetCurrentJobToolDescription
			Dim strTool As String

			Dim strToolAndStepTool As String
			Dim strStep As String

			If m_JobParams Is Nothing Then
				strToolAndStepTool = "??"
			Else
				strTool = Me.GetParam("ToolName")

				strToolAndStepTool = Me.GetParam("StepTool")
				If strToolAndStepTool Is Nothing Then strToolAndStepTool = String.Empty

				strStep = Me.GetParam("StepParameters", "Step")
				If strStep Is Nothing Then strStep = String.Empty

				If strToolAndStepTool <> strTool Then
					If strToolAndStepTool.Length > 0 Then
						strToolAndStepTool &= " (" & strTool & ")"
					Else
						strToolAndStepTool &= strTool
					End If
				End If

				If strStep.Length > 0 Then
					strToolAndStepTool &= ", Step " & strStep
				End If
			End If

			Return strToolAndStepTool

		End Function

#End Region

	End Class

End Namespace
