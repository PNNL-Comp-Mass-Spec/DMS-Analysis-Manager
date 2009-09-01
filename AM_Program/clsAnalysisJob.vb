'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2007, Battelle Memorial Institute
' Created 12/18/2007
'
' Last modified 06/11/2009 JDS - Added logging using log4net
'*********************************************************************************************************

Imports System.Collections.Specialized
Imports System.Data.SqlClient
Imports AnalysisManagerBase.clsGlobal
Imports AnalysisManagerBase.clsAnalysisMgrSettings
Imports clsFormattedXMLWriter
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
		Protected m_JobParams As New StringDictionary
        Protected m_JobId As Integer
#End Region

#Region "Methods"
		''' <summary>
		''' Constructor
		''' </summary>
		''' <param name="mgrParams">IMgrParams object containing manager parameters</param>
        ''' <remarks></remarks>
        Public Sub New(ByVal mgrParams As IMgrParams, ByVal DebugLvl As Integer)

            MyBase.New(mgrParams, DebugLvl)

            'Ensure job parameters collection has been cleared
            m_JobParams.Clear()

        End Sub

		''' <summary>
		''' Gets a task parameter from the task params class
		''' </summary>
		''' <param name="Name">Key name for parameter</param>
		''' <returns>Value for specified parameter</returns>
		''' <remarks></remarks>
		Public Function GetParam(ByVal Name As String) As String Implements IJobParams.GetParam

            Dim Value As String

            Value = m_JobParams(Name)
            If Value Is Nothing Then
                Value = String.Empty
            End If

            Return Value

		End Function

		Public Sub SetParam(ByVal KeyName As String, ByVal Value As String) Implements IJobParams.SetParam

            If Value Is Nothing Then Value = String.Empty
			m_JobParams(KeyName) = Value

		End Sub

		''' <summary>
		''' Adds a parameter to the class
		''' </summary>
		''' <param name="ParamName">Name of parameter</param>
		''' <param name="ParamValue">Value for parameter</param>
		''' <returns>TRUE for success, FALSE for error</returns>
		''' <remarks></remarks>
		Public Function AddAdditionalParameter(ByVal ParamName As String, ByVal ParamValue As String) As Boolean _
		 Implements IJobParams.AddAdditionalParameter

			Try
				m_JobParams.Add(ParamName, ParamValue)
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

			Dim RetVal As RequestTaskResult

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
            Dim Params As String
			Dim strProductVersion As String = Application.ProductVersion
			If strProductVersion Is Nothing Then strProductVersion = "??"

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
						Params = CStr(MyCmd.Parameters("@parameters").Value)

						'Step task was found; get the data for it
                        m_JobParams = FillParamDictXml(Params)
                        If m_JobParams IsNot Nothing Then
                            SaveJobParameters(m_MgrParams.GetParam("WorkDir"), Params, CStr(MyCmd.Parameters("@jobNumber").Value))
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
        ''' <remarks>Overloads the CloseTask sub inherited from base class.</remarks>
        Private Function SaveJobParameters(ByVal WorkDir As String, ByVal paramXml As String, ByVal jobNum As String) As Boolean

            Dim xmlWriter As New clsFormattedXMLWriter
            Dim xmlParameterFilename As String = String.Empty
            Dim xmlParameterFilePath As String = String.Empty

            Try
                xmlParameterFilename = XML_FILENAME_PREFIX & jobNum & "." & XML_FILENAME_EXTENSION
                xmlParameterFilePath = Path.Combine(WorkDir, xmlParameterFilename)

                xmlWriter.WriteXMLToFile(paramXml, xmlParameterFilePath)

                If Not AddAdditionalParameter("genJobParamsFilename", xmlParameterFilename) Then Return False

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
        ''' <remarks>Overloads the CloseTask sub inherited from base class.</remarks>
		Public Overrides Sub CloseTask(ByVal CloseOut As IJobParams.CloseOutType, ByVal CompMsg As String)

			'NOTE: This sub actually overrides and overloads sub CloseTask in base class

			Dim MsgStr As String
			Dim CompCode As Integer

			CompCode = CInt(CloseOut)
			'Evaluation Code and Evaluation message are presently not used
			If Not SetAnalysisJobComplete(SP_NAME_SET_COMPLETE, CompCode, CompMsg, 0, "", m_BrokerConnStr) Then
				MsgStr = "Error setting job complete in database, job " & m_JobId
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, MsgStr)
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
		Protected Function SetAnalysisJobComplete(ByVal SpName As String, ByVal CompCode As Integer, _
		  ByVal CompMsg As String, ByVal EvalCode As Integer, ByVal EvalMsg As String, ByVal ConnStr As String) As Boolean

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
				.Parameters.Item("@job").Value = CInt(m_JobParams("Job"))
				.Parameters.Add(New SqlClient.SqlParameter("@step", SqlDbType.Int))
				.Parameters.Item("@step").Direction = ParameterDirection.Input
				.Parameters.Item("@step").Value = CInt(m_JobParams("Step"))
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
				.Parameters.Item("@organismDBName").Value = CStr(IIf(m_JobParams.ContainsKey("generatedFastaName"), _
					m_JobParams.Item("generatedFastaName"), ""))
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
		''' Uses the "ToolName" and "StepTool" entries in m_jobParams to generate the tool name for the current analysis job
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

                strStep = Me.GetParam("Step")
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
