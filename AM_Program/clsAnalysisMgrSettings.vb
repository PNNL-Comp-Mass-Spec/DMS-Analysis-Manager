'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2007, Battelle Memorial Institute
' Created 12/18/2007
'
' Last modified 06/11/2009 JDS - Added logging using log4net
'*********************************************************************************************************

Option Strict On

Imports AnalysisManagerBase
Imports System.Data.SqlClient
Imports System.IO
Imports System.Xml
Imports System.Threading
Imports System.Reflection

Public Class clsAnalysisMgrSettings
	Implements IMgrParams

	'*********************************************************************************************************
	'Class for loading, storing and accessing manager parameters.
	'	Loads initial settings from local config file, then checks to see if remainder of settings should be
	'		loaded or manager set to inactive. If manager active, retrieves remainder of settings from manager
	'		parameters database.
	'*********************************************************************************************************

#Region "Module variables"
	Private Const SP_NAME_ACKMANAGERUPDATE As String = "AckManagerUpdateRequired"

	Private m_ParamDictionary As Dictionary(Of String, String)
	Private m_ErrMsg As String = ""
	Private ReadOnly m_EmergencyLogSource As String = ""
	Private ReadOnly m_EmergencyLogName As String = ""
	Private ReadOnly m_MgrFolderPath As String
#End Region

#Region "Properties"
	Public ReadOnly Property ErrMsg() As String Implements IMgrParams.ErrMsg
		Get
			Return m_ErrMsg
		End Get
	End Property
#End Region

#Region "Methods"
	''' <summary>
	''' Calls stored procedure AckManagerUpdateRequired in the Manager Control DB
	''' </summary>
	''' <remarks></remarks>
	Public Sub AckManagerUpdateRequired() Implements IMgrParams.AckManagerUpdateRequired

		Dim MyConnection As SqlConnection
		Dim MyCmd As New SqlCommand
		Dim ConnectionString As String

		Try
			ConnectionString = Me.GetParam("MgrCnfgDbConnectStr")
			MyConnection = New SqlConnection(ConnectionString)
			MyConnection.Open()

			'Set up the command object prior to SP execution
			With MyCmd
				.CommandType = CommandType.StoredProcedure
				.CommandText = SP_NAME_ACKMANAGERUPDATE
				.Connection = MyConnection

				.Parameters.Add(New SqlParameter("@Return", SqlDbType.Int))
				.Parameters.Item("@Return").Direction = ParameterDirection.ReturnValue

				.Parameters.Add(New SqlParameter("@managerName", SqlDbType.VarChar, 128))
				.Parameters.Item("@managerName").Direction = ParameterDirection.Input
				.Parameters.Item("@managerName").Value = Me.GetParam("MgrName")

				.Parameters.Add(New SqlParameter("@message", SqlDbType.VarChar, 512))
				.Parameters.Item("@message").Direction = ParameterDirection.Output
				.Parameters.Item("@message").Value = ""
			End With

			'Execute the SP
			MyCmd.ExecuteNonQuery()

		Catch ex As Exception
			Const strErrorMessage As String = "Exception calling " & SP_NAME_ACKMANAGERUPDATE
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, strErrorMessage & ex.Message)
		End Try

	End Sub

	Public Function DisableManagerLocally() As Boolean Implements IMgrParams.DisableManagerLocally
		Return WriteConfigSetting("MgrActive_Local", "False")
	End Function

	''' <summary>
	''' Constructor
	''' </summary>
	''' <param name="EmergencyLogSource">Source name registered for emergency logging</param>
	''' <param name="EmergencyLogName">Name of system log for emergency logging</param>
	''' <remarks></remarks>
	Public Sub New(ByVal EmergencyLogSource As String, ByVal EmergencyLogName As String, ByVal lstMgrSettings As Dictionary(Of String, String), ByVal MgrFolderPath As String)
		m_EmergencyLogName = EmergencyLogName
		m_EmergencyLogSource = EmergencyLogSource
		m_MgrFolderPath = MgrFolderPath

		If Not LoadSettings(lstMgrSettings) Then
			If Not String.IsNullOrEmpty(m_ErrMsg) Then
				Throw New ApplicationException("Unable to initialize manager settings class: " & m_ErrMsg)
			Else
				Throw New ApplicationException("Unable to initialize manager settings class: unknown error")
			End If

		End If

	End Sub

	''' <summary>
	''' Loads manager settings from config file and database
	''' </summary>
	''' <param name="ConfigFileSettings">Manager settings loaded from file AnalysisManagerProg.exe.config</param>
	''' <returns>True if successful; False on error</returns>
	''' <remarks></remarks>
	Public Function LoadSettings(ByVal ConfigFileSettings As Dictionary(Of String, String)) As Boolean Implements IMgrParams.LoadSettings

		m_ErrMsg = ""

		m_ParamDictionary = ConfigFileSettings

		'Test the settings retrieved from the config file
		If Not CheckInitialSettings(m_ParamDictionary) Then
			'Error logging handled by CheckInitialSettings
			Return False
		End If

		'Determine if manager is deactivated locally
		If Not CBool(m_ParamDictionary("MgrActive_Local")) Then
			WriteToEmergencyLog(m_EmergencyLogSource, m_EmergencyLogName, "Manager deactivated locally")
			m_ErrMsg = "Manager deactivated locally"
			Return False
		End If

		'Get settings from Manager Control DB and Broker DB
		If Not LoadDBSettings() Then
			' Errors have already been logged; return False
			Return False
		End If

		'No problems found
		Return True

	End Function

	''' <summary>
	''' Tests initial settings retrieved from config file
	''' </summary>
	''' <param name="InpDict"></param>
	''' <returns></returns>
	''' <remarks></remarks>
	Private Function CheckInitialSettings(ByRef InpDict As Dictionary(Of String, String)) As Boolean

		Dim MyMsg As String

		'Verify manager settings dictionary exists
		If InpDict Is Nothing Then
			MyMsg = "clsMgrSettings.CheckInitialSettings(); Manager parameter string dictionary not found"
			WriteToEmergencyLog(m_EmergencyLogSource, m_EmergencyLogName, MyMsg)
			Return False
		End If

		'Verify intact config file was found
		Dim strValue As String = String.Empty
		If Not InpDict.TryGetValue("UsingDefaults", strValue) Then
			MyMsg = "clsMgrSettings.CheckInitialSettings(); 'UsingDefaults' entry not found in Config file"
			WriteToEmergencyLog(m_EmergencyLogSource, m_EmergencyLogName, MyMsg)
		Else
			Dim blnValue As Boolean
			If Boolean.TryParse(strValue, blnValue) Then
				If blnValue Then
					MyMsg = "clsMgrSettings.CheckInitialSettings(); Config file problem, contains UsingDefaults=True"
					WriteToEmergencyLog(m_EmergencyLogSource, m_EmergencyLogName, MyMsg)
					Return False
				End If
			End If
		End If

		'No problems found
		Return True

	End Function

	' Retrieves the manager and global settings from various databases
	Public Function LoadDBSettings() As Boolean Implements IMgrParams.LoadDBSettings
		Dim blnSuccess As Boolean

		blnSuccess = LoadMgrSettingsFromDB()

		If blnSuccess Then
			blnSuccess = LoadBrokerDBSettings()
		End If

		Return blnSuccess
	End Function

	''' <summary>
	''' Gets manager config settings from manager control DB
	''' </summary>
	''' <returns>True for success; False for error</returns>
	''' <remarks></remarks>
	Protected Function LoadMgrSettingsFromDB() As Boolean

		'Requests manager specific settings from database. Performs retries if necessary.

		Dim ManagerName As String
		Dim strMgrSettingsGroup As String
		Dim dtSettings As DataTable = Nothing
		Dim blnSuccess As Boolean

		Dim blnSkipExistingParameters As Boolean
		Dim blnReturnErrorIfNoParameters As Boolean

		ManagerName = Me.GetParam("MgrName", "")
		If String.IsNullOrEmpty(ManagerName) Then
			m_ErrMsg = "MgrName parameter not found in m_ParamDictionary; it should be defined in the AnalysisManagerProg.exe.config file"
			Return False
		End If

		blnReturnErrorIfNoParameters = True
		blnSuccess = LoadMgrSettingsFromDBWork(ManagerName, dtSettings, blnReturnErrorIfNoParameters)
		If Not blnSuccess Then
			Return False
		End If

		blnSkipExistingParameters = False
		blnSuccess = StoreParameters(dtSettings, blnSkipExistingParameters, ManagerName)

		strMgrSettingsGroup = Me.GetParam("MgrSettingGroupName", "")
		If Not String.IsNullOrEmpty(strMgrSettingsGroup) Then
			' This manager has group-based settings defined; load them now

			blnReturnErrorIfNoParameters = False
			blnSuccess = LoadMgrSettingsFromDBWork(strMgrSettingsGroup, dtSettings, blnReturnErrorIfNoParameters)

			If blnSuccess Then
				blnSkipExistingParameters = True
				blnSuccess = StoreParameters(dtSettings, blnSkipExistingParameters, ManagerName)
			End If
		End If

		Return blnSuccess

	End Function

	Private Function LoadMgrSettingsFromDBWork(ByVal ManagerName As String, ByRef dtSettings As DataTable, ByVal blnReturnErrorIfNoParameters As Boolean) As Boolean

		Dim RetryCount As Short = 3
		Dim ConnectionString As String = Me.GetParam("MgrCnfgDbConnectStr", "")
		dtSettings = Nothing

		If String.IsNullOrEmpty(ManagerName) Then
			m_ErrMsg = "MgrCnfgDbConnectStr parameter not found in m_ParamDictionary; it should be defined in the AnalysisManagerProg.exe.config file"
			Return False
		End If

		Dim SqlStr As String = "SELECT ParameterName, ParameterValue FROM V_MgrParams WHERE ManagerName = '" & ManagerName & "'"

		'Get a table to hold the results of the query
		Dim blnSuccess = clsGlobal.GetDataTableByQuery(SqlStr, ConnectionString, "LoadMgrSettingsFromDBWork", RetryCount, dtSettings)

		'If loop exited due to errors, return false
		If Not blnSuccess Then
			m_ErrMsg = "clsMgrSettings.LoadMgrSettingsFromDBWork; Excessive failures attempting to retrieve manager settings from database for manager '" & ManagerName & "'"
			WriteErrorMsg(m_ErrMsg)
			If Not dtSettings Is Nothing Then dtSettings.Dispose()
			Return False
		End If

		'Verify at least one row returned
		If dtSettings.Rows.Count < 1 And blnReturnErrorIfNoParameters Then
			' No data was returned
			m_ErrMsg = "clsMgrSettings.LoadMgrSettingsFromDBWork; Manager '" & ManagerName & "' not defined in the manager control database; using " & ConnectionString
			WriteErrorMsg(m_ErrMsg)
			dtSettings.Dispose()
			Return False
		End If

		Return True

	End Function

	Private Function StoreParameters(ByVal dtSettings As DataTable, ByVal blnSkipExisting As Boolean, ByVal ManagerName As String) As Boolean

		Dim ParamKey As String
		Dim ParamVal As String
		Dim blnSuccess As Boolean

		'Fill a string dictionary with the manager parameters that have been found
		Try
			For Each oRow As DataRow In dtSettings.Rows
				'Add the column heading and value to the dictionary
				ParamKey = DbCStr(oRow(dtSettings.Columns("ParameterName")))
				ParamVal = DbCStr(oRow(dtSettings.Columns("ParameterValue")))

				If m_ParamDictionary.ContainsKey(ParamKey) Then
					If Not blnSkipExisting Then
						m_ParamDictionary(ParamKey) = ParamVal
					End If
				Else
					m_ParamDictionary.Add(ParamKey, ParamVal)
				End If
			Next
			blnSuccess = True

		Catch ex As Exception
			m_ErrMsg = "clsMgrSettings.LoadMgrSettingsFromDB; Exception filling string dictionary from table for manager '" & ManagerName & "': " & ex.Message
			WriteErrorMsg(m_ErrMsg)
			blnSuccess = False
		Finally
			If Not dtSettings Is Nothing Then dtSettings.Dispose()
		End Try

		Return blnSuccess

	End Function

	''' <summary>
	''' Gets global settings from Broker DB (aka Pipeline DB)
	''' </summary>
	''' <returns>True for success; False for error</returns>
	''' <remarks></remarks>
	Protected Function LoadBrokerDBSettings() As Boolean

		' Retrieves global settings from the Broker DB. Performs retries if necessary.
		'
		' At present, the only settings being retrieved are the param file storage paths for each step tool
		' The storage path for each step tool will be stored in the manager settings dictionary
		' For example: the LCMSFeatureFinder step tool will have an entry with
		'   Name="StepTool_ParamFileStoragePath_LCMSFeatureFinder"
		'   Value="\\gigasax\dms_parameter_Files\LCMSFeatureFinder"

		Dim RetryCount As Short = 3
		Dim MyMsg As String
		Dim ParamKey As String
		Dim ParamVal As String
		Dim ConnectionString As String = Me.GetParam("brokerconnectionstring")

		' Construct the Sql to obtain the information:
		'   SELECT 'StepTool_ParamFileStoragePath_' + Name AS ParameterName, [Param File Storage Path] AS ParameterValue
		'   FROM V_Pipeline_Step_Tools_Detail_Report
		'   WHERE ISNULL([Param File Storage Path], '') <> ''
		'
		Const SqlStr As String = " SELECT '" & clsGlobal.STEPTOOL_PARAMFILESTORAGEPATH_PREFIX & "' + Name AS ParameterName, " & _
								 " [Param File Storage Path] AS ParameterValue" & _
								 " FROM V_Pipeline_Step_Tools_Detail_Report" & _
								 " WHERE ISNULL([Param File Storage Path], '') <> ''"

		Dim Dt As DataTable = Nothing
		Dim blnSuccess As Boolean

		'Get a table to hold the results of the query
		blnSuccess = clsGlobal.GetDataTableByQuery(SqlStr.ToString(), ConnectionString, "LoadBrokerDBSettings", RetryCount, Dt)
		
		'If loop exited due to errors, return false
		If Not blnSuccess Then
			MyMsg = "clsMgrSettings.LoadBrokerDBSettings; Excessive failures attempting to retrieve settings from broker database"
			WriteErrorMsg(MyMsg)
			Dt.Dispose()
			Return False
		End If

		'Verify at least one row returned
		If Dt.Rows.Count < 1 Then
			' No data was returned
			MyMsg = "clsMgrSettings.LoadBrokerDBSettings; V_Pipeline_Step_Tools_Detail_Report returned no rows using " & ConnectionString
			WriteErrorMsg(MyMsg)
			Dt.Dispose()
			Return False
		End If

		' Fill a string dictionary with the new parameters that have been found
		Dim CurRow As DataRow
		Try
			For Each CurRow In Dt.Rows
				'Add the column heading and value to the dictionary
				ParamKey = DbCStr(CurRow(Dt.Columns("ParameterName")))
				ParamVal = DbCStr(CurRow(Dt.Columns("ParameterValue")))

				Me.SetParam(ParamKey, ParamVal)
			Next
			blnsuccess = True
		Catch ex As Exception
			MyMsg = "clsMgrSettings.LoadBrokerDBSettings; Exception filling string dictionary from table: " & ex.Message
			WriteErrorMsg(MyMsg)
			blnsuccess = False
		Finally
			Dt.Dispose()
		End Try

		Return blnsuccess

	End Function

	''' <summary>
	''' Gets a parameter from the manager parameters dictionary
	''' </summary>
	''' <param name="ItemKey">Key name for item</param>
	''' <returns>String value associated with specified key</returns>
	''' <remarks>Returns empty string if key isn't found</remarks>
	Public Function GetParam(ByVal ItemKey As String) As String Implements IMgrParams.GetParam
		Dim Value As String = String.Empty

		If Not m_ParamDictionary Is Nothing Then
			If m_ParamDictionary.TryGetValue(ItemKey, Value) Then
				If String.IsNullOrWhiteSpace(Value) Then
					Return String.Empty
				End If
			Else
				Return String.Empty
			End If
		End If

		Return Value

	End Function

	''' <summary>
	''' Gets a parameter from the manager parameters dictionary
	''' </summary>
	''' <param name="ItemKey">Key name for item</param>
	''' <param name="ValueIfMissing">Value to return if the parameter is not found</param>
	''' <returns>Value for specified parameter; ValueIfMissing if not found</returns>
	Public Function GetParam(ByVal ItemKey As String, ByVal ValueIfMissing As Boolean) As Boolean Implements IMgrParams.GetParam
		Return clsGlobal.CBoolSafe(GetParam(ItemKey), ValueIfMissing)
	End Function

	''' <summary>
	''' Gets a parameter from the manager parameters dictionary
	''' </summary>
	''' <param name="ItemKey">Key name for item</param>
	''' <param name="ValueIfMissing">Value to return if the parameter is not found</param>
	''' <returns>Value for specified parameter; ValueIfMissing if not found</returns>
	Public Function GetParam(ByVal ItemKey As String, ByVal ValueIfMissing As Integer) As Integer Implements IMgrParams.GetParam
		Return clsGlobal.CIntSafe(GetParam(ItemKey), ValueIfMissing)
	End Function

	''' <summary>
	''' Gets a parameter from the manager parameters dictionary
	''' </summary>
	''' <param name="ItemKey">Key name for item</param>
	''' <param name="ValueIfMissing">Value to return if the parameter is not found</param>
	''' <returns>Value for specified parameter; ValueIfMissing if not found</returns>
	Public Function GetParam(ByVal ItemKey As String, ByVal ValueIfMissing As String) As String Implements IMgrParams.GetParam
		Dim strValue As String
		strValue = GetParam(ItemKey)
		If String.IsNullOrEmpty(strValue) Then
			Return ValueIfMissing
		Else
			Return strValue
		End If
	End Function

	''' <summary>
	''' Sets a parameter in the parameters string dictionary
	''' </summary>
	''' <param name="ItemKey">Key name for the item</param>
	''' <param name="ItemValue">Value to assign to the key</param>
	''' <remarks></remarks>
	Public Sub SetParam(ByVal ItemKey As String, ByVal ItemValue As String) Implements IMgrParams.SetParam

		If m_ParamDictionary.ContainsKey(ItemKey) Then
			m_ParamDictionary(ItemKey) = ItemValue
		Else
			m_ParamDictionary.Add(ItemKey, ItemValue)
		End If

	End Sub

	''' <summary>
	''' Writes an error message to application log or manager local log
	''' </summary>
	''' <param name="Message">Message to write</param>
	''' <remarks></remarks>
	Private Sub WriteErrorMsg(ByVal Message As String)

		WriteToEmergencyLog(m_EmergencyLogSource, m_EmergencyLogName, Message)
		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Message)

	End Sub

	''' <summary>
	''' Converts a database output object that could be dbNull to a string
	''' </summary>
	''' <param name="InpObj"></param>
	''' <returns>String equivalent of object; empty string if object is dbNull</returns>
	''' <remarks></remarks>
	Protected Function DbCStr(ByVal InpObj As Object) As String

		'If input object is DbNull, returns "", otherwise returns String representation of object
		If InpObj Is DBNull.Value Then
			Return ""
		Else
			Return CStr(InpObj)
		End If

	End Function

	''' <summary>
	''' Writes specfied value to an application config file.
	''' </summary>
	''' <param name="Key">Name for parameter (case sensitive)</param>
	''' <param name="Value">New value for parameter</param>
	''' <returns>TRUE for success; FALSE for error (ErrMsg property contains reason)</returns>
	''' <remarks>This bit of lunacy is needed because MS doesn't supply a means to write to an app config file</remarks>
	Public Function WriteConfigSetting(ByVal Key As String, ByVal Value As String) As Boolean

		m_ErrMsg = ""

		'Load the config document
		Dim MyDoc As XmlDocument = LoadConfigDocument()
		If MyDoc Is Nothing Then
			'Error message has already been produced by LoadConfigDocument
			Return False
		End If

		'Retrieve the settings node
		Dim MyNode As XmlNode = MyDoc.SelectSingleNode("//applicationSettings")

		If MyNode Is Nothing Then
			m_ErrMsg = "clsMgrSettings.WriteConfigSettings; appSettings node not found"
			Return False
		End If

		Try
			'Select the eleement containing the value for the specified key containing the key
			Dim MyElement As XmlElement = CType(MyNode.SelectSingleNode(String.Format("//setting[@name='{0}']/value", Key)), XmlElement)
			If MyElement IsNot Nothing Then
				'Set key to specified value
				MyElement.InnerText = Value
			Else
				'Key was not found
				m_ErrMsg = "clsMgrSettings.WriteConfigSettings; specified key not found: " & Key
				Return False
			End If
			MyDoc.Save(GetConfigFilePath())
			Return True
		Catch ex As Exception
			m_ErrMsg = "clsMgrSettings.WriteConfigSettings; Exception updating settings file: " & ex.Message
			Return False
		End Try

	End Function

	Protected Sub WriteToEmergencyLog(ByVal SourceName As String, ByVal LogName As String, ByVal Message As String)
		' Post a message to the the Windows application event log named LogName
		' If the application log does not exist yet, we will try to create it
		' However, in order to do that, the program needs to be running from an elevated (administrative level) command prompt
		' Thus, it is advisable to run this program once from an elevated command prompt while MgrActive_Local is set to false

		'If custom event log doesn't exist yet, create it
		If Not EventLog.SourceExists(SourceName) Then
			Dim SourceData As EventSourceCreationData = New EventSourceCreationData(SourceName, LogName)
			EventLog.CreateEventSource(SourceData)
		End If

		'Create custom event logging object and write to log
		Dim ELog As New EventLog
		ELog.Log = LogName
		ELog.Source = SourceName

		Try
			ELog.MaximumKilobytes = 1024
		Catch ex As Exception
			' Leave this as the default
		End Try

		Try
			ELog.ModifyOverflowPolicy(OverflowAction.OverwriteAsNeeded, 90)
		Catch ex As Exception
			' Leave this as the default
		End Try

		EventLog.WriteEntry(SourceName, Message, EventLogEntryType.Error)

	End Sub

	''' <summary>
	''' Loads an app config file for changing parameters
	''' </summary>
	''' <returns>App config file as an XML document if successful; NOTHING on failure</returns>
	''' <remarks></remarks>
	Private Function LoadConfigDocument() As XmlDocument

		Dim MyDoc As XmlDocument

		Try
			MyDoc = New XmlDocument
			MyDoc.Load(GetConfigFilePath)
			Return MyDoc
		Catch ex As Exception
			m_ErrMsg = "clsMgrSettings.LoadConfigDocument; Exception loading settings file: " & ex.Message
			Return Nothing
		End Try

	End Function

	''' <summary>
	''' Specifies the full name and path for the application config file
	''' </summary>
	''' <returns>String containing full name and path</returns>
	''' <remarks></remarks>
	Private Function GetConfigFilePath() As String

		Dim exeName = Path.GetFileName(Assembly.GetExecutingAssembly().Location)
		Return Path.Combine(m_MgrFolderPath, exeName & ".config")

	End Function

#End Region
	
End Class
