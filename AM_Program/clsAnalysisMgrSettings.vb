'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2007, Battelle Memorial Institute
' Created 12/18/2007
'
' Last modified 01/16/2008
'*********************************************************************************************************

Imports PRISM.Logging
Imports System.Data.SqlClient
Imports System.IO
Imports System.Collections.Specialized
Imports System.Xml
Imports System.Configuration
Imports System.Windows.Forms

Namespace AnalysisManagerBase

	Public Class clsAnalysisMgrSettings
		Implements IMgrParams

		'*********************************************************************************************************
		'Class for loading, storing and accessing manager parameters.
		'	Loads initial settings from local config file, then checks to see if remainder of settings should be
		'		loaded or manager set to inactive. If manager active, retrieves remainder of settings from manager
		'		parameters database.
		'*********************************************************************************************************

#Region "Module variables"
		Private m_ParamDictionary As StringDictionary
		Private m_ErrMsg As String = ""
		Private m_EmergencyLogSource As String = ""
		Private m_EmergencyLogName As String = ""
#End Region

#Region "Properties"
		Public ReadOnly Property ErrMsg() As String
			Get
				Return m_ErrMsg
			End Get
		End Property
#End Region

#Region "Methods"
		''' <summary>
		''' Constructor
		''' </summary>
		''' <param name="EmergencyLogSource">Source name registered for emergency logging</param>
		''' <param name="EmergencyLogName">Name of system log for emergency logging</param>
		''' <remarks></remarks>
		Public Sub New(ByVal EmergencyLogSource As String, ByVal EmergencyLogName As String)
			m_EmergencyLogName = EmergencyLogName
			m_EmergencyLogSource = EmergencyLogSource

			If Not LoadSettings(Nothing) Then
				Throw New ApplicationException("Unable to initialize manager settings class")
			End If

		End Sub

		''' <summary>
		''' Loads manager settings from config file and database
		''' </summary>
		''' <param name="MyLogger">Logging object if logger has been loaded; otherwise NOTHING</param>
		''' <returns>True if successful; False on error</returns>
		''' <remarks></remarks>
		Public Function LoadSettings(ByVal MyLogger As ILogger) As Boolean

			m_ErrMsg = ""

			'If the param dictionary exists, it needs to be cleared out
			If m_ParamDictionary IsNot Nothing Then
				m_ParamDictionary.Clear()
				m_ParamDictionary = Nothing
			End If

			'Get settings from config file
			m_ParamDictionary = LoadMgrSettingsFromFile()

			'Test the settings retrieved from the config file
			If Not CheckInitialSettings(m_ParamDictionary) Then
				'Error logging handled by CheckInitialSettings
				Return False
			End If

			'Determine if manager is deactivated locally
			If Not CBool(m_ParamDictionary("MgrActive_Local")) Then
				clsEmergencyLog.WriteToLog(m_EmergencyLogSource, m_EmergencyLogName, "Manager deactivated locally")
				m_ErrMsg = "Manager deactivated locally"
				Return False
			End If

			'Get remaining settings from database
			If Not LoadMgrSettingsFromDB(m_ParamDictionary, MyLogger) Then
				'Error logging handled by LoadMgrSettingsFromDB
				Return False
			End If

			'No problems found
			Return True

		End Function

		''' <summary>
		''' Loads the initial settings from application config file
		''' </summary>
		''' <returns>String dictionary containing initial settings if suceessful; NOTHING on error</returns>
		''' <remarks></remarks>
		Private Function LoadMgrSettingsFromFile() As StringDictionary

			'Load initial settings into string dictionary for return
			Dim RetDict As New StringDictionary

			My.Settings.Reload()
			'Manager config db connection string
			RetDict.Add("MgrCnfgDbConnectStr", My.Settings.MgrCnfgDbConnectStr)

			'Manager active flag
			RetDict.Add("MgrActive_Local", My.Settings.MgrActive_Local.ToString)

			'Manager name
			RetDict.Add("MgrName", My.Settings.MgrName)

			'Default settings in use flag
			RetDict.Add("UsingDefaults", My.Settings.UsingDefaults.ToString)

			Return RetDict

		End Function

		''' <summary>
		''' Tests initial settings retrieved from config file
		''' </summary>
		''' <param name="InpDict"></param>
		''' <returns></returns>
		''' <remarks></remarks>
		Private Function CheckInitialSettings(ByRef InpDict As StringDictionary) As Boolean

			Dim MyMsg As String

			'Verify manager settings dictionary exists
			If InpDict Is Nothing Then
				MyMsg = "clsMgrSettings.CheckInitialSettings(); Manager parameter string dictionary not found"
				clsEmergencyLog.WriteToLog(m_EmergencyLogSource, m_EmergencyLogName, MyMsg)
				Return False
			End If

			'Verify intact config file was found
			If CBool(InpDict("UsingDefaults")) Then
				MyMsg = "clsMgrSettings.CheckInitialSettings(); Config file problem, default settings being used"
				clsEmergencyLog.WriteToLog(m_EmergencyLogSource, m_EmergencyLogName, MyMsg)
				Return False
			End If

			'No problems found
			Return True

		End Function

		''' <summary>
		''' Gets remaining manager config settings from config database; 
		''' Overload to use module-level string dictionary when calling from external method
		''' </summary>
		''' <param name="MyLogger">Logging object or NOTHING</param>
		''' <returns>True for success; False for error</returns>
		''' <remarks></remarks>
		Public Overloads Function LoadMgrSettingsFromDB(ByVal MyLogger As ILogger) As Boolean

			Return LoadMgrSettingsFromDB(m_ParamDictionary, MyLogger)

		End Function


		''' <summary>
		''' Gets remaining manager config settings from config database
		''' </summary>
		''' <param name="MgrSettingsDict">String dictionary containing parameters that have been loaded so far</param>
		''' <param name="MyLogger">Logging object or NOTHING</param>
		''' <returns>True for success; False for error</returns>
		''' <remarks></remarks>
		Public Overloads Function LoadMgrSettingsFromDB(ByRef MgrSettingsDict As StringDictionary, ByVal MyLogger As ILogger) As Boolean

			'Requests job parameters from database. Input string specifies view to use. Performs retries if necessary.

			Dim RetryCount As Short = 3
			Dim MyMsg As String
			Dim ParamKey As String
			Dim ParamVal As String

			Dim SqlStr As String = "SELECT ParameterName, ParameterValue FROM V_MgrParams WHERE ManagerName = '" & _
			  m_ParamDictionary("MgrName") & "'"

			'Get a table containing data for job
			Dim Dt As DataTable = Nothing

			'Get a datatable holding the parameters for one manager
			While RetryCount > 0
				Try
					Using Cn As SqlConnection = New SqlConnection(MgrSettingsDict("MgrCnfgDbConnectStr"))
						Using Da As SqlDataAdapter = New SqlDataAdapter(SqlStr, Cn)
							Using Ds As DataSet = New DataSet
								Da.Fill(Ds)
								Dt = Ds.Tables(0)
							End Using  'Ds
						End Using  'Da
					End Using  'Cn
					Exit While
				Catch ex As System.Exception
					RetryCount -= 1S
					MyMsg = "clsMgrSettings.LoadMgrSettingsFromDB; Exception getting manager settings from database: " & ex.Message
					MyMsg &= ", RetryCount = " & RetryCount.ToString
					WriteErrorMsg(MyMsg, MyLogger)
					System.Threading.Thread.Sleep(5000)				'Delay for 5 second before trying again
				End Try
			End While

			'If loop exited due to errors, return false
			If RetryCount < 1 Then
				MyMsg = "clsMgrSettings.LoadMgrSettingsFromDB; Excessive failures attempting to retrieve manager settings from database"
				WriteErrorMsg(MyMsg, MyLogger)
				Dt.Dispose()
				Return False
			End If

			'Verify at least one row returned
			If Dt.Rows.Count < 1 Then
				'Wrong number of rows returned
				MyMsg = "clsMgrSettings.LoadMgrSettingsFromDB; Invalid row count retrieving manager settings: RowCount = "
				MyMsg &= Dt.Rows.Count.ToString
				WriteErrorMsg(MyMsg, MyLogger)
				Dt.Dispose()
				Return False
			End If

			'Fill a string dictionary with the manager parameters that have been found
			Dim CurRow As DataRow
			Try
				For Each CurRow In Dt.Rows
					'Add the column heading and value to the dictionary
					ParamKey = DbCStr(CurRow(Dt.Columns("ParameterName")))
					ParamVal = DbCStr(CurRow(Dt.Columns("ParameterValue")))
					If m_ParamDictionary.ContainsKey(ParamKey) Then
						m_ParamDictionary(ParamKey) = ParamVal
					Else
						m_ParamDictionary.Add(ParamKey, ParamVal)
					End If
				Next
				Return True
			Catch ex As System.Exception
				MyMsg = "clsMgrSettings.LoadMgrSettingsFromDB; Exception filling string dictionary from table: " & ex.Message
				WriteErrorMsg(MyMsg, MyLogger)
				Return False
			Finally
				Dt.Dispose()
			End Try

		End Function

		''' <summary>
		''' Gets a parameter from the parameters string dictionary
		''' </summary>
		''' <param name="ItemKey">Key name for item</param>
		''' <returns>String value associated with specified key</returns>
		''' <remarks>Returns Nothing if key isn't found</remarks>
		Public Function GetParam(ByVal ItemKey As String) As String Implements IMgrParams.GetParam

			Return m_ParamDictionary.Item(ItemKey)

		End Function

		''' <summary>
		''' Sets a parameter in the parameters string dictionary
		''' </summary>
		''' <param name="ItemKey">Key name for the item</param>
		''' <param name="ItemValue">Value to assign to the key</param>
		''' <remarks></remarks>
		Public Sub SetParam(ByVal ItemKey As String, ByVal ItemValue As String) Implements IMgrParams.SetParam

			m_ParamDictionary.Item(ItemKey) = ItemValue

		End Sub

		''' <summary>
		''' Writes an error message to application log or manager local log
		''' </summary>
		''' <param name="ErrMsg">Message to write</param>
		''' <param name="Logger">Logging object of logger has been created; otherwise NOTHING</param>
		''' <remarks></remarks>
		Private Sub WriteErrorMsg(ByVal ErrMsg As String, ByVal Logger As ILogger)

			If Logger Is Nothing Then
				clsEmergencyLog.WriteToLog(m_EmergencyLogSource, m_EmergencyLogName, ErrMsg)
			Else
				Logger.PostEntry(ErrMsg, ILogger.logMsgType.logError, True)
			End If

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
		''' Gets a collection representing all keys in the parameters string dictionary
		''' </summary>
		''' <returns></returns>
		''' <remarks></remarks>
		Public Function GetAllKeys() As ICollection

			Return m_ParamDictionary.Keys

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
			Catch ex As System.Exception
				m_ErrMsg = "clsMgrSettings.WriteConfigSettings; Exception updating settings file: " & ex.Message
				Return False
			End Try

		End Function

		''' <summary>
		''' Loads an app config file for changing parameters
		''' </summary>
		''' <returns>App config file as an XML document if successful; NOTHING on failure</returns>
		''' <remarks></remarks>
		Private Function LoadConfigDocument() As XmlDocument

			Dim MyDoc As XmlDocument = Nothing

			Try
				MyDoc = New XmlDocument
				MyDoc.Load(GetConfigFilePath)
				Return MyDoc
			Catch ex As System.Exception
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

			Return Application.ExecutablePath & ".config"

		End Function

#End Region

	End Class

End Namespace
