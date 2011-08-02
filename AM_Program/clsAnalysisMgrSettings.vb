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
Imports System.IO
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
        Public Const STEPTOOL_PARAMFILESTORAGEPATH_PREFIX As String = "StepTool_ParamFileStoragePath_"

        Private Const SP_NAME_ACKMANAGERUPDATE As String = "AckManagerUpdateRequired"

        Private m_ParamDictionary As System.Collections.Generic.Dictionary(Of String, String)
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
		''' Calls stored procedure AckManagerUpdateRequired in the Manager Control DB
		''' </summary>
		''' <remarks></remarks>
        Public Sub AckManagerUpdateRequired()

            Dim MyConnection As System.Data.SqlClient.SqlConnection
            Dim MyCmd As New System.Data.SqlClient.SqlCommand
            Dim RetVal As Integer
            Dim ConnectionString As String

            Try
                ConnectionString = Me.GetParam("MgrCnfgDbConnectStr")
                MyConnection = New System.Data.SqlClient.SqlConnection(ConnectionString)
                MyConnection.Open()

                'Set up the command object prior to SP execution
                With MyCmd
                    .CommandType = CommandType.StoredProcedure
                    .CommandText = SP_NAME_ACKMANAGERUPDATE
                    .Connection = MyConnection

                    .Parameters.Add(New SqlClient.SqlParameter("@Return", SqlDbType.Int))
                    .Parameters.Item("@Return").Direction = ParameterDirection.ReturnValue

                    .Parameters.Add(New SqlClient.SqlParameter("@managerName", SqlDbType.VarChar, 128))
                    .Parameters.Item("@managerName").Direction = ParameterDirection.Input
                    .Parameters.Item("@managerName").Value = Me.GetParam("MgrName")

                    .Parameters.Add(New SqlClient.SqlParameter("@message", SqlDbType.VarChar, 512))
                    .Parameters.Item("@message").Direction = ParameterDirection.Output
                    .Parameters.Item("@message").Value = ""
                End With

                'Execute the SP
                RetVal = MyCmd.ExecuteNonQuery

            Catch ex As System.Exception
                Dim strErrorMessage As String = "Exception calling " & SP_NAME_ACKMANAGERUPDATE
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, strErrorMessage & ex.Message)
            End Try

        End Sub

		''' <summary>
		''' Constructor
		''' </summary>
		''' <param name="EmergencyLogSource">Source name registered for emergency logging</param>
		''' <param name="EmergencyLogName">Name of system log for emergency logging</param>
		''' <remarks></remarks>
		Public Sub New(ByVal EmergencyLogSource As String, ByVal EmergencyLogName As String)
			m_EmergencyLogName = EmergencyLogName
			m_EmergencyLogSource = EmergencyLogSource

            If Not LoadSettings() Then
                Throw New ApplicationException("Unable to initialize manager settings class")
            End If

		End Sub

		''' <summary>
		''' Loads manager settings from config file and database
		''' </summary>
        ''' <returns>True if successful; False on error</returns>
		''' <remarks></remarks>
        Public Function LoadSettings() As Boolean

            m_ErrMsg = ""

            'If the param dictionary exists, it needs to be cleared out
            If Not m_ParamDictionary Is Nothing Then
                m_ParamDictionary.Clear()
                m_ParamDictionary = Nothing
            End If

            ' Note: When you are editing this project using the Visual Studio IDE, if you edit the values
            '  ->My Project>Settings.settings, then when you run the program (from within the IDE), then it
            '  will update file AnalysisManagerProg.exe.config with your settings
            ' The manager will exit if the "UsingDefaults" value is "True", thus you need to have 
            '  "UsingDefaults" be "False" to run (and/or debug) the application

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

            'Get settings from Manager Control DB and Broker DB
            If Not LoadDBSettings() Then
                ' Errors have already been logged; return False
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
        Private Function LoadMgrSettingsFromFile() As System.Collections.Generic.Dictionary(Of String, String)

            'Load initial settings into string dictionary for return
            Dim RetDict As New System.Collections.Generic.Dictionary(Of String, String)(StringComparer.CurrentCultureIgnoreCase)

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
        Private Function CheckInitialSettings(ByRef InpDict As System.Collections.Generic.Dictionary(Of String, String)) As Boolean

            Dim MyMsg As String

            'Verify manager settings dictionary exists
            If InpDict Is Nothing Then
                MyMsg = "clsMgrSettings.CheckInitialSettings(); Manager parameter string dictionary not found"
                clsEmergencyLog.WriteToLog(m_EmergencyLogSource, m_EmergencyLogName, MyMsg)
                Return False
            End If

            'Verify intact config file was found
            Dim strValue As String = String.Empty
            If Not InpDict.TryGetValue("UsingDefaults", strValue) Then
                MyMsg = "clsMgrSettings.CheckInitialSettings(); 'UsingDefaults' entry not found in Config file"
                clsEmergencyLog.WriteToLog(m_EmergencyLogSource, m_EmergencyLogName, MyMsg)
            Else
                Dim blnValue As Boolean
                If Boolean.TryParse(strValue, blnValue) Then
                    If blnValue Then
                        MyMsg = "clsMgrSettings.CheckInitialSettings(); Config file problem, contains UsingDefaults=True"
                        clsEmergencyLog.WriteToLog(m_EmergencyLogSource, m_EmergencyLogName, MyMsg)
                        Return False
                    End If
                End If
            End If

            'No problems found
            Return True

        End Function

        ' Retrieves the manager and global settings from various databases
        Public Function LoadDBSettings() As Boolean
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

            Dim RetryCount As Short = 3
            Dim MyMsg As String
            Dim ParamKey As String
            Dim ParamVal As String
            Dim ConnectionString As String = Me.GetParam("MgrCnfgDbConnectStr")

            Dim SqlStr As String = "SELECT ParameterName, ParameterValue FROM V_MgrParams " & _
                                   "WHERE ManagerName = '" & Me.GetParam("MgrName") & "'"

            Dim Dt As DataTable = Nothing
            Dim blnsuccess As Boolean

            'Get a table to hold the results of the query
            While RetryCount > 0
                Try
                    Using Cn As SqlConnection = New SqlConnection(ConnectionString)
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
                    MyMsg = "clsMgrSettings.LoadMgrSettingsFromDB; Exception getting manager settings from database: " & ex.Message & "; ConnectionString: " & ConnectionString
                    MyMsg &= ", RetryCount = " & RetryCount.ToString

                    WriteErrorMsg(MyMsg)

                    System.Threading.Thread.Sleep(5000)             'Delay for 5 second before trying again
                End Try
            End While

            'If loop exited due to errors, return false
            If RetryCount < 1 Then
                MyMsg = "clsMgrSettings.LoadMgrSettingsFromDB; Excessive failures attempting to retrieve manager settings from database"
                WriteErrorMsg(MyMsg)
                Dt.Dispose()
                Return False
            End If

            'Verify at least one row returned
            If Dt.Rows.Count < 1 Then
                ' No data was returned
                MyMsg = "clsMgrSettings.LoadMgrSettingsFromDB; Manager '" & Me.GetParam("MgrName") & "' not found using " & ConnectionString
                WriteErrorMsg(MyMsg)
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

                    Me.SetParam(ParamKey, ParamVal)
                Next
                blnsuccess = True
            Catch ex As System.Exception
                MyMsg = "clsMgrSettings.LoadMgrSettingsFromDB; Exception filling string dictionary from table: " & ex.Message
                WriteErrorMsg(MyMsg)
                blnsuccess = False
            Finally
                Dt.Dispose()
            End Try

            Return blnsuccess

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

            Dim SqlStr As String = " SELECT '" & STEPTOOL_PARAMFILESTORAGEPATH_PREFIX & "' + Name AS ParameterName, " & _
                                           "[Param File Storage Path] AS ParameterValue" & _
                                   " FROM V_Pipeline_Step_Tools_Detail_Report" & _
                                   " WHERE ISNULL([Param File Storage Path], '') <> ''"

            Dim Dt As DataTable = Nothing
            Dim blnsuccess As Boolean = False

            'Get a table to hold the results of the query
            While RetryCount > 0
                Try
                    Using Cn As SqlConnection = New SqlConnection(ConnectionString)
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
                    MyMsg = "clsMgrSettings.LoadBrokerDBSettings; Exception getting settings from broker database: " & ex.Message & "; ConnectionString: " & ConnectionString
                    MyMsg &= ", RetryCount = " & RetryCount.ToString

                    WriteErrorMsg(MyMsg)

                    System.Threading.Thread.Sleep(5000)             'Delay for 5 second before trying again
                End Try
            End While

            'If loop exited due to errors, return false
            If RetryCount < 1 Then
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
            Catch ex As System.Exception
                MyMsg = "clsMgrSettings.LoadBrokerDBSettings; Exception filling string dictionary from table: " & ex.Message
                WriteErrorMsg(MyMsg)
                blnsuccess = False
            Finally
                Dt.Dispose()
            End Try

            Return blnsuccess

        End Function

        ''' <summary>
        ''' Gets a parameter from the parameters string dictionary
        ''' </summary>
        ''' <param name="ItemKey">Key name for item</param>
        ''' <returns>String value associated with specified key</returns>
        ''' <remarks>Returns Nothing if key isn't found</remarks>
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
        ''' <param name="ErrMsg">Message to write</param>
        ''' <remarks></remarks>
        Private Sub WriteErrorMsg(ByVal ErrMsg As String)

            clsEmergencyLog.WriteToLog(m_EmergencyLogSource, m_EmergencyLogName, ErrMsg)
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, ErrMsg)

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
