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
Imports System.Reflection
Imports System.Runtime.InteropServices
Imports PRISM

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

    Private mParamDictionary As Dictionary(Of String, String)
    Private mErrMsg As String = ""

    Private ReadOnly mEmergencyLogSource As String
    Private ReadOnly mEmergencyLogName As String
    Private ReadOnly mMgrFolderPath As String

    Private ReadOnly mTraceMode As Boolean
#End Region

#Region "Properties"
    Public ReadOnly Property ErrMsg() As String Implements IMgrParams.ErrMsg
        Get
            Return mErrMsg
        End Get
    End Property
#End Region

#Region "Methods"
    ''' <summary>
    ''' Calls stored procedure AckManagerUpdateRequired in the Manager Control DB
    ''' </summary>
    ''' <remarks></remarks>
    Public Sub AckManagerUpdateRequired() Implements IMgrParams.AckManagerUpdateRequired

        Try

            ' Data Source=proteinseqs;Initial Catalog=manager_control
            Dim connectionString = Me.GetParam("MgrCnfgDbConnectStr")

            If mTraceMode = True Then ShowTraceMessage("AckManagerUpdateRequired using " & connectionString)

            Dim myConnection = New SqlConnection(connectionString)
            myConnection.Open()

            'Set up the command object prior to SP execution
            Dim myCmd = New SqlCommand(SP_NAME_ACKMANAGERUPDATE, myConnection)
            With myCmd
                .CommandType = CommandType.StoredProcedure

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
            myCmd.ExecuteNonQuery()

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
    Public Sub New(
       EmergencyLogSource As String,
       EmergencyLogName As String,
       lstMgrSettings As Dictionary(Of String, String),
       MgrFolderPath As String,
       traceMode As Boolean)

        mEmergencyLogName = EmergencyLogName
        mEmergencyLogSource = EmergencyLogSource
        mMgrFolderPath = MgrFolderPath
        mTraceMode = traceMode

        If Not LoadSettings(lstMgrSettings) Then
            If Not String.IsNullOrEmpty(mErrMsg) Then
                Throw New ApplicationException("Unable to initialize manager settings class: " & mErrMsg)
            Else
                Throw New ApplicationException("Unable to initialize manager settings class: unknown error")
            End If
        End If

        If mTraceMode = True Then ShowTraceMessage("Initialized clsAnalysisMgrSettings")

    End Sub

    ''' <summary>
    ''' Loads manager settings from config file and database
    ''' </summary>
    ''' <param name="ConfigFileSettings">Manager settings loaded from file AnalysisManagerProg.exe.config</param>
    ''' <returns>True if successful; False on error</returns>
    ''' <remarks></remarks>
    Public Function LoadSettings(ConfigFileSettings As Dictionary(Of String, String)) As Boolean Implements IMgrParams.LoadSettings

        mErrMsg = ""

        mParamDictionary = ConfigFileSettings

        'Test the settings retrieved from the config file
        If Not CheckInitialSettings(mParamDictionary) Then
            'Error logging handled by CheckInitialSettings
            Return False
        End If

        'Determine if manager is deactivated locally
        If Not CBool(mParamDictionary("MgrActive_Local")) Then
            WriteToEmergencyLog(mEmergencyLogSource, mEmergencyLogName, "Manager deactivated locally")
            mErrMsg = "Manager deactivated locally"
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
    Private Function CheckInitialSettings(InpDict As Dictionary(Of String, String)) As Boolean

        Dim errorMessage As String

        'Verify manager settings dictionary exists
        If InpDict Is Nothing Then
            errorMessage = "clsMgrSettings.CheckInitialSettings(); Manager parameter string dictionary not found"

            If mTraceMode = True Then ShowTraceMessage("Error in " & errorMessage)

            WriteToEmergencyLog(mEmergencyLogSource, mEmergencyLogName, errorMessage)
            Return False
        End If

        'Verify intact config file was found
        Dim strValue As String = String.Empty
        If Not InpDict.TryGetValue("UsingDefaults", strValue) Then
            errorMessage = "clsMgrSettings.CheckInitialSettings(); 'UsingDefaults' entry not found in Config file"

            If mTraceMode = True Then ShowTraceMessage("Error in " & errorMessage)

            WriteToEmergencyLog(mEmergencyLogSource, mEmergencyLogName, errorMessage)
        Else
            Dim blnValue As Boolean
            If Boolean.TryParse(strValue, blnValue) Then
                If blnValue Then
                    errorMessage = "clsMgrSettings.CheckInitialSettings(); Config file problem, contains UsingDefaults=True"

                    If mTraceMode = True Then ShowTraceMessage("Error in " & errorMessage)

                    WriteToEmergencyLog(mEmergencyLogSource, mEmergencyLogName, errorMessage)
                    Return False
                End If
            End If
        End If

        'No problems found
        Return True

    End Function

    Private Function GetGroupNameFromSettings(dtSettings As DataTable) As String

        For Each oRow As DataRow In dtSettings.Rows
            'Add the column heading and value to the dictionary
            Dim paramKey As String = DbCStr(oRow(dtSettings.Columns("ParameterName")))

            If clsGlobal.IsMatch(paramKey, "MgrSettingGroupName") Then
                Dim groupName As String = DbCStr(oRow(dtSettings.Columns("ParameterValue")))
                If Not String.IsNullOrWhiteSpace(groupName) Then
                    Return groupName
                Else
                    Return String.Empty
                End If
            End If

        Next
        Return String.Empty

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
            mErrMsg = "MgrName parameter not found in m_ParamDictionary; it should be defined in the AnalysisManagerProg.exe.config file"

            If mTraceMode = True Then ShowTraceMessage("Error in LoadMgrSettingsFromDB: " & mErrMsg)

            Return False
        End If

        blnReturnErrorIfNoParameters = True
        blnSuccess = LoadMgrSettingsFromDBWork(ManagerName, dtSettings, blnReturnErrorIfNoParameters)
        If Not blnSuccess Then
            Return False
        End If

        blnSkipExistingParameters = False
        blnSuccess = StoreParameters(dtSettings, blnSkipExistingParameters, ManagerName)
        If Not blnSuccess Then Return False

        While blnSuccess

            strMgrSettingsGroup = GetGroupNameFromSettings(dtSettings)
            If String.IsNullOrEmpty(strMgrSettingsGroup) Then
                Exit While
            End If

            ' This manager has group-based settings defined; load them now

            blnReturnErrorIfNoParameters = False
            dtSettings = Nothing
            blnSuccess = LoadMgrSettingsFromDBWork(strMgrSettingsGroup, dtSettings, blnReturnErrorIfNoParameters)

            If blnSuccess Then
                blnSkipExistingParameters = True
                blnSuccess = StoreParameters(dtSettings, blnSkipExistingParameters, ManagerName)
            End If

        End While

        Return blnSuccess

    End Function

    Private Function LoadMgrSettingsFromDBWork(ManagerName As String, <Out()> ByRef dtSettings As DataTable, blnReturnErrorIfNoParameters As Boolean) As Boolean

        Const retryCount As Short = 3

        ' Data Source=proteinseqs;Initial Catalog=manager_control
        Dim connectionString As String = Me.GetParam("MgrCnfgDbConnectStr", "")

        dtSettings = Nothing

        If String.IsNullOrEmpty(ManagerName) Then
            mErrMsg = "MgrCnfgDbConnectStr parameter not found in m_ParamDictionary; it should be defined in the AnalysisManagerProg.exe.config file"
            If mTraceMode = True Then ShowTraceMessage("LoadMgrSettingsFromDBWork: " & mErrMsg)
            Return False
        End If

        If mTraceMode = True Then ShowTraceMessage("LoadMgrSettingsFromDBWork using [" & connectionString & "] for manager " & ManagerName)

        Dim SqlStr As String = "SELECT ParameterName, ParameterValue FROM V_MgrParams WHERE ManagerName = '" & ManagerName & "'"

        'Get a table to hold the results of the query
        Dim blnSuccess = clsGlobal.GetDataTableByQuery(SqlStr, connectionString, "LoadMgrSettingsFromDBWork", retryCount, dtSettings)

        ' If unable to retrieve the data, return false
        If Not blnSuccess Then
            ' Log the message to the DB if the monthly Windows updates are not pending
            Dim allowLogToDB = Not (clsWindowsUpdateStatus.ServerUpdatesArePending())

            mErrMsg = "clsMgrSettings.LoadMgrSettingsFromDBWork; Excessive failures attempting to retrieve manager settings from database for manager '" & ManagerName & "'"
            WriteErrorMsg(mErrMsg, allowLogToDB)

            If Not dtSettings Is Nothing Then dtSettings.Dispose()
            Return False
        End If

        ' Verify at least one row returned
        If dtSettings.Rows.Count < 1 And blnReturnErrorIfNoParameters Then
            ' No data was returned
            mErrMsg = "clsMgrSettings.LoadMgrSettingsFromDBWork; Manager '" & ManagerName & "' not defined in the manager control database; using " & connectionString
            WriteErrorMsg(mErrMsg)
            dtSettings.Dispose()
            Return False
        End If

        Return True

    End Function

    Private Function StoreParameters(dtSettings As DataTable, blnSkipExisting As Boolean, ManagerName As String) As Boolean

        Dim blnSuccess As Boolean

        'Fill a string dictionary with the manager parameters that have been found
        Try
            For Each oRow As DataRow In dtSettings.Rows
                'Add the column heading and value to the dictionary
                Dim paramKey As String = DbCStr(oRow(dtSettings.Columns("ParameterName")))
                Dim paramVal As String = DbCStr(oRow(dtSettings.Columns("ParameterValue")))

                If mParamDictionary.ContainsKey(paramKey) Then
                    If Not blnSkipExisting Then
                        mParamDictionary(paramKey) = paramVal
                    End If
                Else
                    mParamDictionary.Add(paramKey, paramVal)
                End If
            Next
            blnSuccess = True

        Catch ex As Exception
            mErrMsg = "clsAnalysisMgrSettings.StoreParameters; Exception filling string dictionary from table for manager '" & ManagerName & "': " & ex.Message
            WriteErrorMsg(mErrMsg)
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
        Dim connectionString As String = Me.GetParam("brokerconnectionstring")   ' Gigasax.DMS_Pipeline

        If mTraceMode = True Then ShowTraceMessage("LoadBrokerDBSettings has brokerconnectionstring = " & connectionString)

        ' Construct the Sql to obtain the information:
        '   SELECT 'StepTool_ParamFileStoragePath_' + Name AS ParameterName, [Param File Storage Path] AS ParameterValue
        '   FROM V_Pipeline_Step_Tools_Detail_Report
        '   WHERE ISNULL([Param File Storage Path], '') <> ''
        '
        Const SqlStr As String = " SELECT '" & clsGlobal.STEPTOOL_PARAMFILESTORAGEPATH_PREFIX & "' + Name AS ParameterName, " &
                                 " [Param File Storage Path] AS ParameterValue" &
                                 " FROM V_Pipeline_Step_Tools_Detail_Report" &
                                 " WHERE ISNULL([Param File Storage Path], '') <> ''"

        Dim Dt As DataTable = Nothing
        Dim blnSuccess As Boolean

        If mTraceMode = True Then ShowTraceMessage("Query V_Pipeline_Step_Tools_Detail_Report in broker")

        'Get a table to hold the results of the query
        blnSuccess = clsGlobal.GetDataTableByQuery(SqlStr.ToString(), connectionString, "LoadBrokerDBSettings", RetryCount, Dt)

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
            MyMsg = "clsMgrSettings.LoadBrokerDBSettings; V_Pipeline_Step_Tools_Detail_Report returned no rows using " & connectionString
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
            blnSuccess = True
        Catch ex As Exception
            MyMsg = "clsMgrSettings.LoadBrokerDBSettings; Exception filling string dictionary from table: " & ex.Message
            WriteErrorMsg(MyMsg)
            blnSuccess = False
        Finally
            Dt.Dispose()
        End Try

        Return blnSuccess

    End Function

    ''' <summary>
    ''' Gets a parameter from the manager parameters dictionary
    ''' </summary>
    ''' <param name="ItemKey">Key name for item</param>
    ''' <returns>String value associated with specified key</returns>
    ''' <remarks>Returns empty string if key isn't found</remarks>
    Public Function GetParam(ItemKey As String) As String Implements IMgrParams.GetParam
        Dim Value As String = String.Empty

        If Not mParamDictionary Is Nothing Then
            If mParamDictionary.TryGetValue(ItemKey, Value) Then
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
    Public Function GetParam(ItemKey As String, ValueIfMissing As Boolean) As Boolean Implements IMgrParams.GetParam
        Return clsGlobal.CBoolSafe(GetParam(ItemKey), ValueIfMissing)
    End Function

    ''' <summary>
    ''' Gets a parameter from the manager parameters dictionary
    ''' </summary>
    ''' <param name="ItemKey">Key name for item</param>
    ''' <param name="ValueIfMissing">Value to return if the parameter is not found</param>
    ''' <returns>Value for specified parameter; ValueIfMissing if not found</returns>
    Public Function GetParam(ItemKey As String, ValueIfMissing As Integer) As Integer Implements IMgrParams.GetParam
        Return clsGlobal.CIntSafe(GetParam(ItemKey), ValueIfMissing)
    End Function

    ''' <summary>
    ''' Gets a parameter from the manager parameters dictionary
    ''' </summary>
    ''' <param name="ItemKey">Key name for item</param>
    ''' <param name="ValueIfMissing">Value to return if the parameter is not found</param>
    ''' <returns>Value for specified parameter; ValueIfMissing if not found</returns>
    Public Function GetParam(ItemKey As String, ValueIfMissing As String) As String Implements IMgrParams.GetParam
        Dim strValue As String
        strValue = GetParam(ItemKey)
        If String.IsNullOrEmpty(strValue) Then
            Return ValueIfMissing
        Else
            Return strValue
        End If
    End Function

    Private Shared Sub ShowTraceMessage(strMessage As String)
        Console.WriteLine(DateTime.Now.ToString("hh:mm:ss.fff tt") & ": " & strMessage)
    End Sub

    ''' <summary>
    ''' Sets a parameter in the parameters string dictionary
    ''' </summary>
    ''' <param name="ItemKey">Key name for the item</param>
    ''' <param name="ItemValue">Value to assign to the key</param>
    ''' <remarks></remarks>
    Public Sub SetParam(ItemKey As String, ItemValue As String) Implements IMgrParams.SetParam

        If mParamDictionary.ContainsKey(ItemKey) Then
            mParamDictionary(ItemKey) = ItemValue
        Else
            mParamDictionary.Add(ItemKey, ItemValue)
        End If

    End Sub

    ''' <summary>
    ''' Writes an error message to the application log and the database
    ''' </summary>
    ''' <param name="errorMessage">Message to write</param>
    ''' <remarks></remarks>
    Private Sub WriteErrorMsg(errorMessage As String, Optional allowLogToDB As Boolean = True)

        WriteToEmergencyLog(mEmergencyLogSource, mEmergencyLogName, errorMessage)
        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, errorMessage)

        If (allowLogToDB) Then
            ' Also post a log to the database
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, errorMessage)
        End If

        If mTraceMode Then
            ShowTraceMessage(errorMessage)
        End If

    End Sub

    ''' <summary>
    ''' Converts a database output object that could be dbNull to a string
    ''' </summary>
    ''' <param name="InpObj"></param>
    ''' <returns>String equivalent of object; empty string if object is dbNull</returns>
    ''' <remarks></remarks>
    Protected Function DbCStr(InpObj As Object) As String

        'If input object is DbNull, returns "", otherwise returns String representation of object
        If InpObj Is DBNull.Value Then
            Return String.Empty
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
    Public Function WriteConfigSetting(Key As String, Value As String) As Boolean

        mErrMsg = ""

        'Load the config document
        Dim MyDoc As XmlDocument = LoadConfigDocument()
        If MyDoc Is Nothing Then
            'Error message has already been produced by LoadConfigDocument
            Return False
        End If

        'Retrieve the settings node
        Dim MyNode As XmlNode = MyDoc.SelectSingleNode("//applicationSettings")

        If MyNode Is Nothing Then
            mErrMsg = "clsMgrSettings.WriteConfigSettings; appSettings node not found"
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
                mErrMsg = "clsMgrSettings.WriteConfigSettings; specified key not found: " & Key
                Return False
            End If
            MyDoc.Save(GetConfigFilePath())
            Return True
        Catch ex As Exception
            mErrMsg = "clsMgrSettings.WriteConfigSettings; Exception updating settings file: " & ex.Message
            Return False
        End Try

    End Function

    Protected Sub WriteToEmergencyLog(SourceName As String, LogName As String, Message As String)
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
            mErrMsg = "clsMgrSettings.LoadConfigDocument; Exception loading settings file: " & ex.Message
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
        Return Path.Combine(mMgrFolderPath, exeName & ".config")

    End Function

#End Region

End Class
