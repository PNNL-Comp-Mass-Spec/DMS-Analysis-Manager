'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2007, Battelle Memorial Institute
' Created 12/18/2007
'
'*********************************************************************************************************

Option Strict On

Imports System.Data.SqlClient
Imports System.IO
Imports System.Xml
Imports System.Text

''' <summary>
''' Provides DB access and tools for one analysis job
''' </summary>
''' <remarks></remarks>
Public Class clsAnalysisJob
    Inherits clsDBTask
    Implements IJobParams

#Region "Constants"
    Protected Const SP_NAME_SET_COMPLETE As String = "SetStepTaskComplete"
    Protected Const SP_NAME_REQUEST_TASK As String = "RequestStepTaskXML"   '"RequestStepTask"
#End Region

#Region "Module variables"
    ' The outer dictionary tracks section names, then the inner dictionary tracks key/value pairs within each section
    Protected m_JobParams As Dictionary(Of String, Dictionary(Of String, String))

    Protected m_JobId As Integer
    Protected m_TaskWasClosed As Boolean

    Protected m_ResultFilesToSkip As New SortedSet(Of String)(StringComparer.CurrentCultureIgnoreCase)              ' List of file names to NOT move to the result folder; this list is used by MoveResultFiles()
    Protected m_ResultFileExtensionsToSkip As New SortedSet(Of String)(StringComparer.CurrentCultureIgnoreCase)     ' List of file extensions (or even partial file names like _peaks.txt) to NOT move to the result folder; comparison checks if the end of the fileName matches any entry ResultFileExtensionsToSkip: If TmpFileNameLcase.EndsWith(ext.ToLower()) Then OkToMove = False
    Protected m_ResultFilesToKeep As New SortedSet(Of String)(StringComparer.CurrentCultureIgnoreCase)              ' List of file names that WILL be moved to the result folder, even if they are in ResultFilesToSkip or ResultFileExtensionsToSkip
    Protected m_ServerFilesToDelete As New SortedSet(Of String)(StringComparer.CurrentCultureIgnoreCase)            ' List of file path to delete from the storage server (must be full file paths)

    Protected m_DatasetInfoList As New Dictionary(Of String, Integer)(StringComparer.CurrentCultureIgnoreCase)         ' List of dataset names and dataset IDs

#End Region

#Region "Properties"

    ''' <summary>
    '''  List of dataset names and dataset IDs associated with this aggregation job
    ''' </summary>
    ''' <value></value>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public ReadOnly Property DatasetInfoList As Dictionary(Of String, Integer) Implements IJobParams.DatasetInfoList
        Get
            Return m_DatasetInfoList
        End Get
    End Property

    ''' <summary>
    ''' List of file names that WILL be moved to the result folder, even if they are in ResultFilesToSkip or ResultFileExtensionsToSkip
    ''' </summary>
    ''' <value></value>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public ReadOnly Property ResultFilesToKeep As SortedSet(Of String) Implements IJobParams.ResultFilesToKeep
        Get
            Return m_ResultFilesToKeep
        End Get
    End Property

    ''' <summary>
    ''' List of file names to NOT move to the result folder
    ''' </summary>
    ''' <value></value>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public ReadOnly Property ResultFilesToSkip As SortedSet(Of String) Implements IJobParams.ResultFilesToSkip
        Get
            Return m_ResultFilesToSkip
        End Get
    End Property

    ''' <summary>
    ''' List of file extensions to NOT move to the result folder; comparison checks if the end of the fileName matches any entry in ResultFileExtensionsToSkip
    ''' </summary>
    ''' <value></value>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public ReadOnly Property ResultFileExtensionsToSkip As SortedSet(Of String) Implements IJobParams.ResultFileExtensionsToSkip
        Get
            Return m_ResultFileExtensionsToSkip
        End Get
    End Property

    ''' <summary>
    ''' List of file paths to remove from the storage server (full file paths)
    ''' </summary>
    ''' <value></value>
    ''' <returns></returns>
    ''' <remarks>Used by clsAnalysisToolRunnerBase.RemoveNonResultServerFiles</remarks>
    Public ReadOnly Property ServerFilesToDelete As SortedSet(Of String) Implements IJobParams.ServerFilesToDelete
        Get
            Return m_ServerFilesToDelete
        End Get
    End Property
#End Region

#Region "Methods"

    ''' <summary>
    ''' Constructor
    ''' </summary>
    ''' <param name="mgrParams">IMgrParams object containing manager parameters</param>
    ''' <remarks></remarks>
    Public Sub New(mgrParams As IMgrParams, DebugLvl As Integer)

        MyBase.New(mgrParams, DebugLvl)

        Reset()

    End Sub

    ''' <summary>
    ''' Adds (or updates) a job parameter
    ''' </summary>
    ''' <param name="SectionName">Section name for parameter</param>
    ''' <param name="ParamName">Name of parameter</param>
    ''' <param name="ParamValue">Boolean value for parameter</param>
    ''' <returns>True if success, False if an error</returns>
    ''' <remarks></remarks>
    Public Function AddAdditionalParameter(SectionName As String, ParamName As String, ParamValue As Boolean) As Boolean Implements IJobParams.AddAdditionalParameter

        Try

            SetParam(SectionName, ParamName, ParamValue.ToString())

            Return True
        Catch ex As Exception
            Dim Msg As String = "Exception adding parameter: " & ParamName & " Value: " & ParamValue & "; " & clsGlobal.GetExceptionStackTrace(ex)
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
            Return False
        End Try

    End Function

    ''' <summary>
    ''' Adds (or updates) a job parameter
    ''' </summary>
    ''' <param name="SectionName">Section name for parameter</param>
    ''' <param name="ParamName">Name of parameter</param>
    ''' <param name="ParamValue">Value for parameter</param>
    ''' <returns>True if success, False if an error</returns>
    ''' <remarks></remarks>
    Public Function AddAdditionalParameter(SectionName As String, ParamName As String, ParamValue As String) As Boolean Implements IJobParams.AddAdditionalParameter

        Try
            If ParamValue Is Nothing Then ParamValue = String.Empty

            SetParam(SectionName, ParamName, ParamValue)

            Return True
        Catch ex As Exception
            Dim Msg As String = "Exception adding parameter: " & ParamName & " Value: " & ParamValue & "; " & clsGlobal.GetExceptionStackTrace(ex)
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
            Return False
        End Try

    End Function

    ''' <summary>
    ''' Add new dataset name and ID to DatasetInfoList
    ''' </summary>
    ''' <param name="datasetName"></param>
    ''' <param name="datasetID"></param>
    ''' <remarks></remarks>
    Public Sub AddDatasetInfo(datasetName As String, datasetID As Integer) Implements IJobParams.AddDatasetInfo
        If String.IsNullOrWhiteSpace(datasetName) Then Exit Sub
        If Not m_DatasetInfoList.ContainsKey(datasetName) Then
            m_DatasetInfoList.Add(datasetName, datasetID)
        End If
    End Sub

    ''' <summary>
    ''' Add a fileName extension to not move to the results folder
    ''' </summary>
    ''' <param name="fileExtension"></param>
    ''' <remarks>Can be a file extension (like .raw) or even a partial file name like _peaks.txt</remarks>
    Public Sub AddResultFileExtensionToSkip(fileExtension As String) Implements IJobParams.AddResultFileExtensionToSkip
        If String.IsNullOrWhiteSpace(fileExtension) Then Exit Sub

        If Not m_ResultFileExtensionsToSkip.Contains(fileExtension) Then
            m_ResultFileExtensionsToSkip.Add(fileExtension)
        End If
    End Sub

    ''' <summary>
    ''' Add a fileName to definitely move to the results folder
    ''' </summary>
    ''' <param name="fileName"></param>
    ''' <remarks></remarks>
    Public Sub AddResultFileToKeep(fileName As String) Implements IJobParams.AddResultFileToKeep
        If String.IsNullOrWhiteSpace(fileName) Then Exit Sub

        fileName = Path.GetFileName(fileName)
        If Not m_ResultFilesToKeep.Contains(fileName) Then
            m_ResultFilesToKeep.Add(fileName)
        End If
    End Sub

    ''' <summary>
    ''' Add a fileName to not move to the results folder
    ''' </summary>
    ''' <param name="fileName"></param>
    ''' <remarks></remarks>
    Public Sub AddResultFileToSkip(fileName As String) Implements IJobParams.AddResultFileToSkip
        If String.IsNullOrWhiteSpace(fileName) Then Exit Sub

        fileName = Path.GetFileName(fileName)
        If Not m_ResultFilesToSkip.Contains(fileName) Then
            m_ResultFilesToSkip.Add(fileName)
        End If
    End Sub

    ''' <summary>
    ''' Add a file to be deleted from the storage server (requires full file path)
    ''' </summary>
    ''' <param name="filePath">Full path to the file</param>
    ''' <remarks></remarks>
    Public Sub AddServerFileToDelete(filePath As String) Implements IJobParams.AddServerFileToDelete
        If String.IsNullOrWhiteSpace(filePath) Then Exit Sub

        If Not m_ServerFilesToDelete.Contains(filePath) Then
            m_ServerFilesToDelete.Add(filePath)
        End If
    End Sub

    ''' <summary>
    ''' Gets a job parameter with the given name (in any parameter section)
    ''' </summary>
    ''' <param name="Name">Key name for parameter</param>
    ''' <returns>Value for specified parameter; ValueIfMissing if not found</returns>
    ''' <remarks>If the value associated with the parameter is found, yet is not True or False, then an exception will be occur; the calling procedure must handle this exception</remarks>
    Public Function GetJobParameter(Name As String, ValueIfMissing As Boolean) As Boolean Implements IJobParams.GetJobParameter

        Dim strValue As String

        Try
            strValue = GetParam(Name)

            If String.IsNullOrEmpty(strValue) Then
                Return ValueIfMissing
            End If

        Catch
            Return ValueIfMissing
        End Try

        ' Note: if strValue is not True or False, this will throw an exception; the calling procedure will need to handle that exception
        Return CBool(strValue)

    End Function

    ''' <summary>
    ''' Gets a job parameter with the given name (in any parameter section)
    ''' </summary>
    ''' <param name="Name">Key name for parameter</param>
    ''' <returns>Value for specified parameter; ValueIfMissing if not found</returns>
    Public Function GetJobParameter(Name As String, ValueIfMissing As String) As String Implements IJobParams.GetJobParameter

        Dim strValue As String

        Try
            strValue = GetParam(Name)

            If String.IsNullOrEmpty(strValue) Then
                Return ValueIfMissing
            End If

        Catch
            Return ValueIfMissing
        End Try

        Return strValue
    End Function

    ''' <summary>
    ''' Gets a job parameter with the given name (in any parameter section)
    ''' </summary>
    ''' <param name="Name">Key name for parameter</param>
    ''' <returns>Value for specified parameter; ValueIfMissing if not found</returns>
    Public Function GetJobParameter(Name As String, ValueIfMissing As Integer) As Integer Implements IJobParams.GetJobParameter
        Dim strValue As String

        Try
            strValue = GetParam(Name)

            If String.IsNullOrEmpty(strValue) Then
                Return ValueIfMissing
            End If

        Catch
            Return ValueIfMissing
        End Try

        ' Note: if strValue is not a number, this will throw an exception; the calling procedure will need to handle that exception
        Return CInt(strValue)

    End Function

    ''' <summary>
    ''' Gets a job parameter with the given name (in any parameter section)
    ''' </summary>
    ''' <param name="Name">Key name for parameter</param>
    ''' <returns>Value for specified parameter; ValueIfMissing if not found</returns>
    Public Function GetJobParameter(Name As String, ValueIfMissing As Short) As Short Implements IJobParams.GetJobParameter
        Return CShort(GetJobParameter(Name, CInt(ValueIfMissing)))
    End Function

    ''' <summary>
    ''' Gets a job parameter with the given name (in any parameter section)
    ''' </summary>
    ''' <param name="Name">Key name for parameter</param>
    ''' <returns>Value for specified parameter; ValueIfMissing if not found</returns>
    Public Function GetJobParameter(Name As String, ValueIfMissing As Single) As Single Implements IJobParams.GetJobParameter
        Return clsGlobal.CSngSafe(GetParam(Name), ValueIfMissing)
    End Function

    ''' <summary>
    ''' Gets a job parameter with the given name, preferentially using the specified parameter section
    ''' </summary>
    ''' <param name="Section">Section name for parameter</param>
    ''' <param name="Name">Key name for parameter</param>
    ''' <param name="ValueIfMissing">Value to return if the parameter is not found</param>
    ''' <returns>Value for specified parameter; ValueIfMissing if not found</returns>
    Public Function GetJobParameter(Section As String, Name As String, ValueIfMissing As Boolean) As Boolean Implements IJobParams.GetJobParameter
        Return clsGlobal.CBoolSafe(GetParam(Section, Name), ValueIfMissing)
    End Function

    ''' <summary>
    ''' Gets a job parameter with the given name, preferentially using the specified parameter section
    ''' </summary>
    ''' <param name="Section">Section name for parameter</param>
    ''' <param name="Name">Key name for parameter</param>
    ''' <param name="ValueIfMissing">Value to return if the parameter is not found</param>
    ''' <returns>Value for specified parameter; ValueIfMissing if not found</returns>
    Public Function GetJobParameter(Section As String, Name As String, ValueIfMissing As Integer) As Integer Implements IJobParams.GetJobParameter
        Return clsGlobal.CIntSafe(GetParam(Section, Name), ValueIfMissing)
    End Function

    ''' <summary>
    ''' Gets a job parameter with the given name, preferentially using the specified parameter section
    ''' </summary>
    ''' <param name="Section">Section name for parameter</param>
    ''' <param name="Name">Key name for parameter</param>
    ''' <param name="ValueIfMissing">Value to return if the parameter is not found</param>
    ''' <returns>Value for specified parameter; ValueIfMissing if not found</returns>
    Public Function GetJobParameter(Section As String, Name As String, ValueIfMissing As String) As String Implements IJobParams.GetJobParameter
        Dim strValue As String
        strValue = GetParam(Section, Name)
        If String.IsNullOrEmpty(strValue) Then
            Return ValueIfMissing
        Else
            Return strValue
        End If
    End Function

    ''' <summary>
    ''' Gets a job parameter with the given name, preferentially using the specified parameter section
    ''' </summary>
    ''' <param name="Section">Section name for parameter</param>
    ''' <param name="Name">Key name for parameter</param>
    ''' <param name="ValueIfMissing">Value to return if the parameter is not found</param>
    ''' <returns>Value for specified parameter; ValueIfMissing if not found</returns>
    Public Function GetJobParameter(Section As String, Name As String, ValueIfMissing As Single) As Single Implements IJobParams.GetJobParameter
        Return clsGlobal.CSngSafe(GetParam(Section, Name), ValueIfMissing)
    End Function

    ''' <summary>
    ''' Gets a job parameter with the given name (in any parameter section)
    ''' </summary>
    ''' <param name="Name">Key name for parameter</param>
    ''' <returns>Value for specified parameter; empty string if not found</returns>
    ''' <remarks></remarks>
    Public Function GetParam(Name As String) As String Implements IJobParams.GetParam

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
    Public Function GetParam(Section As String, Name As String) As String Implements IJobParams.GetParam

        Dim strValue As String = String.Empty

        If String.IsNullOrEmpty(Name) Then
            ' User actually wanted to look for the parameter that is currently in the Section Variable, using an empty string as the default value
            Return GetParam(Section)
        End If

        If TryGetParam(Section, Name, strValue) Then
            Return strValue
        Else
            Return String.Empty
        End If

    End Function

    Public Shared Function JobParametersFilename(jobNum As String) As String
        Return clsGlobal.XML_FILENAME_PREFIX & jobNum & "." & clsGlobal.XML_FILENAME_EXTENSION
    End Function

    ''' <summary>
    ''' Add/updates the value for the given parameter (searches all sections)
    ''' </summary>
    ''' <param name="ParamName">Parameter name</param>
    ''' <param name="ParamValue">Parameter value</param>
    ''' <remarks></remarks>
    Public Sub SetParam(ParamName As String, ParamValue As String) Implements IJobParams.SetParam

        Dim blnMatchFound As Boolean

        If ParamValue Is Nothing Then ParamValue = String.Empty

        For Each oSection In m_JobParams
            If oSection.Value.ContainsKey(ParamName) Then
                oSection.Value(ParamName) = ParamValue
                blnMatchFound = True
            End If
        Next

        If Not blnMatchFound AndAlso m_JobParams.Count > 0 Then
            ' Add the parameter to the first section
            m_JobParams.First.Value.Add(ParamName, ParamValue)
        End If

    End Sub

    ''' <summary>
    ''' Add/updates the value for the given parameter
    ''' </summary>
    ''' <param name="Section">Section name</param>
    ''' <param name="ParamName">Parameter name</param>
    ''' <param name="ParamValue">Parameter value</param>
    ''' <remarks></remarks>
    Public Sub SetParam(Section As String, ParamName As String, ParamValue As String) Implements IJobParams.SetParam

        Dim oParams As Dictionary(Of String, String) = Nothing

        If Not m_JobParams.TryGetValue(Section, oParams) Then
            ' Need to add a section with a blank name
            oParams = New Dictionary(Of String, String)(StringComparer.CurrentCultureIgnoreCase)
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
    Public Function TryGetParam(ParamName As String, ByRef ParamValue As String) As Boolean

        ParamValue = String.Empty

        If Not m_JobParams Is Nothing Then
            For Each oEntry As KeyValuePair(Of String, Dictionary(Of String, String)) In m_JobParams
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
    Public Function TryGetParam(Section As String, ParamName As String, ByRef ParamValue As String) As Boolean
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
    Public Function TryGetParam(Section As String, ParamName As String, ByRef ParamValue As String, SearchAllSectionsIfNotFound As Boolean) As Boolean

        Dim oParams As Dictionary(Of String, String) = Nothing
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

    ''' <summary>
    ''' Remove a fileName that was previously added to ResultFilesToSkip
    ''' </summary>
    ''' <param name="fileName"></param>
    ''' <remarks></remarks>
    Public Sub RemoveResultFileToSkip(fileName As String) Implements IJobParams.RemoveResultFileToSkip

        If m_ResultFilesToSkip.Contains(fileName) Then
            m_ResultFilesToSkip.Remove(fileName)
        End If

    End Sub

    Private Function RemoveXmlSection(paramXml As String, sectionName As String) As String

        Try

            Dim doc = New XmlDocument()

            doc.LoadXml(paramXml)

            Dim lstSections = doc.GetElementsByTagName("section")

            Dim intindex = 0
            While intindex < lstSections.Count

                Dim section As XmlNode = lstSections(intindex)

                Dim removeNode = (From item As XmlAttribute In section.Attributes.Cast(Of XmlAttribute)()
                   Where item.Name = "name" And item.Value = sectionName
                   Select item).ToList().Any()

                If removeNode Then
                    section.ParentNode.RemoveChild(section)
                Else
                    intindex += 1
                End If
            End While

            Dim sbOutput = New StringBuilder

            Dim settings = New XmlWriterSettings()
            settings.Indent = True
            settings.IndentChars = "  "
            settings.OmitXmlDeclaration = True

            Using writer As XmlWriter = XmlWriter.Create(sbOutput, settings)
                doc.Save(writer)
            End Using

            Dim filteredXML = sbOutput.ToString()

            Return filteredXML

        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error in RemoveXmlSection: " & ex.Message)
            Return paramXml
        End Try

    End Function

    ''' <summary>
    ''' Requests a task from the database
    ''' </summary>
    ''' <returns>Enum indicating if task was found</returns>
    ''' <remarks></remarks>
    Public Overrides Function RequestTask() As RequestTaskResult Implements IJobParams.RequestTask

        Dim RetVal As RequestTaskResult

        RetVal = RequestAnalysisJob()
        Select Case RetVal
            Case RequestTaskResult.NoTaskFound
                m_TaskWasAssigned = False
            Case RequestTaskResult.TaskFound
                m_TaskWasAssigned = True
            Case Else
                m_TaskWasAssigned = False
        End Select
        Return RetVal

    End Function

    ''' <summary>
    ''' Requests a single analysis job using RequestStepTaskXML
    ''' </summary>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Private Function RequestAnalysisJob() As RequestTaskResult

        Dim Outcome As RequestTaskResult
        Dim RetVal As Integer
        Dim paramXml As String

        Dim strProductVersion As String = clsGlobal.GetAssemblyVersion()
        If strProductVersion Is Nothing Then strProductVersion = "??"

        Reset()

        Try
            'Set up the command object prior to SP execution
            Dim myCmd = New SqlCommand(SP_NAME_REQUEST_TASK)
            With myCmd
                .CommandType = CommandType.StoredProcedure

                .Parameters.Add(New SqlParameter("@Return", SqlDbType.Int))
                .Parameters.Item("@Return").Direction = ParameterDirection.ReturnValue

                .Parameters.Add(New SqlParameter("@processorName", SqlDbType.VarChar, 128))
                .Parameters.Item("@processorName").Direction = ParameterDirection.Input
                .Parameters.Item("@processorName").Value = m_MgrParams.GetParam("MgrName")

                .Parameters.Add(New SqlParameter("@jobNumber", SqlDbType.Int))
                .Parameters.Item("@jobNumber").Direction = ParameterDirection.Output

                .Parameters.Add(New SqlParameter("@parameters", SqlDbType.VarChar, 8000))
                .Parameters.Item("@parameters").Direction = ParameterDirection.Output
                .Parameters.Item("@parameters").Value = ""

                .Parameters.Add(New SqlParameter("@message", SqlDbType.VarChar, 512))
                .Parameters.Item("@message").Direction = ParameterDirection.Output
                .Parameters.Item("@message").Value = ""

                .Parameters.Add(New SqlParameter("@infoOnly", SqlDbType.TinyInt))
                .Parameters.Item("@infoOnly").Direction = ParameterDirection.Input
                .Parameters.Item("@infoOnly").Value = 0

                .Parameters.Add(New SqlParameter("@AnalysisManagerVersion", SqlDbType.VarChar, 128))
                .Parameters.Item("@AnalysisManagerVersion").Direction = ParameterDirection.Input
                .Parameters.Item("@AnalysisManagerVersion").Value = strProductVersion
            End With

            If m_DebugLevel > 4 Then
                Dim MyMsg As String = "clsAnalysisJob.RequestAnalysisJob(), connection string: " & m_BrokerConnStr
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, MyMsg)
                MyMsg = "clsAnalysisJob.RequestAnalysisJob(), printing param list"
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, MyMsg)
                PrintCommandParams(myCmd)
            End If

            ' Execute the SP
            RetVal = PipelineDBProcedureExecutor.ExecuteSP(myCmd, 1)

            Select Case RetVal
                Case RET_VAL_OK
                    'No errors found in SP call, so see if any step tasks were found
                    m_JobId = CInt(myCmd.Parameters("@jobNumber").Value)
                    paramXml = CStr(myCmd.Parameters("@parameters").Value)

                    'Step task was found; get the data for it
                    Dim dctParameters As IEnumerable(Of udtParameterInfoType) = FillParamDictXml(paramXml)

                    If dctParameters IsNot Nothing Then

                        For Each udtParamInfo As udtParameterInfoType In dctParameters
                            SetParam(udtParamInfo.Section, udtParamInfo.ParamName, udtParamInfo.Value)
                        Next

                        SaveJobParameters(m_MgrParams.GetParam("WorkDir"), paramXml, m_JobId)
                        Outcome = RequestTaskResult.TaskFound
                    Else
                        'There was an error
                        Const Msg As String = "clsAnalysisJob.AddTaskParamsToDictionary(), Unable to obtain job data"
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
                        Outcome = RequestTaskResult.ResultError
                    End If
                Case RET_VAL_TASK_NOT_AVAILABLE
                    'No jobs found
                    Outcome = RequestTaskResult.NoTaskFound
                Case RET_VAL_EXCESSIVE_RETRIES
                    ' Too many retries
                    Outcome = RequestTaskResult.TooManyRetries
                Case RET_VAL_DEADLOCK
                    ' Transaction was deadlocked on lock resources with another process and has been chosen as the deadlock victim
                    Outcome = RequestTaskResult.Deadlock
                Case Else
                    'There was an SP error
                    Dim msg As String = "clsAnalysisJob.RequestAnalysisJob(), SP execution error " & RetVal.ToString
                    msg &= "; Msg text = " & CStr(myCmd.Parameters("@message").Value)
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg)
                    Outcome = RequestTaskResult.ResultError
            End Select

        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception requesting analysis job: " & ex.Message)
            Outcome = RequestTaskResult.ResultError
        End Try

        Return Outcome

    End Function

    ''' <summary>
    ''' Reset the class-wide variables to their defaults
    ''' </summary>
    ''' <remarks></remarks>
    Public Sub Reset()
        m_TaskWasClosed = False

        m_ResultFilesToSkip.Clear()
        m_ResultFileExtensionsToSkip.Clear()
        m_ResultFilesToKeep.Clear()
        m_ServerFilesToDelete.Clear()

        m_DatasetInfoList.Clear()

        If m_JobParams Is Nothing Then
            m_JobParams = New Dictionary(Of String, Dictionary(Of String, String))(StringComparer.CurrentCultureIgnoreCase)
        End If

        m_JobParams.Clear()

    End Sub

    ''' <summary>
    ''' Saves job Parameters to an XML File
    ''' </summary>
    ''' <param name="WorkDir">Full path to work directory</param>
    ''' <param name="paramXml">Contains the xml for all the job parameters</param>
    ''' <param name="jobNum">Contains the job number</param>
    ''' <remarks>Skipped if the debug level is less than 4</remarks>
    Private Sub SaveJobParameters(WorkDir As String, paramXml As String, jobNum As Integer)

        Dim xmlWriter As New clsFormattedXMLWriter
        Dim xmlParameterFilename As String
        Dim xmlParameterFilePath As String = String.Empty

        Try
            xmlParameterFilename = JobParametersFilename(jobNum.ToString())
            xmlParameterFilePath = Path.Combine(WorkDir, xmlParameterFilename)

            Dim filteredXML As String
            If m_DebugLevel < 4 Then
                ' Remove the StepParameters section from the XML; that section changes with each job step and we don't need to write it out to disk
                filteredXML = RemoveXmlSection(paramXml, "StepParameters")
            Else
                filteredXML = paramXml
            End If

            xmlWriter.WriteXMLToFile(filteredXML, xmlParameterFilePath)

            If Not AddAdditionalParameter("JobParameters", "genJobParamsFilename", xmlParameterFilename) Then Return

            Dim Msg As String = "Job Parameters successfully saved to file: " & xmlParameterFilePath

            ' Copy the Job Parameter file to the Analysis Manager folder so that we can inspect it if the job fails
            clsGlobal.CopyAndRenameFileWithBackup(xmlParameterFilePath, clsGlobal.GetAppFolderPath(), "RecentJobParameters.xml", 5)

            If m_DebugLevel >= 2 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, Msg)
            End If

        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception saving analysis job parameters to " & xmlParameterFilePath & ": " & ex.Message)
            Return
        End Try

        Return
    End Sub

    ''' <summary>
    ''' Contact the Pipeline database to close the analysis job
    ''' </summary>
    ''' <param name="CloseOut">IJobParams enum specifying close out type</param>
    ''' <param name="CompMsg">Completion message to be added to database upon closeout</param>
    Public Overrides Sub CloseTask(CloseOut As IJobParams.CloseOutType, CompMsg As String) Implements IJobParams.CloseTask
        CloseTask(CloseOut, CompMsg, 0, String.Empty)
    End Sub

    ''' <summary>
    ''' Contact the Pipeline database to close the analysis job
    ''' </summary>
    ''' <param name="CloseOut">IJobParams enum specifying close out type</param>
    ''' <param name="CompMsg">Completion message to be added to database upon closeout</param>
    ''' <param name="EvalCode">Evaluation code (0 if no special evaulation message)</param>
    ''' <param name="EvalMessage">Evaluation message ("" if no special message)</param>
    Public Overrides Sub CloseTask(CloseOut As IJobParams.CloseOutType, CompMsg As String, EvalCode As Integer, EvalMessage As String) Implements IJobParams.CloseTask

        Dim MsgStr As String
        Dim CompCode As Integer

        CompCode = CInt(CloseOut)

        If EvalMessage Is Nothing Then EvalMessage = String.Empty

        If m_TaskWasClosed Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Job " & m_JobId & " has already been closed; will not call " & SP_NAME_SET_COMPLETE & " again")
        Else
            m_TaskWasClosed = True
            If Not SetAnalysisJobComplete(SP_NAME_SET_COMPLETE, CompCode, CompMsg, EvalCode, EvalMessage) Then
                MsgStr = "Error setting job complete in database, job " & m_JobId
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, MsgStr)
            End If
        End If

    End Sub

    ''' <summary>
    ''' Communicates with database to perform job closeout
    ''' </summary>
    ''' <param name="SpName">Name of SP in database to call for closeout, typically SetStepTaskComplete</param>
    ''' <param name="CompCode">Integer version of ITaskParams specifying closeout type</param>
    ''' <param name="CompMsg">Comment to insert in database</param>
    ''' <param name="EvalCode">Integer results evaluation code</param>
    ''' <param name="EvalMsg">Message describing evaluation results</param>
    ''' <returns>True for success, False for failure</returns>
    ''' <remarks>EvalCode and EvalMsg not presently used</remarks>
    Protected Function SetAnalysisJobComplete(
       spName As String,
       compCode As Integer,
       compMsg As String,
       evalCode As Integer,
       evalMsg As String) As Boolean

        Dim success As Boolean
        Dim returnCode As Integer

        ' Setup for execution of the stored procedure
        Dim myCmd As New SqlCommand(spName)
        With myCmd
            .CommandType = CommandType.StoredProcedure

            .Parameters.Add(New SqlParameter("@Return", SqlDbType.Int))
            .Parameters.Item("@Return").Direction = ParameterDirection.ReturnValue
            .Parameters.Add(New SqlParameter("@job", SqlDbType.Int))

            .Parameters.Item("@job").Direction = ParameterDirection.Input
            .Parameters.Item("@job").Value = GetJobParameter("StepParameters", "Job", 0)

            .Parameters.Add(New SqlParameter("@step", SqlDbType.Int))
            .Parameters.Item("@step").Direction = ParameterDirection.Input
            .Parameters.Item("@step").Value = GetJobParameter("StepParameters", "Step", 0)

            .Parameters.Add(New SqlParameter("@completionCode", SqlDbType.Int))
            .Parameters.Item("@completionCode").Direction = ParameterDirection.Input
            .Parameters.Item("@completionCode").Value = compCode

            .Parameters.Add(New SqlParameter("@completionMessage", SqlDbType.VarChar, 256))
            .Parameters.Item("@completionMessage").Direction = ParameterDirection.Input
            .Parameters.Item("@completionMessage").Value = compMsg

            .Parameters.Add(New SqlParameter("@evaluationCode", SqlDbType.Int))
            .Parameters.Item("@evaluationCode").Direction = ParameterDirection.Input
            .Parameters.Item("@evaluationCode").Value = evalCode

            .Parameters.Add(New SqlParameter("@evaluationMessage", SqlDbType.VarChar, 256))
            .Parameters.Item("@evaluationMessage").Direction = ParameterDirection.Input
            .Parameters.Item("@evaluationMessage").Value = evalMsg

            .Parameters.Add(New SqlParameter("@organismDBName", SqlDbType.VarChar, 128))
            .Parameters.Item("@organismDBName").Direction = ParameterDirection.Input

            Dim strValue As String = String.Empty
            If TryGetParam("PeptideSearch", "generatedFastaName", strValue) Then
                .Parameters.Item("@organismDBName").Value = strValue
            Else
                .Parameters.Item("@organismDBName").Value = String.Empty
            End If

        End With

        ' Execute the Stored Procedure (retry the call up to 20 times)
        returnCode = PipelineDBProcedureExecutor.ExecuteSP(myCmd, 20)

        If returnCode = 0 Then
            success = True
        Else
            Dim Msg As String = "Error " & returnCode.ToString & " setting analysis job complete"
            '			Msg &= "; Message = " & CStr(myCmd.Parameters("@message").Value)
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
            success = False
        End If

        Return success

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
            strTool = GetParam("ToolName")

            strToolAndStepTool = GetParam("StepTool")
            If String.IsNullOrWhiteSpace(strToolAndStepTool) Then strToolAndStepTool = String.Empty

            strStep = GetParam("StepParameters", "Step")
            If strStep Is Nothing Then strStep = String.Empty

            If Not String.IsNullOrWhiteSpace(strTool) AndAlso Not String.Equals(strToolAndStepTool, strTool) Then
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


