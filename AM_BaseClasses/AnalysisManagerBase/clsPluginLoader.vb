Imports System.Xml
Imports System.Collections.Specialized
Imports AnalysisManagerBase.clsSummaryFile
Imports System.IO

Public Class clsPluginLoader

	Private Shared m_msgList As New ArrayList
	Private Shared m_pluginConfigFile As String = "plugin_info.xml"

	Public Shared Property FileName() As String
		Get
			Return m_pluginConfigFile
		End Get
		Set(ByVal Value As String)
			m_pluginConfigFile = Value
		End Set
	End Property

	Public Shared ReadOnly Property Message() As String
		Get
			Dim s As String
			Dim i As Integer

			For Each DumStr As String In m_msgList
				If s <> "" Then s &= vbCrLf
				s &= DumStr
			Next

			Return s
		End Get
	End Property

	Public Shared Sub ClearMessageList()

		'Clears the message list
		m_msgList.Clear()

	End Sub

	Private Shared Function GetPluginInfo(ByVal XPath As String, ByRef className As String, ByRef assyName As String) As Boolean
		Dim doc As XmlDocument = New XmlDocument
		Dim nodeList As XmlNodeList
		Dim n As XmlElement
		Dim e As Exception

		Try
			'read the tool runner info file
			'			doc.Load(m_pluginConfigFile)
			doc.Load(GetPluginInfoFilePath(m_pluginConfigFile))
			Dim root As XmlElement = doc.DocumentElement

			'find the element that matches the tool name
			nodeList = root.SelectNodes(XPath)

			' make sure that we found exactly one element, 
			' and if we did, retrieve its information 
			If nodeList.Count <> 1 Then
				Throw New System.Exception("Could not resolve tool name")
			End If
			For Each n In nodeList
				className = n.GetAttribute("Class")
				assyName = n.GetAttribute("AssemblyFile")
			Next
			GetPluginInfo = True
		Catch e
			m_msgList.Add(e.Message)
			GetPluginInfo = False
		End Try
	End Function

	Private Shared Function LoadObject(ByVal className As String, ByVal assyName As String) As Object
		Dim e As Exception
		Dim obj As Object = Nothing
		m_msgList.Clear()
		Try
			'Build instance of tool runner subclass from class name and assembly file name.
			Dim a As System.Reflection.Assembly
			a = System.Reflection.Assembly.LoadFrom(GetPluginInfoFilePath(assyName))
			Dim t As Type = a.GetType(className, False, True)
			obj = Activator.CreateInstance(t)
		Catch e
			''Catch any exceptions
			m_msgList.Add("clsPluginLoader.LoadObject(), exception: " & e.Message)
		End Try
		LoadObject = obj
	End Function

	Public Shared Function GetToolRunner(ByVal ToolName As String, ByVal clustered As Boolean) As IToolRunner
		' if manager is configured for a cluster, append a suffix to the tool name
		If clustered Then
			ToolName &= "-cluster"
		End If
		Dim xpath As String = "//ToolRunners/ToolRunner[@Tool='" & ToolName.ToLower & "']"

		Dim className As String
		Dim assyName As String
		Dim myToolRunner As IToolRunner = Nothing
		Dim e As Exception
		If GetPluginInfo(xpath, className, assyName) Then
			Dim obj As Object = LoadObject(className, assyName)
			If Not obj Is Nothing Then
				Try
					myToolRunner = DirectCast(obj, IToolRunner)
				Catch e
					''Catch any exceptions
					m_msgList.Add(e.Message)
				End Try
			End If
		End If
		clsSummaryFile.Add("Loaded ToolRunner: " & className & " from " & assyName)
		GetToolRunner = myToolRunner
	End Function

	Public Shared Function GetSpectraGenerator(ByVal SpectraDataType As String) As ISpectraFileProcessor
		Dim xpath As String = "//DTAGenerators/DTAGenerator[@DataType='" & SpectraDataType & "']"
		Dim className As String
		Dim assyName As String
		Dim myModule As ISpectraFileProcessor = Nothing
		Dim e As Exception
		If GetPluginInfo(xpath, className, assyName) Then
			Dim obj As Object = LoadObject(className, assyName)
			If Not obj Is Nothing Then
				Try
					myModule = DirectCast(obj, ISpectraFileProcessor)
				Catch e
					''Catch any exceptions
					m_msgList.Add(e.Message)
				End Try
			End If
		End If
		clsSummaryFile.Add("Loaded DTAGenerator: " & className & " from " & assyName)
		GetSpectraGenerator = myModule
	End Function

	Public Shared Function GetSpectraFilter(ByVal FilterType As String) As ISpectraFilter
		Dim xpath As String = "//DTAFilters/DTAFilter[@FilterType='" & FilterType & "']"
		Dim className As String
		Dim assyName As String
		Dim myModule As ISpectraFilter = Nothing
		Dim e As Exception
		If GetPluginInfo(xpath, className, assyName) Then
			Dim obj As Object = LoadObject(className, assyName)
			If Not obj Is Nothing Then
				Try
					myModule = DirectCast(obj, ISpectraFilter)
				Catch e
					''Catch any exceptions
					m_msgList.Add(e.Message)
				End Try
			End If
		End If
		clsSummaryFile.Add("Loaded DTAFilter: " & className & " from " & assyName)
		GetSpectraFilter = myModule
	End Function

	Public Shared Function GetAnalysisResources(ByVal ToolName As String) As IAnalysisResources

		Dim xpath As String = "//Resourcers/Resourcer[@Tool='" & ToolName & "']"
		Dim className As String
		Dim assyName As String
		Dim myModule As IAnalysisResources = Nothing
		Dim e As Exception
		If GetPluginInfo(xpath, className, assyName) Then
			Dim obj As Object = LoadObject(className, assyName)
			If Not obj Is Nothing Then
				Try
					myModule = DirectCast(obj, IAnalysisResources)
				Catch e
					''Catch any exceptions
					m_msgList.Add(e.Message)
				End Try
			End If
		End If
		clsSummaryFile.Add("Loaded resourcer: " & className & " from " & assyName)
		GetAnalysisResources = myModule
	End Function

	Private Shared Function GetPluginInfoFilePath(ByVal PluginInfoFileName As String) As String
		Dim fi As New FileInfo(System.Windows.Forms.Application.ExecutablePath)
		Return System.IO.Path.Combine(fi.DirectoryName, PluginInfoFileName)
	End Function


End Class
