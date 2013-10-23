'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2007, Battelle Memorial Institute
' Created 12/19/2007
'
' Last modified 01/16/2008
'*********************************************************************************************************

Option Strict On

Imports System.Xml
Imports AnalysisManagerBase
Imports System.IO

Public Class clsPluginLoader

	'*********************************************************************************************************
	'Class for loading analysis manager plugins
	'*********************************************************************************************************

#Region "Module variables"
	Private m_msgList As New Generic.List(Of String)
	Private m_MgrFolderPath As String
	Private m_pluginConfigFile As String = "plugin_info.xml"
	Private m_SummaryFile As AnalysisManagerBase.clsSummaryFile
#End Region

#Region "Properties"
	Public Property FileName() As String
		Get
			Return m_pluginConfigFile
		End Get
		Set(ByVal Value As String)
			m_pluginConfigFile = Value
		End Set
	End Property

	Public ReadOnly Property Message() As String
		Get
			Dim s As String = ""

			For Each DumStr As String In m_msgList
				If s <> "" Then s &= System.Environment.NewLine
				s &= DumStr
			Next

			Return s
		End Get
	End Property
#End Region

#Region "Methods"

	Public Sub New(ByRef objSummaryFile As AnalysisManagerBase.clsSummaryFile, ByVal MgrFolderPath As String)
		m_SummaryFile = objSummaryFile
		m_MgrFolderPath = MgrFolderPath
	End Sub

	''' <summary>
	''' Clears internal list of error messages
	''' </summary>
	''' <remarks></remarks>
	Public Sub ClearMessageList()

		'Clears the message list
		m_msgList.Clear()

	End Sub


#If PLUGIN_DEBUG_MODE_ENABLED Then

	Private Function DebugModeGetToolRunner(ByVal className As String) As AnalysisManagerBase.IToolRunner

		Dim myToolRunner As AnalysisManagerBase.IToolRunner = Nothing

		Select Case className.ToLower()
			'Case "AnalysisManagerXTandemPlugIn.clsAnalysisToolRunnerXT".ToLower()
			'    myToolRunner = DirectCast(New AnalysisManagerXTandemPlugIn.clsAnalysisToolRunnerXT, IToolRunner)

			'Case "AnalysisManagerDtaSplitPlugIn.clsAnalysisToolRunnerDtaSplit".ToLower()
			'	myToolRunner = DirectCast(New AnalysisManagerDtaSplitPlugIn.clsAnalysisToolRunnerDtaSplit, IToolRunner)

			'Case "AnalysisManagerInSpecTPlugIn.clsAnalysisToolRunnerIN".ToLower()
			'   myToolRunner = DirectCast(New AnalysisManagerInSpecTPlugIn.clsAnalysisToolRunnerIN, IToolRunner)

			'Case "AnalysisManagerInspResultsAssemblyPlugIn.clsAnalysisToolRunnerInspResultsAssembly".ToLower()
			'    myToolRunner = DirectCast(New AnalysisManagerInspResultsAssemblyPlugIn.clsAnalysisToolRunnerInspResultsAssembly, IToolRunner)

			'Case "AnalysisManagerDecon2lsPlugIn.clsAnalysisToolRunnerDecon2lsDeIsotope".ToLower()
			'    myToolRunner = DirectCast(New AnalysisManagerDecon2lsPlugIn.clsAnalysisToolRunnerDecon2lsDeIsotope, IToolRunner)
			'Case "AnalysisManagerDecon2lsPlugIn.clsAnalysisToolRunnerDecon2lsTIC".ToLower()
			'    myToolRunner = DirectCast(New AnalysisManagerDecon2lsPlugIn.clsAnalysisToolRunnerDecon2lsDeIsotope, IToolRunner)
			'Case "AnalysisManagerMSClusterDTAtoDATPlugIn.clsAnalysisToolRunnerDTAtoDAT".ToLower()
			'    myToolRunner = DirectCast(New AnalysisManagerMSClusterDTAtoDATPlugIn.clsAnalysisToolRunnerDTAtoDAT, IToolRunner)

			'Case "AnalysisManagerExtractionPlugin.clsExtractToolRunner".ToLower()
			'	myToolRunner = DirectCast(New AnalysisManagerExtractionPlugin.clsExtractToolRunner, IToolRunner)

			'Case "AnalysisManagerMsXmlGenPlugIn.clsAnalysisToolRunnerMSXMLGen".ToLower()
			'	myToolRunner = DirectCast(New AnalysisManagerMsXmlGenPlugIn.clsAnalysisToolRunnerMSXMLGen, IToolRunner)

			'Case "AnalysisManagerMsXmlBrukerPlugIn.clsAnalysisToolRunnerMSXMLBruker".ToLower()
			'    myToolRunner = DirectCast(New AnalysisManagerMsXmlBrukerPlugIn.clsAnalysisToolRunnerMSXMLBruker, IToolRunner)

			'Case "MSMSSpectrumFilterAM.clsAnalysisToolRunnerMsMsSpectrumFilter".ToLower()
			'    myToolRunner = DirectCast(New MSMSSpectrumFilterAM.clsAnalysisToolRunnerMsMsSpectrumFilter, IToolRunner)
			'Case "AnalysisManagerMasicPlugin.clsAnalysisToolRunnerMASICFinnigan".ToLower()
			'    myToolRunner = DirectCast(New AnalysisManagerMasicPlugin.clsAnalysisToolRunnerMASICFinnigan, IToolRunner)
			'Case "AnalysisManagerICR2LSPlugIn.clsAnalysisToolRunnerICR".ToLower()
			'    myToolRunner = DirectCast(New AnalysisManagerICR2LSPlugIn.clsAnalysisToolRunnerICR, IToolRunner)
			'Case "AnalysisManagerICR2LSPlugIn.clsAnalysisToolRunnerLTQ_FTPek".ToLower()
			'    myToolRunner = DirectCast(New AnalysisManagerICR2LSPlugIn.clsAnalysisToolRunnerLTQ_FTPek, IToolRunner)
			'Case "AnalysisManagerLCMSFeatureFinderPlugIn.clsAnalysisToolRunnerLCMSFF".ToLower()
			'    myToolRunner = DirectCast(New AnalysisManagerLCMSFeatureFinderPlugIn.clsAnalysisToolRunnerLCMSFF, IToolRunner)
			'Case "AnalysisManagerOMSSAPlugIn.clsAnalysisToolRunnerOM".ToLower()
			'    myModule = DirectCast(New AnalysisManagerOMSSAPlugin.clsAnalysisToolRunnerOM, IToolRunner)
			'Case "AnalysisManagerDecon2lsV2PlugIn.clsAnalysisToolRunnerDecon2ls".ToLower()
			'    myToolRunner = DirectCast(New AnalysisManagerDecon2lsV2PlugIn.clsAnalysisToolRunnerDecon2ls, IToolRunner)
			'Case "AnalysisManagerDtaRefineryPlugIn.clsAnalysisToolRunnerDtaRefinery".ToLower()
			'    myToolRunner = DirectCast(New AnalysisManagerDtaRefineryPlugIn.clsAnalysisToolRunnerDtaRefinery, IToolRunner)
			'Case "AnalysisManagerPRIDEMzXMLPlugIn.clsAnalysisToolRunnerPRIDEMzXML".ToLower()
			'    myToolRunner = DirectCast(New AnalysisManagerPRIDEMzXMLPlugIn.clsAnalysisToolRunnerPRIDEMzXML, IToolRunner)
			'Case "AnalysisManagerPhospho_FDR_AggregatorPlugIn.clsAnalysisToolRunnerPhosphoFdrAggregator".ToLower()
			'    myToolRunner = DirectCast(New AnalysisManagerPhospho_FDR_AggregatorPlugIn.clsAnalysisToolRunnerPhosphoFdrAggregator, IToolRunner)

			'Case "AnalysisManagerMSGFPlugin.clsMSGFRunner".ToLower()
			'	myToolRunner = DirectCast(New AnalysisManagerMSGFPlugin.clsMSGFRunner, IToolRunner)

			'Case "AnalysisManagerMSGFDBPlugin.clsAnalysisToolRunnerMSGFDB".ToLower()
			'	myToolRunner = DirectCast(New AnalysisManagerMSGFDBPlugIn.clsAnalysisToolRunnerMSGFDB, IToolRunner)

			'Case "AnalysisManagerMSDeconvPlugIn.clsAnalysisToolRunnerMSDeconv".ToLower()
			'	myToolRunner = DirectCast(New AnalysisManagerMSDeconvPlugIn.clsAnalysisToolRunnerMSDeconv, IToolRunner)

			'Case "AnalysisManagerMSAlignPlugIn.clsAnalysisToolRunnerMSAlign".ToLower()
			'	myToolRunner = DirectCast(New AnalysisManagerMSAlignPlugIn.clsAnalysisToolRunnerMSAlign, IToolRunner)

			'Case "AnalysisManagerMSAlignPlugIn.clsAnalysisToolRunnerMSAlignHistone".ToLower()
			'	myToolRunner = DirectCast(New AnalysisManagerMSAlignHistonePlugIn.clsAnalysisToolRunnerMSAlignHistone, IToolRunner)

			'Case "AnalysisManagerSMAQCPlugIn.clsAnalysisToolRunnerSMAQC".ToLower()
			'	myToolRunner = DirectCast(New AnalysisManagerSMAQCPlugIn.clsAnalysisToolRunnerSMAQC, IToolRunner)

			'Case "DTASpectraFileGen.clsDtaGenToolRunner".ToLower()
			'	myToolRunner = DirectCast(New DTASpectraFileGen.clsDtaGenToolRunner, IToolRunner)

			'Case "AnalysisManagerSequestPlugin.clsAnalysisToolRunnerSeqCluster".ToLower()
			'	myToolRunner = DirectCast(New AnalysisManagerSequestPlugin.clsAnalysisToolRunnerSeqCluster, IToolRunner)

			'Case "AnalysisManagerIDPickerPlugIn.clsAnalysisToolRunnerIDPicker".ToLower()
			'	myToolRunner = DirectCast(New AnalysisManagerIDPickerPlugIn.clsAnalysisToolRunnerIDPicker, IToolRunner)

			'Case "AnalysisManagerLipidMapSearchPlugIn.clsAnalysisToolRunnerLipidMapSearch".ToLower()
			'	myToolRunner = DirectCast(New AnalysisManagerLipidMapSearchPlugIn.clsAnalysisToolRunnerLipidMapSearch, IToolRunner)

			'Case "AnalysisManagerMSAlignQuantPlugIn.clsAnalysisToolRunnerMSAlignQuant".ToLower()
			'	myToolRunner = DirectCast(New AnalysisManagerMSAlignQuantPlugIn.clsAnalysisToolRunnerMSAlignQuant, IToolRunner)

			'Case "AnalysisManagerPRIDEConverterPlugIn.clsAnalysisToolRunnerPRIDEConverter".ToLower()
			'	myToolRunner = DirectCast(New AnalysisManagerPRIDEConverterPlugIn.clsAnalysisToolRunnerPRIDEConverter, IToolRunner)

			'Case "AnalysisManagerMultiAlign_AggregatorPlugIn.clsAnalysisToolRunnerMultiAlignAggregator".ToLower()
			'	myToolRunner = DirectCast(New AnalysisManagerMultiAlign_AggregatorPlugIn.clsAnalysisToolRunnerMultiAlignAggregator, IToolRunner)

			'Case "AnalysisManager_Mage_PlugIn.clsAnalysisToolRunnerMage".ToLower()
			'	myToolRunner = DirectCast(New AnalysisManager_Mage_PlugIn.clsAnalysisToolRunnerMage, IToolRunner)

			'Case "AnalysisManager_Cyclops_PlugIn.clsAnalysisToolRunnerCyclops".ToLower()
			'	myToolRunner = DirectCast(New AnalysisManager_Cyclops_PlugIn.clsAnalysisToolRunnerCyclops, IToolRunner)

			'Case "AnalysisManager_Cyclops_PlugIn.clsAnalysisToolRunnerCyclops".ToLower()
			'	myToolRunner = DirectCast(New AnalysisManager_Cyclops_PlugIn.clsAnalysisToolRunnerCyclops, IToolRunner)

			'Case "AnalysisManager_AScore_PlugIn.clsAnalysisToolRunnerAScore".ToLower()
			'	myToolRunner = DirectCast(New AnalysisManager_AScore_PlugIn.clsAnalysisToolRunnerAScore, IToolRunner)

		End Select

		Return myToolRunner
	End Function

	Private Function DebugModeGetAnalysisResources(ByVal className As String) As AnalysisManagerBase.IAnalysisResources

		Dim myModule As AnalysisManagerBase.IAnalysisResources = Nothing

		Select Case className.ToLower()
			'Case "AnalysisManagerXTandemPlugIn.clsAnalysisResourcesXT".ToLower()
			'    myModule = DirectCast(New AnalysisManagerXTandemPlugIn.clsAnalysisResourcesXT, IAnalysisResources)

			'Case "AnalysisManagerDtaSplitPlugIn.clsAnalysisResourcesDtaSplit".ToLower()
			'	myModule = DirectCast(New AnalysisManagerDtaSplitPlugIn.clsAnalysisResourcesDtaSplit, IAnalysisResources)

			'Case "AnalysisManagerInSpecTPlugIn.clsAnalysisResourcesIN".ToLower()
			'   myModule = DirectCast(New AnalysisManagerInSpecTPlugIn.clsAnalysisResourcesIN, IAnalysisResources)

			'Case "AnalysisManagerInspResultsAssemblyPlugIn.clsAnalysisResourcesInspResultsAssembly".ToLower()
			'    myModule = DirectCast(New AnalysisManagerInspResultsAssemblyPlugIn.clsAnalysisResourcesInspResultsAssembly, IAnalysisResources)

			'Case "AnalysisManagerDecon2lsPlugIn.clsAnalysisResourcesDecon2ls".ToLower()
			'    myModule = DirectCast(New AnalysisManagerDecon2lsPlugIn.clsAnalysisResourcesDecon2ls, IAnalysisResources)
			'Case "AnalysisManagerDecon2lsPlugIn.clsAnalysisResourcesDecon2ls".ToLower()
			'    myModule = DirectCast(New AnalysisManagerDecon2lsPlugIn.clsAnalysisResourcesDecon2ls, IAnalysisResources)
			'Case "AnalysisManagerMSClusterDTAtoDATPlugIn.clsAnalysisResourcesDTAtoDAT".ToLower()
			'    myModule = DirectCast(New AnalysisManagerMSClusterDTAtoDATPlugIn.clsAnalysisResourcesDTAtoDAT, IAnalysisResources)

			'Case "AnalysisManagerExtractionPlugin.clsAnalysisResourcesExtraction".ToLower()
			'	myModule = DirectCast(New AnalysisManagerExtractionPlugin.clsAnalysisResourcesExtraction, IAnalysisResources)

			'Case "AnalysisManagerMsXmlGenPlugIn.clsAnalysisResourcesMSXMLGen".ToLower()
			'	myModule = DirectCast(New AnalysisManagerMsXmlGenPlugIn.clsAnalysisResourcesMSXMLGen, IAnalysisResources)

			'Case "AnalysisManagerMsXmlBrukerPlugIn.clsAnalysisResourcesMSXmlBruker".ToLower()
			'    myModule = DirectCast(New AnalysisManagerMsXmlBrukerPlugIn.clsAnalysisResourcesMSXMLBruker, IAnalysisResources)

			'Case "MSMSSpectrumFilterAM.clsAnalysisResourcesMsMsSpectrumFilter".ToLower()
			'    myToolRunner = DirectCast(New MSMSSpectrumFilterAM.clsAnalysisResourcesMsMsSpectrumFilter, IAnalysisResources)
			'Case "AnalysisManagerMasicPlugin.clsAnalysisResourcesMASIC".ToLower()
			'    myModule = DirectCast(New AnalysisManagerMasicPlugin.clsAnalysisResourcesMASIC, IAnalysisResources)
			'Case "AnalysisManagerICR2LSPlugIn.clsAnalysisResourcesIcr2ls".ToLower()
			'    myModule = DirectCast(New AnalysisManagerICR2LSPlugIn.clsAnalysisResourcesIcr2ls, IAnalysisResources)
			'Case "AnalysisManagerICR2LSPlugIn.clsAnalysisResourcesLTQ_FTPek".ToLower()
			'    myModule = DirectCast(New AnalysisManagerICR2LSPlugIn.clsAnalysisResourcesLTQ_FTPek, IAnalysisResources)
			'Case "AnalysisManagerLCMSFeatureFinderPlugIn.clsAnalysisResourcesLCMSFF".ToLower()
			'    myModule = DirectCast(New AnalysisManagerLCMSFeatureFinderPlugIn.clsAnalysisResourcesLCMSFF, IAnalysisResources)
			'Case "AnalysisManagerOMSSAPlugIn.clsAnalysisResourcesOM".ToLower()
			'    myModule = DirectCast(New AnalysisManagerOMSSAPlugin.clsAnalysisResourcesOM, IAnalysisResources)
			'Case "AnalysisManagerDecon2lsV2PlugIn.clsAnalysisResourcesDecon2ls".ToLower()
			'    myModule = DirectCast(New AnalysisManagerDecon2lsV2PlugIn.clsAnalysisResourcesDecon2ls, IAnalysisResources)
			'Case "AnalysisManagerDtaRefineryPlugIn.clsAnalysisResourcesDtaRefinery".ToLower()
			'    myModule = DirectCast(New AnalysisManagerDtaRefineryPlugIn.clsAnalysisResourcesDtaRefinery, IAnalysisResources)
			'Case "AnalysisManagerPRIDEMzXMLPlugIn.clsAnalysisResourcesPRIDEMzXML".ToLower()
			'    myModule = DirectCast(New AnalysisManagerPRIDEMzXMLPlugIn.clsAnalysisResourcesPRIDEMzXML, IAnalysisResources)

			'Case "AnalysisManagerPhospho_FDR_AggregatorPlugIn.clsAnalysisResourcesPhosphoFdrAggregator".ToLower()
			'    myModule = DirectCast(New AnalysisManagerPhospho_FDR_AggregatorPlugIn.clsAnalysisResourcesPhosphoFdrAggregator, IAnalysisResources)

			'Case "AnalysisManagerMSGFPlugin.clsAnalysisResourcesMSGF".ToLower()
			'	myModule = DirectCast(New AnalysisManagerMSGFPlugin.clsAnalysisResourcesMSGF, IAnalysisResources)

			'Case "AnalysisManagerMSGFDBPlugin.clsAnalysisResourcesMSGFDB".ToLower()
			'	myModule = DirectCast(New AnalysisManagerMSGFDBPlugIn.clsAnalysisResourcesMSGFDB, IAnalysisResources)

			'Case "AnalysisManagerMSDeconvPlugin.clsAnalysisResourcesMSDeconv".ToLower()()
			'	myModule = DirectCast(New AnalysisManagerMSDeconvPlugIn.clsAnalysisResourcesMSDeconv, IAnalysisResources)

			'Case "AnalysisManagerMSAlignPlugin.clsAnalysisResourcesMSAlign".ToLower()()
			'	myModule = DirectCast(New AnalysisManagerMSAlignPlugIn.clsAnalysisResourcesMSAlign, IAnalysisResources)

			'Case "AnalysisManagerMSAlignHistonePlugin.clsAnalysisResourcesMSAlignHistone".ToLower()()
			'	myModule = DirectCast(New AnalysisManagerMSAlignHistonePlugIn.clsAnalysisResourcesMSAlignHistone, IAnalysisResources)

			'Case "AnalysisManagerSMAQCPlugIn.clsAnalysisResourcesSMAQC".ToLower()
			'	myModule = DirectCast(New AnalysisManagerSMAQCPlugIn.clsAnalysisResourcesSMAQC, IAnalysisResources)

			'Case "DTASpectraFileGen.clsDtaGenResources".ToLower()
			'	myModule = DirectCast(New DTASpectraFileGen.clsDtaGenResources, IAnalysisResources)

			'Case "AnalysisManagerSequestPlugin.clsAnalysisResourcesSeq".ToLower()()
			'	myModule = DirectCast(New AnalysisManagerSequestPlugin.clsAnalysisResourcesSeq, IAnalysisResources)

			'Case "AnalysisManagerIDPickerPlugIn.clsAnalysisResourcesIDPicker".ToLower()
			'	myModule = DirectCast(New AnalysisManagerIDPickerPlugIn.clsAnalysisResourcesIDPicker, IAnalysisResources)

			'Case "AnalysisManagerLipidMapSearchPlugIn.clsAnalysisResourcesLipidMapSearch".ToLower()
			'	myModule = DirectCast(New AnalysisManagerLipidMapSearchPlugIn.clsAnalysisResourcesLipidMapSearch, IAnalysisResources)

			'Case "AnalysisManagerMSAlignQuantPlugIn.clsAnalysisResourcesMSAlignQuant".ToLower()
			'	myModule = DirectCast(New AnalysisManagerMSAlignQuantPlugIn.clsAnalysisResourcesMSAlignQuant, IAnalysisResources)

			'Case "AnalysisManagerPRIDEConverterPlugIn.clsAnalysisResourcesPRIDEConverter".ToLower()
			'	myModule = DirectCast(New AnalysisManagerPRIDEConverterPlugIn.clsAnalysisResourcesPRIDEConverter, IAnalysisResources)

			'Case "AnalysisManagerMultiAlign_AggregatorPlugIn.clsAnalysisResourcesMultiAlignAggregator".ToLower()
			'	myModule = DirectCast(New AnalysisManagerMultiAlign_AggregatorPlugIn.clsAnalysisResourcesMultiAlignAggregator, IAnalysisResources)

			'Case "AnalysisManager_Mage_PlugIn.clsAnalysisResourcesMage".ToLower()
			'	myModule = DirectCast(New AnalysisManager_Mage_PlugIn.clsAnalysisResourcesMage, IAnalysisResources)

			'Case "AnalysisManager_Cyclops_PlugIn.clsAnalysisResourcesCyclops".ToLower()
			'	myModule = DirectCast(New AnalysisManager_Cyclops_PlugIn.clsAnalysisResourcesCyclops, IAnalysisResources)

			'Case "AnalysisManager_AScore_PlugIn.clsAnalysisResourcesAScore".ToLower()
			'	myModule = DirectCast(New AnalysisManager_AScore_PlugIn.clsAnalysisResourcesAScore, IAnalysisResources)

		End Select

		Return myModule

	End Function
#End If

	''' <summary>
	''' Retrieves data for specified plugin from plugin info config file
	''' </summary>
	''' <param name="XPath">XPath spec for specified plugin</param>
	''' <param name="className">Name of class for plugin (return value) </param>
	''' <param name="assyName">Name of assembly for plugin (return value)</param>
	''' <returns>TRUE for success, FALSE for failure</returns>
	''' <remarks></remarks>
	Private Function GetPluginInfo(ByVal XPath As String, ByRef className As String, ByRef assyName As String) As Boolean
		Dim doc As XmlDocument = New XmlDocument
		Dim nodeList As XmlNodeList
		Dim n As XmlElement
		Dim strPluginInfo As String = String.Empty

		Try
			If XPath Is Nothing Then XPath = String.Empty
			If className Is Nothing Then className = String.Empty
			If assyName Is Nothing Then assyName = String.Empty

			strPluginInfo = "XPath=""" & XPath & """; className=""" & className & """; assyName=" & assyName & """"

			'read the tool runner info file
			doc.Load(GetPluginInfoFilePath(m_pluginConfigFile))
			Dim root As XmlElement = doc.DocumentElement

			'find the element that matches the tool name
			nodeList = root.SelectNodes(XPath)

			' make sure that we found exactly one element, 
			' and if we did, retrieve its information 
			If nodeList.Count <> 1 Then
				Throw New System.Exception("Could not resolve tool name; " & strPluginInfo)
			End If
			For Each n In nodeList
				className = n.GetAttribute("Class")
				assyName = n.GetAttribute("AssemblyFile")
			Next
			GetPluginInfo = True
		Catch ex As Exception
			m_msgList.Add("Error in GetPluginInfo:" & ex.Message & "; " & strPluginInfo)
			GetPluginInfo = False
		End Try
	End Function

	''' <summary>
	''' Loads the specifed dll
	''' </summary>
	''' <param name="className">Name of class to load (from GetPluginInfo)</param>
	''' <param name="assyName">Name of assembly to load (from GetPluginInfo)</param>
	''' <returns>An object referencing the specified dll</returns>
	''' <remarks></remarks>
	Private Function LoadObject(ByVal className As String, ByVal assyName As String) As Object
		Dim obj As Object = Nothing
		m_msgList.Clear()

		Try
			'Build instance of tool runner subclass from class name and assembly file name.
			Dim a As System.Reflection.Assembly
			a = System.Reflection.Assembly.LoadFrom(GetPluginInfoFilePath(assyName))
			Dim t As Type = a.GetType(className, False, True)
			obj = Activator.CreateInstance(t)
		Catch ex As Exception
			''Catch any exceptions
			m_msgList.Add("clsPluginLoader.LoadObject(), exception: " & ex.Message)
		End Try
		LoadObject = obj
	End Function

	''' <summary>
	''' Loads a tool runner object
	''' </summary>
	''' <param name="ToolName">Name of tool</param>
	''' <returns>An object meeting the IToolRunner interface</returns>
	''' <remarks></remarks>
	Public Function GetToolRunner(ByVal ToolName As String) As AnalysisManagerBase.IToolRunner

		Dim xpath As String = "//ToolRunners/ToolRunner[@Tool='" & ToolName.ToLower & "']"

		Dim className As String = ""
		Dim assyName As String = ""
		Dim myToolRunner As AnalysisManagerBase.IToolRunner = Nothing

		If GetPluginInfo(xpath, className, assyName) Then

#If PLUGIN_DEBUG_MODE_ENABLED Then
			myToolRunner = DebugModeGetToolRunner(className)
			If Not myToolRunner Is Nothing Then
				Return myToolRunner
			End If
#End If

			Dim obj As Object = LoadObject(className, assyName)
			If Not obj Is Nothing Then
				Try
					myToolRunner = DirectCast(obj, AnalysisManagerBase.IToolRunner)
				Catch ex As Exception
					''Catch any exceptions
					m_msgList.Add(ex.Message)
				End Try
			End If
		End If
		m_SummaryFile.Add("Loaded ToolRunner: " & className & " from " & assyName)
		Return myToolRunner
	End Function

	''' <summary>
	''' Loads a resourcer object
	''' </summary>
	''' <param name="ToolName">Name of analysis tool</param>
	''' <returns>An object meeting the IAnalysisResources interface</returns>
	''' <remarks></remarks>
	Public Function GetAnalysisResources(ByVal ToolName As String) As AnalysisManagerBase.IAnalysisResources

		Dim xpath As String = "//Resourcers/Resourcer[@Tool='" & ToolName & "']"
		Dim className As String = ""
		Dim assyName As String = ""
		Dim myModule As AnalysisManagerBase.IAnalysisResources = Nothing
		
		If GetPluginInfo(xpath, className, assyName) Then

#If PLUGIN_DEBUG_MODE_ENABLED Then
			myModule = DebugModeGetAnalysisResources(className)
			If Not myModule Is Nothing Then
				Return myModule
			End If
#End If

			Dim obj As Object = LoadObject(className, assyName)
			If Not obj Is Nothing Then
				Try
					myModule = DirectCast(obj, AnalysisManagerBase.IAnalysisResources)
				Catch ex As Exception
					''Catch any exceptions
					m_msgList.Add(ex.Message)
				End Try
			End If
			m_SummaryFile.Add("Loaded resourcer: " & className & " from " & assyName)
		Else
			m_SummaryFile.Add("Unable to load resourcer for tool " & ToolName)
		End If
		Return myModule
	End Function

	''' <summary>
	''' Gets the path to the plugin info config file
	''' </summary>
	''' <param name="PluginInfoFileName">Name of plugin info file</param>
	''' <returns>Path to plugin info file</returns>
	''' <remarks></remarks>
	Private Function GetPluginInfoFilePath(ByVal PluginInfoFileName As String) As String
		Return System.IO.Path.Combine(m_MgrFolderPath, PluginInfoFileName)
	End Function
#End Region

End Class
