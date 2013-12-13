
Public Class clsXMLParamFileReader

	Protected mParamFilePath As String
	Protected mSections As Dictionary(Of String, Dictionary(Of String, String))

	Public ReadOnly Property ParamFilePath As String
		Get
			Return mParamFilePath
		End Get
	End Property

	Public ReadOnly Property ParameterCount As Integer
		Get
			Dim intCount As Integer = 0

			For Each section In mSections
				intCount += section.Value.Count
			Next

			Return intCount
		End Get
	End Property

	Public ReadOnly Property SectionCount As Integer
		Get
			Return mSections.Count
		End Get
	End Property

	Public Sub New(ByVal strParamFilePath As String)
		mParamFilePath = strParamFilePath

		If Not IO.File.Exists(strParamFilePath) Then
			Throw New IO.FileNotFoundException(strParamFilePath)
		End If

		mSections = CacheXMLParamFile(mParamFilePath)
	End Sub

	''' <summary>
	''' Parse an XML parameter file with the hierarchy of Section, ParamName, ParamValue 
	''' </summary>
	''' <param name="strParamFilePath"></param>
	''' <returns>Dictionary object where keys are section names and values are dictionary objects of key/value pairs</returns>
	''' <remarks></remarks>
	Protected Function CacheXMLParamFile(ByVal strParamFilePath As String) As Dictionary(Of String, Dictionary(Of String, String))

		Dim dctSections = New Dictionary(Of String, Dictionary(Of String, String))

		' Read the entire XML file into a Linq to XML XDocument object
		' Note: For this to work, the project must have a reference to System.XML.Linq
		Dim xParamFile As Xml.Linq.XDocument = Xml.Linq.XDocument.Load(strParamFilePath)

		Dim parameters As IEnumerable(Of Xml.Linq.XElement) = xParamFile.Elements()

		' Store the parameters
		CacheXMLParseSection(parameters, dctSections)

		Return dctSections

	End Function

	''' <summary>
	''' Parses the XML elements in parameters, populating dctParameters
	''' </summary>
	''' <param name="parameters">XML parameters to examine</param>
	''' <param name="dctParameters">Dictionary object where keys are section names and values are dictionary objects of key/value pairs</param>
	''' <remarks></remarks>
	Protected Sub CacheXMLParseSection(parameters As IEnumerable(Of Xml.Linq.XElement), ByRef dctParameters As Dictionary(Of String, Dictionary(Of String, String)))

		For Each parameter In parameters
			If parameter.Descendants.Count > 0 Then
				' Recursively call this function with the content
				CacheXMLParseSection(parameter.Descendants, dctParameters)
			Else
				' Store this as a parameter
				Dim strSection As String = parameter.Parent.Name.LocalName
				Dim strParamName As String = parameter.Name.LocalName
				Dim strParamValue As String = parameter.Value

				Dim dctSectionSettings As Dictionary(Of String, String) = Nothing

				If Not dctParameters.TryGetValue(strSection, dctSectionSettings) Then
					dctSectionSettings = New Dictionary(Of String, String)
					dctParameters.Add(strSection, dctSectionSettings)
				End If

				If Not dctSectionSettings.ContainsKey(strParamName) Then
					dctSectionSettings.Add(strParamName, strParamValue)
				End If
			End If
		Next

	End Sub

	Public Function GetParameter(ByVal strParameterName As String, ByVal blnValueIfMissing As Boolean) As Boolean

		Dim strValue As String = GetParameter(strParameterName, String.Empty)

		If String.IsNullOrEmpty(strValue) Then Return blnValueIfMissing

		Dim blnValue As Boolean
		If Boolean.TryParse(strValue, blnValue) Then
			Return blnValue
		End If

		Return blnValueIfMissing

	End Function

	Public Function GetParameter(ByVal strParameterName As String, ByVal strValueIfMissing As String) As String

		For Each section In mSections
			Dim strValue As String = String.Empty

			If section.Value.TryGetValue(strParameterName, strValue) Then
				Return strValue
			End If

		Next

		Return strValueIfMissing

	End Function

	Public Function GetParameterBySection(ByVal strSectionName As String, ByVal strParameterName As String, ByVal strValueIfMissing As String) As String

		Dim dctParameters = New Dictionary(Of String, String)

		If mSections.TryGetValue(strSectionName, dctParameters) Then
			Dim strValue As String = String.Empty

			If dctParameters.TryGetValue(strParameterName, strValue) Then
				Return strValue
			End If
		End If

		Return strValueIfMissing

	End Function
End Class
