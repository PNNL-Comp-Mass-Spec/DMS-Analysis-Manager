Public Interface IMgrParams

	Function GetParam(ByVal Section As String, ByVal Item As String) As String

	Function GetParam(ByVal Item As String) As String

	Sub SetParam(ByVal Section As String, ByVal Name As String, ByVal Value As String)

	Sub SetParam(ByVal Name As String, ByVal Value As String)

	Sub SetSection(ByVal Name As String)

End Interface

