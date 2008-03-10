'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 12/18/2007
'
' Last modified 01/16/2008
'*********************************************************************************************************

'*********************************************************************************************************
'Interface for manager params storage class
'*********************************************************************************************************

Namespace AnalysisManagerBase

	Public Interface IMgrParams

#Region "Methods"
		Function GetParam(ByVal ItemKey As String) As String
		Sub SetParam(ByVal ItemKey As String, ByVal ItemValue As String)
#End Region

	End Interface

End Namespace
