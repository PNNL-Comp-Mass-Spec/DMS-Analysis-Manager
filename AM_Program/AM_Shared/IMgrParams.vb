'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 12/18/2007
'
' Last modified 01/16/2008
'*********************************************************************************************************

Option Strict On

Public Interface IMgrParams

	'*********************************************************************************************************
	'Interface for manager params storage class
	'*********************************************************************************************************

	ReadOnly Property ErrMsg As String

#Region "Methods"
	Sub AckManagerUpdateRequired()
	Function DisableManagerLocally() As Boolean

	Function GetParam(ByVal ItemKey As String) As String
	Function GetParam(ByVal ItemKey As String, ByVal ValueIfMissing As String) As String
	Function GetParam(ByVal ItemKey As String, ByVal ValueIfMissing As Boolean) As Boolean
	Function GetParam(ByVal ItemKey As String, ByVal ValueIfMissing As Integer) As Integer

	Function LoadDBSettings() As Boolean
	Function LoadSettings(ByVal ConfigFileSettings As System.Collections.Generic.Dictionary(Of String, String)) As Boolean
	Sub SetParam(ByVal ItemKey As String, ByVal ItemValue As String)
#End Region

End Interface


