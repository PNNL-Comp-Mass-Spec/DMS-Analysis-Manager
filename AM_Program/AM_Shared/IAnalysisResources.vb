'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2006, Battelle Memorial Institute
' Created 06/07/2006
'
' Last modified 02/27/2008
'*********************************************************************************************************

Option Strict On

Public Interface IAnalysisResources

	'*********************************************************************************************************
	'Interface for analysis resources
	'*********************************************************************************************************

#Region "Properties"
	ReadOnly Property Message() As String
#End Region

#Region "Methods"
	Sub Setup(ByRef mgrParams As IMgrParams, ByRef jobParams As IJobParams)
	Sub Setup(ByVal mgrParams As IMgrParams, ByVal jobParams As IJobParams, ByVal statusTools As IStatusFile)
	Function GetResources() As IJobParams.CloseOutType
#End Region

End Interface


