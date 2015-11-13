'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2006, Battelle Memorial Institute
' Created 06/07/2006
'
' Last modified 07/29/2008
'*********************************************************************************************************

Option Strict On

Public Interface IToolRunner

	'*********************************************************************************************************
	'Insert general class description here
	'*********************************************************************************************************

#Region "Properties"
	ReadOnly Property EvalCode As Integer
	ReadOnly Property EvalMessage As String

	ReadOnly Property ResFolderName() As String

	' Explanation of what happened to last operation this class performed
	' Used to report error messages
	ReadOnly Property Message() As String

	ReadOnly Property NeedToAbortProcessing() As Boolean

	' the state of completion of the job (as a percentage)
	ReadOnly Property Progress() As Single
#End Region

#Region "Methods"
    Sub Setup(mgrParams As IMgrParams, jobParams As IJobParams, StatusTools As IStatusFile, SummaryFile As clsSummaryFile)

	Function RunTool() As IJobParams.CloseOutType

#End Region

End Interface


