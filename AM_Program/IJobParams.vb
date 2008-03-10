'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2007, Battelle Memorial Institute
' Created 12/18/2007
'
' Last modified 01/16/2008
'*********************************************************************************************************

Namespace AnalysisManagerBase

	Public Interface IJobParams

		'*********************************************************************************************************
		'Interface for Analysis job param storage class
		'*********************************************************************************************************

#Region "Enums"
		'Used for job closeout
		Enum CloseOutType
			CLOSEOUT_SUCCESS = 0
			CLOSEOUT_FAILED = 1
			CLOSEOUT_NO_DTA_FILES = 2
			CLOSEOUT_NO_OUT_FILES = 3
			CLOSEOUT_NO_ANN_FILES = 5
			CLOSEOUT_NO_FAS_FILES = 6
		End Enum
#End Region

#Region "Methods"
		Function GetParam(ByVal Name As String) As String

		Function AddAdditionalParameter(ByVal ParamName As String, ByVal ParamValue As String) As Boolean
#End Region

	End Interface

End Namespace
