'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2007, Battelle Memorial Institute
' Created 12/18/2007
'
' Last modified 01/16/2008
'*********************************************************************************************************

Option Strict On

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
			CLOSEOUT_NO_PARAM_FILE = 7
			CLOSEOUT_NO_SETTINGS_FILE = 8
			CLOSEOUT_NO_MODDEFS_FILE = 9
			CLOSEOUT_NO_MASSCORRTAG_FILE = 10
			CLOSEOUT_NO_XT_FILES = 12
            CLOSEOUT_NO_INSP_FILES = 13
            CLOSEOUT_FILE_NOT_FOUND = 14
            CLOSEOUT_ERROR_ZIPPING_FILE = 15
			CLOSEOUT_NO_DATA = 20
		End Enum
#End Region

#Region "Methods"
		Function GetCurrentJobToolDescription() As String

		Function GetParam(ByVal Name As String) As String
		Function GetParam(ByVal Section As String, ByVal Name As String) As String

		Function AddAdditionalParameter(ByVal ParamSection As String, ByVal ParamName As String, ByVal ParamValue As String) As Boolean

		Sub SetParam(ByVal Section As String, ByVal KeyName As String, ByVal Value As String)
#End Region

	End Interface

End Namespace
