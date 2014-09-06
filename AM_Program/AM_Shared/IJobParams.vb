'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2007, Battelle Memorial Institute
' Created 12/18/2007
'
' Last modified 01/16/2008
'*********************************************************************************************************

Option Strict On

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
		CLOSEOUT_MZML_FILE_NOT_IN_CACHE = 16
		CLOSEOUT_NO_DATA = 20
	End Enum
#End Region

#Region "Properties"

	ReadOnly Property DatasetInfoList As Dictionary(Of String, Integer)
	ReadOnly Property ResultFilesToKeep As SortedSet(Of String)
	ReadOnly Property ResultFilesToSkip As SortedSet(Of String)
	ReadOnly Property ResultFileExtensionsToSkip As SortedSet(Of String)
	ReadOnly Property ServerFilesToDelete As SortedSet(Of String)

#End Region

#Region "Methods"

	Function AddAdditionalParameter(ByVal ParamSection As String, ByVal ParamName As String, ByVal ParamValue As String) As Boolean

	Sub AddDatasetInfo(ByVal DatasetName As String, ByVal DatasetID As Integer)

	''' <summary>
	''' Add a filename to definitely move to the results folder
	''' </summary>
	''' <param name="FileName"></param>
	''' <remarks></remarks>
	Sub AddResultFileToKeep(ByVal FileName As String)

	''' <summary>
	''' Add a file to be deleted from the storage server (requires full file path)
	''' </summary>
	''' <param name="FilePath">Full path to the file</param>
	''' <remarks></remarks>
	Sub AddServerFileToDelete(ByVal FilePath As String)

	''' <summary>
	''' Add a filename to not move to the results folder
	''' </summary>
	''' <param name="FileName"></param>
	''' <remarks></remarks>
	Sub AddResultFileToSkip(ByVal FileName As String)

	''' <summary>
	''' Add a filename extension to not move to the results folder
	''' </summary>
	''' <param name="Extension"></param>
	''' <remarks>Can be a file extension (like .raw) or even a partial file name like _peaks.txt</remarks>
	Sub AddResultFileExtensionToSkip(ByVal Extension As String)

	Sub CloseTask(ByVal CloseOut As IJobParams.CloseOutType, ByVal CompMsg As String)
	Sub CloseTask(ByVal CloseOut As IJobParams.CloseOutType, ByVal CompMsg As String, ByVal EvalCode As Integer, ByVal EvalMessage As String)

	Function GetCurrentJobToolDescription() As String

	Function GetParam(ByVal Name As String) As String
	Function GetParam(ByVal Section As String, ByVal Name As String) As String

	Function GetJobParameter(ByVal Name As String, ByVal ValueIfMissing As Boolean) As Boolean
	Function GetJobParameter(ByVal Name As String, ByVal ValueIfMissing As String) As String
	Function GetJobParameter(ByVal Name As String, ByVal ValueIfMissing As Integer) As Integer
	Function GetJobParameter(ByVal Name As String, ByVal ValueIfMissing As Short) As Short
	Function GetJobParameter(ByVal Name As String, ByVal ValueIfMissing As Single) As Single

	Function GetJobParameter(ByVal Section As String, ByVal Name As String, ByVal ValueIfMissing As Boolean) As Boolean
	Function GetJobParameter(ByVal Section As String, ByVal Name As String, ByVal ValueIfMissing As String) As String
	Function GetJobParameter(ByVal Section As String, ByVal Name As String, ByVal ValueIfMissing As Integer) As Integer
	Function GetJobParameter(ByVal Section As String, ByVal Name As String, ByVal ValueIfMissing As Single) As Single

	Sub RemoveResultFileToSkip(ByVal FileName As String)

	Function RequestTask() As clsDBTask.RequestTaskResult

	Sub SetParam(ByVal KeyName As String, ByVal Value As String)
	Sub SetParam(ByVal Section As String, ByVal KeyName As String, ByVal Value As String)

#End Region

End Interface


