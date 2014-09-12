'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2007, Battelle Memorial Institute
' Created 12/18/2007
'*********************************************************************************************************

Option Strict On

''' <summary>
''' Interface for the analysis job parameter storage class
''' Also has the methods for Requesting a task and Closing a task
''' </summary>
''' <remarks>Implemented in clsAnalysisJob</remarks>
Public Interface IJobParams

#Region "Enums"

	''' <summary>
	''' Job result codes
	''' </summary>
	''' <remarks></remarks>
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

	''' <summary>
	''' Adds (or updates) a job parameter
	''' </summary>
	''' <param name="ParamSection">Section name for parameter</param>
	''' <param name="ParamName">Name of parameter</param>
	''' <param name="ParamValue">Value for parameter</param>
	''' <returns>True if success, False if an error</returns>
	''' <remarks></remarks>
	Function AddAdditionalParameter(ByVal ParamSection As String, ByVal ParamName As String, ByVal ParamValue As String) As Boolean

	''' <summary>
	''' Add new dataset name and ID to DatasetInfoList
	''' </summary>
	''' <param name="DatasetName"></param>
	''' <param name="DatasetID"></param>
	''' <remarks></remarks>
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

	''' <summary>
	''' Contact the Pipeline database to close the analysis job
	''' </summary>
	''' <param name="CloseOut"></param>
	''' <param name="CompMsg"></param>
	''' <remarks>Implemented in clsAnalysisJob</remarks>
	Sub CloseTask(ByVal CloseOut As IJobParams.CloseOutType, ByVal CompMsg As String)

	''' <summary>
	''' Contact the Pipeline database to close the analysis job
	''' </summary>
	''' <param name="CloseOut"></param>
	''' <param name="CompMsg"></param>
	''' <remarks>Implemented in clsAnalysisJob</remarks>
	Sub CloseTask(ByVal CloseOut As IJobParams.CloseOutType, ByVal CompMsg As String, ByVal EvalCode As Integer, ByVal EvalMessage As String)

	''' <summary>
	''' Uses the "ToolName" and "StepTool" entries in m_JobParamsTable to generate the tool name for the current analysis job
	''' Example tool names are "Sequest" or "DTA_Gen (Sequest)" or "DataExtractor (XTandem)"
	''' </summary>
	''' <returns>Tool name</returns>
	''' <remarks></remarks>
	Function GetCurrentJobToolDescription() As String

	''' <summary>
	''' Gets a job parameter with the given name (in any parameter section)
	''' </summary>
	''' <param name="Name">Key name for parameter</param>
	''' <returns>Value for specified parameter; empty string if not found</returns>
	''' <remarks></remarks>
	Function GetParam(ByVal Name As String) As String

	''' <summary>
	''' Gets a job parameter with the given name, preferentially using the specified parameter section
	''' </summary>
	''' <param name="Section">Section name for parameter</param>
	''' <param name="Name">Key name for parameter</param>
	''' <returns>Value for specified parameter; empty string if not found</returns>
	''' <remarks></remarks>
	Function GetParam(ByVal Section As String, ByVal Name As String) As String

	''' <summary>
	''' Gets a job parameter with the given name (in any parameter section)
	''' </summary>
	''' <param name="Name">Key name for parameter</param>
	''' <returns>Value for specified parameter; ValueIfMissing if not found</returns>
	''' <remarks>If the value associated with the parameter is found, yet is not True or False, then an exception will be occur; the calling procedure must handle this exception</remarks>
	Function GetJobParameter(ByVal Name As String, ByVal ValueIfMissing As Boolean) As Boolean
	Function GetJobParameter(ByVal Name As String, ByVal ValueIfMissing As String) As String
	Function GetJobParameter(ByVal Name As String, ByVal ValueIfMissing As Integer) As Integer
	Function GetJobParameter(ByVal Name As String, ByVal ValueIfMissing As Short) As Short
	Function GetJobParameter(ByVal Name As String, ByVal ValueIfMissing As Single) As Single

	''' <summary>
	''' Gets a job parameter with the given name, preferentially using the specified parameter section
	''' </summary>
	''' <param name="Section">Section name for parameter</param>
	''' <param name="Name">Key name for parameter</param>
	''' <param name="ValueIfMissing">Value to return if the parameter is not found</param>
	''' <returns>Value for specified parameter; ValueIfMissing if not found</returns>
	Function GetJobParameter(ByVal Section As String, ByVal Name As String, ByVal ValueIfMissing As Boolean) As Boolean
	Function GetJobParameter(ByVal Section As String, ByVal Name As String, ByVal ValueIfMissing As String) As String
	Function GetJobParameter(ByVal Section As String, ByVal Name As String, ByVal ValueIfMissing As Integer) As Integer
	Function GetJobParameter(ByVal Section As String, ByVal Name As String, ByVal ValueIfMissing As Single) As Single

	''' <summary>
	''' Remove a filename that was previously added to ResultFilesToSkip
	''' </summary>
	''' <param name="FileName"></param>
	''' <remarks></remarks>
	Sub RemoveResultFileToSkip(ByVal FileName As String)

	''' <summary>
	''' Requests a task from the database
	''' </summary>
	''' <returns>Enum indicating if task was found</returns>
	''' <remarks></remarks>
	Function RequestTask() As clsDBTask.RequestTaskResult

	''' <summary>
	''' Add/updates the value for the given parameter (searches all sections)
	''' </summary>
	''' <param name="KeyName">Parameter name</param>
	''' <param name="Value">Parameter value</param>
	''' <remarks></remarks>
	Sub SetParam(ByVal KeyName As String, ByVal Value As String)

	''' <summary>
	''' Add/updates the value for the given parameter
	''' </summary>
	''' <param name="Section">Section name</param>
	''' <param name="KeyName">Parameter name</param>
	''' <param name="Value">Parameter value</param>
	''' <remarks></remarks>
	Sub SetParam(ByVal Section As String, ByVal KeyName As String, ByVal Value As String)

#End Region

End Interface


