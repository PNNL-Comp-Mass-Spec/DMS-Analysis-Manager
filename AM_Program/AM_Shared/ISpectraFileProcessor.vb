'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2006, Battelle Memorial Institute
' Created 06/07/2006
'
' Last modified 07/29/2008
'*********************************************************************************************************

Option Strict On

Public Interface ISpectraFileProcessor
	'*********************************************************************************************************
	'Defines minimum required functionality for classes that will generate spectra files
	'*********************************************************************************************************

#Region "Enums"
	Enum ProcessResults
		'Return values for MakeDTA and Abort functions
		SF_SUCCESS = 0		'Operation succeeded
		SF_FAILURE = -1		'Operation failed
		SF_NO_FILES_CREATED = -2		'Spectra file creation didn't fail, but no output files were created
		SF_ABORTED = -3		'Spectra file creation aborted
	End Enum

	Enum ProcessStatus
		'Return value for status property
		SF_STARTING		'Plugin initialization in progress
		SF_RUNNING		'Plugin is attempting to do its job
		SF_COMPLETE		'Plugin successfully completed its job
		SF_ERROR		'There was an error somewhere
		SF_ABORTING		'An ABORT command has been received; plugin shutdown in progress
	End Enum
#End Region

#Region "Structures"
	Structure InitializationParams
		Dim DebugLevel As Integer
		Dim MgrParams As IMgrParams
		Dim JobParams As IJobParams
		Dim StatusTools As IStatusFile
		Dim WorkDir As String
		Dim DatasetName As String
	End Structure
#End Region

#Region "Properties"
	''' <summary>
	''' Allows calling program to get current status
	''' </summary>
	ReadOnly Property Status() As ISpectraFileProcessor.ProcessStatus

	''' <summary>
	''' Allows calling program to determine if DTA creation succeeded
	''' </summary>
	ReadOnly Property Results() As ISpectraFileProcessor.ProcessResults

	''' <summary>
	''' Error message describing any errors encountered
	''' </summary>
	ReadOnly Property ErrMsg() As String

	''' <summary>
	''' Allows control of debug information verbosity; 0=minimum, 5=maximum verbosity
	''' </summary>
	Property DebugLevel() As Integer

	''' <summary>
	''' Path to the program used to create .DTA files
	''' </summary>
	ReadOnly Property DtaToolNameLoc() As String

	''' <summary>
	''' Count of spectra files that have been created
	''' </summary>
	ReadOnly Property SpectraFileCount() As Integer

	''' <summary>
	'''  Percent complete (Value between 0 and 100)
	''' </summary>
	ReadOnly Property Progress() As Single

	''' <summary>
	''' Machine-specific parameters, such as file locations
	''' </summary>
	WriteOnly Property MgrParams() As IMgrParams	'

	''' <summary>
	''' Job-specific parameters
	''' </summary>
	WriteOnly Property JobParams() As IJobParams	'

	''' <summary>
	''' Interface for updating task status
	''' </summary>
	WriteOnly Property StatusTools() As IStatusFile	 '
#End Region

#Region "Methods"
    Sub Setup(ByVal InitParams As InitializationParams, toolRunner As clsAnalysisToolRunnerBase)   'Initializes parameters. Must be called before executing Start()

	Function Start() As ISpectraFileProcessor.ProcessStatus	 'Starts the spectra file creation process

	Function Abort() As ISpectraFileProcessor.ProcessStatus	 'Aborts spectra file creation
#End Region

End Interface


