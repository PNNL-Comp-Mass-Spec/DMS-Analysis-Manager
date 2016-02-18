'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2006, Battelle Memorial Institute
' Created 06/07/2006
'
'*********************************************************************************************************

Option Strict On

Public Interface ISpectraFilter

	'*********************************************************************************************************
	'Defines minimum required functionality for classes that will filter spectra files
	'*********************************************************************************************************

#Region "Enums"
	Enum ProcessResults
		'Return values for MakeProcess and Abort functions
		SFILT_SUCCESS = 0				 ' Operation succeeded
		SFILT_FAILURE = -1				 ' Operation failed
		SFILT_NO_FILES_CREATED = -2		 ' Spectra filter operation didn't fail, but no output files were created
		SFILT_ABORTED = -3				 ' Spectra filter operation aborted
		SFILT_NO_SPECTRA_ALTERED = -4	 ' Spectra filter did not alter any spectra
	End Enum

	Enum ProcessStatus
		'Return value for status property
		SFILT_STARTING				'Plugin initialization in progress
		SFILT_RUNNING				'Plugin is attempting to do its job
		SFILT_COMPLETE				'Plugin successfully completed its job
		SFILT_ERROR					'There was an error somewhere
		SFILT_ABORTING				'An ABORT command has been received; plugin shutdown in progress
	End Enum
#End Region

#Region "Structures"
	Structure InitializationParams
		Dim SourceFolderPath As String
		Dim OutputFolderPath As String
		'' Unused: Dim MiscParams As Specialized.StringDictionary
		Dim DebugLevel As Integer
		Dim Logger As PRISM.Logging.ILogger
		Dim MgrParams As IMgrParams
		Dim JobParams As IJobParams
		Dim StatusTools As IStatusFile
	End Structure
#End Region

#Region "Properties"
	ReadOnly Property Status() As ISpectraFilter.ProcessStatus 'Allows calling program to get current status

	ReadOnly Property Results() As ISpectraFilter.ProcessResults  'Allows calling program to determine if DTA creation succeeded

	ReadOnly Property ErrMsg() As String  'Error message describing any errors encountered

	ReadOnly Property SpectraFileCount() As Integer	'Count of spectra files that remain after filtering
#End Region

#Region "Methods"
	Sub Setup(ByVal InitParams As InitializationParams)	 'Initializes parameters. Must be called before executing Start()

	Function Start() As ISpectraFilter.ProcessStatus  'Starts the spectra filter operation

	Function Abort() As ISpectraFilter.ProcessStatus  'Aborts spectra filter operation
#End Region

End Interface


