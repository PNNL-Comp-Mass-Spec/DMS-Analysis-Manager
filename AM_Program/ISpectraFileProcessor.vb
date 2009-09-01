'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2006, Battelle Memorial Institute
' Created 06/07/2006
'
' Last modified 07/29/2008
'*********************************************************************************************************

Imports System.Collections.Specialized

Namespace AnalysisManagerBase

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
			'Dim SourceFolderPath As String
			'Dim OutputFolderPath As String
			'Dim MiscParams As StringDictionary
			Dim DebugLevel As Integer
            Dim MgrParams As IMgrParams
			Dim JobParams As IJobParams
			Dim StatusTools As IStatusFile
		End Structure
#End Region

#Region "Properties"
		'		Property SourceFolderPath() As String	' (in)  – path to folder containing the raw spectra file

		'		Property OutputFolderPath() As String	' (in) – path to folder where generated DTAs are to be placed

		'		WriteOnly Property MiscParams() As StringDictionary	'For passing miscelleneous parameters (not presently used)

		ReadOnly Property Status() As ISpectraFileProcessor.ProcessStatus	'Allows calling program to get current status

		ReadOnly Property Results() As ISpectraFileProcessor.ProcessResults	 'Allows calling program to determine if DTA creation succeeded

		ReadOnly Property ErrMsg() As String	 'Error message describing any errors encountered

		Property DebugLevel() As Integer	'Allows control of debug information verbosity; 0=minimum, 5=maximum verbosity

        ReadOnly Property SpectraFileCount() As Integer 'Count of spectra files that have been created

		WriteOnly Property MgrParams() As IMgrParams	'Machine-specific parameters, such as file locations

		WriteOnly Property JobParams() As IJobParams	'Job-specific parameters

		WriteOnly Property StatusTools() As IStatusFile	 'Interface for updating task status
#End Region

#Region "Methods"
		Sub Setup(ByVal InitParams As InitializationParams)	 'Initializes parameters. Must be called before executing Start()

		Function Start() As ISpectraFileProcessor.ProcessStatus	 'Starts the spectra file creation process

		Function Abort() As ISpectraFileProcessor.ProcessStatus	 'Aborts spectra file creation
#End Region

	End Interface

End Namespace
