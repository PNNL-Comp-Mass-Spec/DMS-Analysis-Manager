Public Interface IJobParams
	'Used for job closeout
	Enum CloseOutType
		CLOSEOUT_SUCCESS = 0
		CLOSEOUT_FAILED = 1
		CLOSEOUT_NO_DTA_FILES = 2
		CLOSEOUT_NO_OUT_FILES = 3
		'		CLOSEOUT_NO_HTML_FILES = 4
		CLOSEOUT_NO_ANN_FILES = 5
		CLOSEOUT_NO_FAS_FILES = 6
	End Enum

	'******************************************************************
	'
	'	Parameters presently defined:
	'		"jobNum"	--	Job number
	'		"datasetNum"	--	Dataset number
	'		"datasetFolderName"	--	Name of dataset folder on storage server (normally same as dataset number)
	'		"datasetFolderStoragePath"	--	Path to dataset folder on storage server (ie, \\proto-2\SWT_LCQ2\)
	'		"transferFolderPath"	--	Results transfer folder on storage server (ie, DMS3_Xfer\)
	'		"parmFileName"	-- Parameter file name
	'		"parmFileStoragePath"	--	Location where param file storage is located (not used for Sequest)
	'		"settingsFileName"	--	Name of settings file
	'		"settingsFileStoragePath"	--	Settings file storage location
	'		"organismDBName"	-- Organism database name
	'		"organismDBStoragePath"	--	Storage path for organism database
	'		"instClass"	-- Instrument class for instrument that dataset was created on
	'		"comment"	--	Misc comments
	'		"tool"	-- Analysis tool for this job
	'		"priority"	-- Job priority
	'
	'	The above parameters are the minimum set. Additional parameters defined by V_Analysis_Job_Additional_Parameters may 
	'		also be loaded into the job parameter dictionary
	'
	'******************************************************************

	Function GetParam(ByVal Name As String) As String

	Function AddAdditionalParameter(ByVal ParamName As String, ByVal ParamValue As String) As Boolean

End Interface
