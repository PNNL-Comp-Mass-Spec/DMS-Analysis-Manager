Public Interface IStatusFile

	'Provides tools for creating and updating an analsysis status file

	'Enum for status constants
	Enum JobStatus As Short
		STATUS_IDLE = 0
		STATUS_RUNNING = 1
		STATUS_STOP = 2
		STATUS_STARTING = 3
		STATUS_CLOSING = 4
		STATUS_RETRIEVING_DATASET = 5
		STATUS_DISABLED = 6
		STATUS_FLAGFILEEXISTS = 7
	End Enum

	ReadOnly Property FileName() As String

	Property MachName() As String

	Property Status() As JobStatus

	Property Progress() As Single

	Property JobNumber() As String

	Property DatasetName() As String

	Property FileLocation() As String

	Property MaxJobDuration() As String

	Property Tool() As String

	Property StartTime() As Date



	Sub WriteStatusFile()


	Overloads Sub UpdateAndWrite(ByVal PercentComplete As Single)

	Overloads Sub UpdateAndWrite(ByVal Status As JobStatus, ByVal PercentComplete As Single)


	Overloads Sub UpdateAndWrite(ByVal Status As JobStatus, ByVal PercentComplete As Single, ByVal DTACount As Integer)


	Sub UpdateIdle()


	Sub UpdateDisabled()


	Sub UpdateFlagFileExists()


End Interface
