'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2006, Battelle Memorial Institute
' Created 06/07/2006
'
' Last modified 01/16/2008
'*********************************************************************************************************

Namespace AnalysisManagerBase

	Public Interface IStatusFile

		'*********************************************************************************************************
		'Interface used by classes that create and update analysis status file
		'*********************************************************************************************************

#Region "Enums"
		'Status constants
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
#End Region

#Region "Properties"
		ReadOnly Property FileName() As String

		Property MachName() As String

		Property Status() As JobStatus

		Property Progress() As Single

		Property JobNumber() As String

		Property DatasetName() As String

		Property FileLocation() As String

		Property Tool() As String

		Property StartTime() As Date
#End Region

#Region "Methods"
		Sub WriteStatusFile()

		Overloads Sub UpdateAndWrite(ByVal PercentComplete As Single)

		Overloads Sub UpdateAndWrite(ByVal Status As JobStatus, ByVal PercentComplete As Single)

		Overloads Sub UpdateAndWrite(ByVal Status As JobStatus, ByVal PercentComplete As Single, ByVal DTACount As Integer)

		Sub UpdateIdle()

		Sub UpdateDisabled()

		Sub UpdateFlagFileExists()
#End Region

	End Interface

End Namespace
