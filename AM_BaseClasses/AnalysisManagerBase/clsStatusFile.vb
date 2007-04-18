Imports System.IO
Imports System.Xml


Public Class clsStatusFile
	Implements IStatusFile


	'Provides tools for creating and updating an analysis status file

	Public Sub New(ByVal FileLocation As String, ByVal MaxDuration As String)
		m_FileLoc = FileLocation
		m_Status = IStatusFile.JobStatus.STATUS_IDLE
		m_Progress = 0
		m_DTACount = 0
		m_DSName = ""
		m_JobNumber = ""
		m_MaxJobDuration = MaxDuration
		m_Tool = ""
	End Sub

#Region "Module variables"

	'Status file name and location
	Private m_FileLoc As String

	'Analysis machine name
	Private m_MachName As String

	'Status value
	Private m_Status As IStatusFile.JobStatus

	'Progess (in percent)
	Private m_Progress As Single

	'Job number
	Private m_JobNumber As String

	'Dataset name
	Private m_DSName As String

	'Number of DTA files created
	Private m_DTACount As Integer

	'Max job duration
	Private m_MaxJobDuration As String

	'Analysis Tool
	Private m_Tool As String

	'Job start time
	Private m_StartTime As Date

#End Region

#Region "Public properties"

	Public ReadOnly Property FileName() As String Implements IStatusFile.FileName
		Get
			Return m_FileLoc
		End Get
	End Property

	Public Property MachName() As String Implements IStatusFile.MachName
		Get
			Return m_MachName
		End Get
		Set(ByVal Value As String)
			m_MachName = Value
		End Set
	End Property

	Public Property Status() As IStatusFile.JobStatus Implements IStatusFile.Status
		Get
			Return m_Status
		End Get
		Set(ByVal Value As IStatusFile.JobStatus)
			m_Status = Value
		End Set
	End Property

	Public Property Progress() As Single Implements IStatusFile.Progress
		Get
			Return m_Progress
		End Get
		Set(ByVal Value As Single)
			m_Progress = Value
		End Set
	End Property

	Public Property JobNumber() As String Implements IStatusFile.JobNumber
		Get
			Return m_JobNumber
		End Get
		Set(ByVal Value As String)
			m_JobNumber = Value
		End Set
	End Property

	Public Property DatasetName() As String Implements IStatusFile.DatasetName
		Get
			Return m_DSName
		End Get
		Set(ByVal Value As String)
			m_DSName = Value
		End Set
	End Property

	Public Property FileLocation() As String Implements IStatusFile.FileLocation
		Get
			Return m_FileLoc
		End Get
		Set(ByVal Value As String)
			m_FileLoc = Value
		End Set
	End Property

	Public Property MaxJobDuration() As String Implements IStatusFile.MaxJobDuration
		Get
			Return m_MaxJobDuration
		End Get
		Set(ByVal Value As String)
			m_MaxJobDuration = Value
		End Set
	End Property

	Public Property Tool() As String Implements IStatusFile.Tool
		Get
			Return m_Tool
		End Get
		Set(ByVal Value As String)
			m_Tool = Value
		End Set
	End Property

	Public Property StartTime() As Date Implements IStatusFile.StartTime
		Get
			Return m_StartTime
		End Get
		Set(ByVal Value As Date)
			m_StartTime = Value
		End Set
	End Property

#End Region

	Private Function ConvertStatusToString(ByVal StatusEnum As IStatusFile.JobStatus) As String

		'Converts a status enum to a string
		Select Case StatusEnum
			Case IStatusFile.JobStatus.STATUS_CLOSING
				Return "Closing"
			Case IStatusFile.JobStatus.STATUS_IDLE
				Return "Idle"
			Case IStatusFile.JobStatus.STATUS_RUNNING
				Return "Running"
			Case IStatusFile.JobStatus.STATUS_STARTING
				Return "Starting"
			Case IStatusFile.JobStatus.STATUS_STOP
				Return "Stopped"
			Case IStatusFile.JobStatus.STATUS_RETRIEVING_DATASET
				Return "Retrieving"
			Case IStatusFile.JobStatus.STATUS_DISABLED
				Return "Disabled"
			Case IStatusFile.JobStatus.STATUS_FLAGFILEEXISTS
				Return "FlagFile"
			Case Else
				'Should never get here
		End Select

	End Function

	Public Sub WriteStatusFile() Implements IStatusFile.WriteStatusFile

		'Writes a status file for external monitor to read
		Dim XWriter As XmlTextWriter
		Dim TempFileLoc As String		'Use a temporary file while writing is in progress

		'Set up the XML writer
		Try
			TempFileLoc = Path.GetDirectoryName(m_FileLoc)
			TempFileLoc = Path.Combine(TempFileLoc, "TempOut.xml")
			XWriter = New XmlTextWriter(TempFileLoc, System.Text.Encoding.UTF8)
			XWriter.Formatting = Formatting.Indented
			XWriter.Indentation = 2

			'Write the file
			XWriter.WriteStartDocument(True)
			XWriter.WriteComment("Analysis manager job status")
			'General job information
			'Root level element
			XWriter.WriteStartElement("Root")
			XWriter.WriteStartElement("General")
			XWriter.WriteElementString("LastUpdate", Now().ToString)
			XWriter.WriteElementString("Machine", m_MachName)
			XWriter.WriteElementString("Status", ConvertStatusToString(m_Status))
			XWriter.WriteElementString("Progress", m_Progress.ToString("##0.00"))
			XWriter.WriteElementString("JobNumber", m_JobNumber)
			XWriter.WriteElementString("DSName", m_DSName)
			'			XWriter.WriteElementString("MaxDuration", m_MaxJobDuration)
			XWriter.WriteElementString("Tool", m_Tool)
			XWriter.WriteElementString("Duration", GetRunTime.ToString)
			XWriter.WriteEndElement()
			'Sequest job information
			XWriter.WriteStartElement("Sequest")
			XWriter.WriteElementString("DTAs", m_DTACount.ToString)
			XWriter.WriteEndElement()
			XWriter.WriteEndElement()
			'Close the document
			XWriter.WriteEndDocument()
			XWriter.Close()

			XWriter = Nothing
			GC.Collect()
			GC.WaitForPendingFinalizers()

			'Copy the temporary file to the real one
			File.Copy(TempFileLoc, m_FileLoc, True)
			File.Delete(TempFileLoc)
		Catch
			'Do nothing
		End Try

	End Sub

	Public Overloads Sub UpdateAndWrite(ByVal PercentComplete As Single) Implements IStatusFile.UpdateAndWrite

		m_Progress = PercentComplete
		Me.WriteStatusFile()

	End Sub

	Public Overloads Sub UpdateAndWrite(ByVal Status As IStatusFile.JobStatus, ByVal PercentComplete As Single) Implements IStatusFile.UpdateAndWrite

		m_Status = Status
		m_Progress = PercentComplete
		Me.WriteStatusFile()

	End Sub

	Public Overloads Sub UpdateAndWrite(ByVal Status As IStatusFile.JobStatus, ByVal PercentComplete As Single, _
	 ByVal DTACount As Integer) Implements IStatusFile.UpdateAndWrite

		m_Status = Status
		m_Progress = PercentComplete
		m_DTACount = DTACount
		Me.WriteStatusFile()

	End Sub

	Public Sub UpdateIdle() Implements IStatusFile.UpdateIdle

		m_Status = IStatusFile.JobStatus.STATUS_IDLE
		m_Progress = 0
		m_DTACount = 0
		m_DSName = ""
		m_JobNumber = ""
		m_Tool = ""
		Me.WriteStatusFile()

	End Sub

	Public Sub UpdateDisabled() Implements IStatusFile.UpdateDisabled

		m_Status = IStatusFile.JobStatus.STATUS_DISABLED
		m_Progress = 0
		m_DTACount = 0
		m_DSName = ""
		m_JobNumber = ""
		m_Tool = ""
		Me.WriteStatusFile()

	End Sub

	Public Sub UpdateFlagFileExists() Implements IStatusFile.UpdateFlagFileExists

		m_Status = IStatusFile.JobStatus.STATUS_FLAGFILEEXISTS
		m_Progress = 0
		m_DTACount = 0
		m_DSName = ""
		m_JobNumber = ""
		m_Tool = ""
		Me.WriteStatusFile()

	End Sub

	Private Function GetRunTime() As Decimal

		'Returns an integer representing the number of hours since the job started running
		Dim ETime As Decimal = DateDiff(DateInterval.Minute, m_StartTime, Now)
		Return Math.Round(ETime / 60, 1)

	End Function

End Class
