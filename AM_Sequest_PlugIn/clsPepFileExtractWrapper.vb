' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2005, Battelle Memorial Institute
' Started 11/04/2005
'
' Last modified 11/04/2005

Imports PeptideFileExtractor

Public Class clsPepFileExtractWrapper

	'Provides a wrapper around Ken Auberry's peptide file extractor dll to simplify use
	'Requires PeptideFileExtractor.dll to be referenced in project

#Region "Module variables"
	Private m_ExtractInProgress As Boolean = False
	Private WithEvents m_ExtractTools As IPeptideFileExtractor
	Private m_ErrMsg As String = ""
	Private m_DataPath As String = ""
	Private m_Progress As Single = 0.0	 'Percent complete, 0-100
#End Region

#Region "Properties"
	Public ReadOnly Property Progress() As Double
		Get
			Return m_Progress
		End Get
	End Property

	Public ReadOnly Property ErrMsg() As String
		Get
			Return m_ErrMsg
		End Get
	End Property

	Public Property DataPath() As String
		Get
			Return m_DataPath
		End Get
		Set(ByVal Value As String)
			m_DataPath = Value
		End Set
	End Property
#End Region

#Region "Public Methods"
	Public Sub New(ByVal DataPath As String)

		m_DataPath = DataPath

	End Sub

	Public Function PerformExtraction(ByVal RootFileName As String) As Boolean

		'Performs the peptide extraction

		'Setup the startup parameters
		Dim StartParams As New clsPeptideFileExtractordb.StartupArguments(m_DataPath, RootFileName)

		'NOTE: Function only used if AM is doing data extraction

		With StartParams
			.ExpandMultiORF = True
			.FilterEFS = False
			.FHTFilterScoreThreshold = 0.1
			.FHTXCorrThreshold = 0.0
			.SynXCorrThreshold = 1.5
			.SynFilterScoreThreshold = 0.1
			.MakeIRRFile = False
			.MakeNLIFile = True
		End With

		'Verify the concatenated _out.txt file exists
		If Not StartParams.CatOutFileExists Then
			m_ErrMsg = "Concatenated Out file not found"
			Return False
		End If

		'Setup the extractor and start extraction process
		m_ExtractTools = New clsPeptideFileExtractordb(StartParams)

		m_ExtractInProgress = True

		Try
			'Call the dll
			m_ExtractTools.ProcessInputFile()

			'Loop until the extraction finishes
			While m_ExtractInProgress
				System.Threading.Thread.Sleep(1000)
			End While

			'extraction must have finished successfully, so exit
			Return True
		Catch ex As Exception
			m_ErrMsg = "Exception while extracting files: " & ex.Message
			Return False
		End Try

	End Function
#End Region

#Region "Private methods"
	Private Sub m_ExtractTools_EndTask() Handles m_ExtractTools.EndTask
		m_ExtractInProgress = False
	End Sub

	Private Sub m_ExtractTools_CurrentProgress(ByVal fractionDone As Double) Handles m_ExtractTools.CurrentProgress
		m_Progress = CSng(100.0 * fractionDone)
	End Sub

	Private Sub m_ExtractTools_CurrentStatus(ByVal taskString As String) Handles m_ExtractTools.CurrentStatus
		'Future use?
	End Sub
#End Region

End Class
