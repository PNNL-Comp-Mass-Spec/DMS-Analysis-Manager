'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2008, Battelle Memorial Institute
' Created 10/09/2008
'
'*********************************************************************************************************
Imports System.IO
Imports AnalysisManagerBase
Imports PeptideFileExtractor

''' <summary>
''' Perform Peptide extraction from Sequest results
''' </summary>
''' <remarks></remarks>
Public Class clsPeptideExtractWrapper

#Region "Event Handlers"
	Private Sub m_ExtractTools_EndTask() Handles m_ExtractTools.EndTask

		m_ExtractInProgress = False

	End Sub

	Private Sub m_ExtractTools_CurrentProgress(ByVal fractionDone As Double) Handles m_ExtractTools.CurrentProgress
        Const MIN_STATUS_INTERVAL_SECONDS = 3
        Const MIN_LOG_INTERVAL_SECONDS = 5
        Const MAX_LOG_INTERVAL_SECONDS = 300

        Static dtLastStatusUpdate As DateTime = DateTime.UtcNow
        Static dtLastLogTime As DateTime = DateTime.UtcNow.Subtract(New TimeSpan(0, 0, MIN_LOG_INTERVAL_SECONDS * 2))

        Dim blnUpdateLog As Boolean = False

        ' We divide the progress by 3 since creation of the FHT and SYN files takes ~33% of the time, while the remainder is spent running PHRP and PeptideProphet
        m_Progress = CSng(100.0 * fractionDone / 3.0)

        If DateTime.UtcNow.Subtract(dtLastStatusUpdate).TotalSeconds >= MIN_STATUS_INTERVAL_SECONDS Then
            dtLastStatusUpdate = DateTime.UtcNow
            m_StatusTools.UpdateAndWrite(m_Progress)
        End If

        If m_DebugLevel > 3 AndAlso DateTime.UtcNow.Subtract(dtLastLogTime).TotalSeconds >= MIN_LOG_INTERVAL_SECONDS Then
            ' Over MIN_LOG_INTERVAL_SECONDS seconds has elapsed; update the log file
            blnUpdateLog = True
        ElseIf m_DebugLevel >= 1 AndAlso DateTime.UtcNow.Subtract(dtLastLogTime).TotalSeconds >= MAX_LOG_INTERVAL_SECONDS Then
            ' Over MAX_LOG_INTERVAL_SECONDS seconds has elapsed; update the log file
            blnUpdateLog = True
        End If

        If blnUpdateLog Then
            dtLastLogTime = DateTime.UtcNow
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Extraction progress: " & m_Progress.ToString("##0.0") & "%")
        End If

    End Sub

	Private Sub m_ExtractTools_CurrentStatus(ByVal taskString As String) Handles m_ExtractTools.CurrentStatus
		'Future use?
	End Sub
#End Region

#Region "Module variables"
    Private ReadOnly m_DebugLevel As Integer
    Private ReadOnly m_MgrParams As IMgrParams
    Private ReadOnly m_JobParams As IJobParams
	Private m_ExtractInProgress As Boolean = False
	Private WithEvents m_ExtractTools As IPeptideFileExtractor
    Private m_Progress As Single = 0.0   'Percent complete, 0-100
    Private ReadOnly m_StatusTools As IStatusFile
#End Region

#Region "Events"
#End Region

#Region "Properties"
#End Region

#Region "Methods"
	''' <summary>
	''' Constructor
	''' </summary>
    ''' <param name="MgrParams">IMgrParams object containing manager settings</param>
	''' <param name="JobParams">IJobParams object containing job parameters</param>
	''' <remarks></remarks>
	Public Sub New(ByVal MgrParams As IMgrParams, ByVal JobParams As IJobParams, ByRef StatusTools As IStatusFile)

		m_JobParams = JobParams
		m_MgrParams = MgrParams
		m_DebugLevel = CInt(m_MgrParams.GetParam("debuglevel"))
		m_StatusTools = StatusTools

	End Sub

	''' <summary>
	''' Performs peptide extraction by calling extractor DLL
	''' </summary>
	''' <returns>IJobParams.CloseOutType indicating success or failure</returns>
	''' <remarks></remarks>
	Public Function PerformExtraction() As IJobParams.CloseOutType

		Dim StartParams As New clsPeptideFileExtractor.StartupArguments( _
		  m_MgrParams.GetParam("workdir"), m_JobParams.GetParam("DatasetNum"))

		With StartParams
			.ExpandMultiORF = True
			.FilterEFS = False
			.FHTFilterScoreThreshold = 0.1
			.FHTXCorrThreshold = 0.0
			.SynXCorrThreshold = 1.5
			.SynFilterScoreThreshold = 0.1
			.MakeIRRFile = False
			.MakeNLIFile = False			' Not actually used by the extractor, since class PeptideHitEntry has COMPUTE_DISCRIMINANT_SCORE = False in the PeptideFileExtractor project
		End With

		'Verify the concatenated _out.txt file exists
		If Not StartParams.CatOutFileExists Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Concatenated Out file not found")
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		'Setup the extractor and start extraction process
		m_ExtractTools = New clsPeptideFileExtractor(StartParams)

		m_ExtractInProgress = True

		Try
			'Call the dll
			If m_DebugLevel >= 1 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Beginning peptide extraction")
			End If

			m_ExtractTools.ProcessInputFile()

			'Loop until the extraction finishes
			While m_ExtractInProgress
                Threading.Thread.Sleep(2000)
			End While

			Dim Result As IJobParams.CloseOutType
			Result = TestOutputSynFile()
			If Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
				'Error messages were generated by TestOutputSynFile, so just exit
				Return Result
			End If

			'extraction must have finished successfully, so exit
			If m_DebugLevel >= 2 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Extraction complete")
			End If
			Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception while extracting files: " & ex.Message & "; " & clsGlobal.GetExceptionStackTrace(ex))
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		Finally
			'Make sure no stray objects are hanging around
			m_ExtractTools = Nothing
            Threading.Thread.Sleep(1000)    'Delay 1 second, then clean up processes
			PRISM.Processes.clsProgRunner.GarbageCollectNow()
		End Try

	End Function

	Private Function TestOutputSynFile() As IJobParams.CloseOutType

		'Verifies an _syn.txt file was created, and that valid data was found (file size > 0 bytes)
        Dim WorkFile As String = String.Empty
        Dim FoundFile = False

		'Test for presence of _syn.txt file
        Dim WorkFiles() As String = Directory.GetFiles(m_MgrParams.GetParam("workdir"))
        For Each WorkFile In WorkFiles
            If Text.RegularExpressions.Regex.IsMatch(WorkFile, "_syn.txt$", Text.RegularExpressions.RegexOptions.IgnoreCase) Then
                FoundFile = True
                Exit For
            End If
        Next
        If Not FoundFile Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsPeptideExtractor.TestOutputSynFile: No _syn.txt file found")
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        'Get the _syn.txt file size and verify it's > 0 bytes
        Dim Fi As FileInfo = New FileInfo(WorkFile)
		If Fi.Length > 0 Then
			Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
		Else
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "clsPeptideExtractor.TestOutputSynFile: No data in _syn.txt file")
            Return IJobParams.CloseOutType.CLOSEOUT_NO_DATA
		End If

	End Function
#End Region

End Class
