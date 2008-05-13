'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2006, Battelle Memorial Institute
' Created 06/07/2006
'
' Last modified 03/11/2008
'*********************************************************************************************************

Imports System.IO

Imports AnalysisManagerBase

Public Class clsDtaGenMainProcess
	Inherits clsDtaGen

	'*********************************************************************************************************
	'Main processing class for simple DTA generation
	'*********************************************************************************************************

#Region "Module variables"
	Private m_DtaToolNameLoc As String
	Private m_NumScans As Integer
	Private m_AbortRequested As Boolean = False
	Private WithEvents m_RunProgTool As clsRunDosProgram
	Private m_thThread As System.Threading.Thread
#End Region

#Region "API Declares"
	'Used for getting dta count in spectra file via ICR2LS
	Private Declare Function lopen Lib "kernel32" Alias "_lopen" (ByVal lpPathName As String, ByVal iReadWrite As Integer) As Integer
	Private Declare Function lclose Lib "kernel32" Alias "_lclose" (ByVal hFile As Integer) As Integer
	Private Declare Function XnumScans Lib "icr2ls32.dll" (ByVal FileHandle As Integer) As Integer

	'API constants
	Private Const OF_READ As Short = &H0S
	Private Const OF_READWRITE As Short = &H2S
	Private Const OF_WRITE As Short = &H1S
	Private Const OF_SHARE_COMPAT As Short = &H0S
	Private Const OF_SHARE_DENY_NONE As Short = &H40S
	Private Const OF_SHARE_DENY_READ As Short = &H30S
	Private Const OF_SHARE_DENY_WRITE As Short = &H20S
	Private Const OF_SHARE_EXCLUSIVE As Short = &H10S
#End Region

#Region "Methods"
	''' <summary>
	''' Aborts processing
	''' </summary>
	''' <returns>ProcessStatus value indicating process was aborted</returns>
	''' <remarks></remarks>
	Public Overrides Function Abort() As ISpectraFileProcessor.ProcessStatus
		m_AbortRequested = True
	End Function

	''' <summary>
	''' Starts DTA creation
	''' </summary>
	''' <returns>ProcessStatus value indicating success or failure</returns>
	''' <remarks></remarks>
	Public Overrides Function Start() As ISpectraFileProcessor.ProcessStatus

		m_Status = ISpectraFileProcessor.ProcessStatus.SF_STARTING

		'Verify necessary files are in specified locations
		If Not InitSetup() Then
			m_Results = ISpectraFileProcessor.ProcessResults.SF_FAILURE
			m_Status = ISpectraFileProcessor.ProcessStatus.SF_ERROR
			Return m_Status
		End If

		'Read the settings file
		If Not ReadSettingsFile(m_SettingsFileName, m_SourceFolderPath) Then
			m_Results = ISpectraFileProcessor.ProcessResults.SF_FAILURE
			m_Status = ISpectraFileProcessor.ProcessStatus.SF_ERROR
			Return m_Status
		End If

		'Verify DTA creation tool exists
		m_DtaToolNameLoc = Path.Combine(m_MgrParams.GetParam("lcqdtaloc"), _
		  m_Settings.GetParam("DtaGenerator", "DtaGenerator", "Bad_Program.exe"))
		If Not VerifyFileExists(m_DtaToolNameLoc) Then
			m_Results = ISpectraFileProcessor.ProcessResults.SF_FAILURE
			m_Status = ISpectraFileProcessor.ProcessStatus.SF_ERROR
			Return m_Status
		End If

		'Make the DTA files (the process runs in a separate thread)
		Try
			m_thThread = New System.Threading.Thread(AddressOf MakeDTAFilesThreaded)
			m_thThread.Start()
			m_Status = ISpectraFileProcessor.ProcessStatus.SF_RUNNING
		Catch ex As Exception
			m_ErrMsg = "Error calling MakeDTAFiles"
			m_Status = ISpectraFileProcessor.ProcessStatus.SF_ERROR
		End Try

		Return m_Status

	End Function

	''' <summary>
	''' Tests for existence of .raw file in specified location
	''' </summary>
	''' <param name="WorkDir">Directory where .raw file should be found</param>
	''' <param name="DSName">Name of dataset being processed</param>
	''' <returns>TRUE if file found; FALSE otherwise</returns>
	''' <remarks></remarks>
	Private Function VerifyRawFileExists(ByVal WorkDir As String, ByVal DSName As String) As Boolean

		'Verifies a .raw file exists in specfied directory
		If File.Exists(Path.Combine(WorkDir, DSName & ".raw")) Then
			m_ErrMsg = ""
			Return True
		Else
			m_ErrMsg = "Data file " & DSName & ".raw not found in working directory"
			Return False
		End If

	End Function

	''' <summary>
	''' Initializes the class
	''' </summary>
	''' <returns>TRUE for success; FALSE for failure</returns>
	''' <remarks></remarks>
	Protected Overrides Function InitSetup() As Boolean

		'Verifies all necessary files exist in the specified locations

		If m_DebugLevel > 0 Then
			m_Logger.PostEntry("clsDtaGenMainProcess.InitSetup: Initializing DTA generator setup", _
			  PRISM.Logging.ILogger.logMsgType.logDebug, True)
		End If

		'Do tests specfied in base class
		If Not MyBase.InitSetup Then Return False

		'Misc parameters exist?
		If m_MiscParams Is Nothing Then
			m_ErrMsg = "No misc parameters specified"
			Return False
		End If

		'Raw data file exists?
		If Not VerifyRawFileExists(m_SourceFolderPath, m_DSName) Then Return False 'Error message handled by VerifyRawFileExists

		''DTA creation tool exists?
		'm_DtaToolNameLoc = Path.Combine(m_MgrParams.GetParam("commonfileandfolderlocations", "lcqdtaloc"), _
		'	m_settings.GetParam("DtaGenerator", "DtaGenerator", "Bad_Program.exe"))
		'If Not VerifyFileExists(m_DtaToolNameLoc) Then Return False 'Error message handled by VerifyFileExists

		'If we got to here, there was no problem
		Return True

	End Function

	''' <summary>
	''' Determines the maximum scan number in the .raw file
	''' </summary>
	''' <param name="RawFile">Data file name</param>
	''' <returns>Number of scans found</returns>
	''' <remarks>Uses ICR2LS function to read data file</remarks>
	Protected Overridable Function GetMaxScan(ByVal RawFile As String) As Integer

		'Uses ICR2LS to get the maximum number of scans in a .raw file
		Dim FileHandle As Integer
		Dim NumScans As Integer
		Dim Dummy As Integer

		FileHandle = lopen(RawFile, OF_READ)
		If FileHandle = 0 Then Return -1 'Bad lopen
		NumScans = XnumScans(FileHandle)
		Dummy = lclose(FileHandle)
		If Dummy <> 0 Then Return -1 'Bad lclose

		Return NumScans

		'**************************************************************************************************************************************************************
		'	Alternate method of generating DTA files using XCalibur OCX. Causes .raw file lock. Code left in place in case file lock problem solved by an update to OCX
		'**************************************************************************************************************************************************************
		'Dim NumScans As Integer
		'Dim StartScan As Integer
		'Dim StopScan As Integer

		'Dim XRawFile As XRAWFILE2Lib.XRawfile
		'XRawFile = New XRAWFILE2Lib.XRawfile
		'XRawFile.Open(RawFile)
		'XRawFile.SetCurrentController(0, 1)
		'XRawFile.GetNumSpectra(NumScans)
		'XRawFile.GetFirstSpectrumNumber(StartScan)
		'XRawFile.GetLastSpectrumNumber(StopScan)
		'XRawFile.Close()

		'XRawFile = Nothing
		''Pause and garbage collect to allow release of file lock on .raw file
		'System.Threading.Thread.Sleep(20000)		'20 second delay
		'GC.Collect()
		'GC.WaitForPendingFinalizers()
		'**************************************************************************************************************************************************************
		'End alternate method of DTA creation
		'**************************************************************************************************************************************************************

		Return NumScans

	End Function

	''' <summary>
	''' Thread for creation of DTA files
	''' </summary>
	''' <remarks></remarks>
	Protected Overridable Sub MakeDTAFilesThreaded()

		m_Status = ISpectraFileProcessor.ProcessStatus.SF_RUNNING
		If Not MakeDTAFiles() Then
			If m_Status <> ISpectraFileProcessor.ProcessStatus.SF_ABORTING Then
				m_Results = ISpectraFileProcessor.ProcessResults.SF_FAILURE
				m_Status = ISpectraFileProcessor.ProcessStatus.SF_ERROR
			End If
		End If

		'Remove any files with non-standard file names (extract_msn bug)
		If Not DeleteNonDosFiles() Then
			If m_Status <> ISpectraFileProcessor.ProcessStatus.SF_ABORTING Then
				m_Results = ISpectraFileProcessor.ProcessResults.SF_FAILURE
				m_Status = ISpectraFileProcessor.ProcessStatus.SF_ERROR
			End If
		End If

		If m_Status = ISpectraFileProcessor.ProcessStatus.SF_ABORTING Then
			m_Results = ISpectraFileProcessor.ProcessResults.SF_ABORTED
		ElseIf m_Status = ISpectraFileProcessor.ProcessStatus.SF_ERROR Then
			m_Results = ISpectraFileProcessor.ProcessResults.SF_FAILURE
		Else
			'Verify at least one dta file was created
			If Not VerifyDtaCreation() Then
				m_Results = ISpectraFileProcessor.ProcessResults.SF_NO_FILES_CREATED
			Else
				m_Results = ISpectraFileProcessor.ProcessResults.SF_SUCCESS
			End If

			m_Status = ISpectraFileProcessor.ProcessStatus.SF_COMPLETE
		End If

	End Sub

	''' <summary>
	''' Method that actually makes the DTA files
	''' </summary>
	''' <returns>TRUE for success; FALSE for failure</returns>
	''' <remarks></remarks>
	Private Function MakeDTAFiles() As Boolean

		'Makes DTA files using extract_msn.exe
		Const DEF_MASS_TOL As String = "3"

		Dim CmdStr As String
		Dim RawFile As String

		Dim ScanStart As Integer
		Dim ScanStop As Integer
		Dim MaxIntermediateScansWhenGrouping As Integer
		Dim MWLower As String
		Dim MWUpper As String
		Dim MassTol As String
		Dim IonCount As String
		Dim CreateDefaultCharges As Boolean = True
		Dim ExplicitChargeStart As Short			' Ignored if ExplicitChargeStart = 0 or ExplicitChargeEnd = 0
		Dim ExplicitChargeEnd As Short			' Ignored if ExplicitChargeStart = 0 or ExplicitChargeEnd = 0

		Dim LocCharge As Short
		Dim LocScanStart As Integer
		Dim LocScanStop As Integer
		Dim MaxScanInFile As Integer
		Dim OutDirParam As String = " -P"		'Output directory parameter, dependent on xcalibur version

		'DAC debugging
		System.Threading.Thread.CurrentThread.Name = "MakeDTAFiles"

		If m_DebugLevel > 0 Then
			m_Logger.PostEntry("clsDtaGenMainProcess.MakeDTAFiles: Making DTA files", _
			 PRISM.Logging.ILogger.logMsgType.logDebug, True)
		End If

		'Get the parameters from the various setup files
		RawFile = Path.Combine(m_SourceFolderPath, m_DSName & ".raw")

		ScanStart = m_Settings.GetParam("ScanControl", "ScanStart", 1)
		ScanStop = m_Settings.GetParam("ScanControl", "ScanStop", 1000000)

		' Note: Set MaxIntermediateScansWhenGrouping to 0 to disable grouping
		MaxIntermediateScansWhenGrouping = m_Settings.GetParam("ScanControl", "MaxIntermediateScansWhenGrouping", 1)

		MWLower = m_Settings.GetParam("MWControl", "MWStart", 200).ToString
		MWUpper = m_Settings.GetParam("MWControl", "MWStop", 5000).ToString
		IonCount = m_Settings.GetParam("IonCounts", "IonCount", 35).ToString
		MassTol = m_Settings.GetParam("MassTol", "MassTol", 3).ToString

		'TODO: Is this line necessary when using Matt's replacement for settings file reader?
		If MassTol Is Nothing Then MassTol = DEF_MASS_TOL 'Some of the settings files don't have this value

		CreateDefaultCharges = m_Settings.GetParam("Charges", "CreateDefaultCharges", True)
		ExplicitChargeStart = m_Settings.GetParam("Charges", "ExplicitChargeStart", 0S)
		ExplicitChargeEnd = m_Settings.GetParam("Charges", "ExplicitChargeEnd", 0S)

		'Get the maximum number of scans in the file
		MaxScanInFile = GetMaxScan(RawFile)

		Select Case MaxScanInFile
			Case -1			'Generic error getting number of scans
				m_ErrMsg = "Unknown error getting number of scans; Maxscan = " & MaxScanInFile.ToString
				Return False
			Case 0			'ICR2LS unable to read file
				m_ErrMsg = "Unable to get maxscan; Maxscan = " & MaxScanInFile.ToString
				Return False
			Case Is > 0
				'This is normal, do nothing
			Case Else
				'This should never happen
				m_ErrMsg = "Critical error getting number of scans; Maxscan = " & MaxScanInFile.ToString
				Return False
		End Select

		'Verify max scan specified is in file
		If ScanStop > MaxScanInFile Then ScanStop = MaxScanInFile

		'Determine max number of scans to be performed
		m_NumScans = ScanStop - ScanStart + 1

		'Setup a program runner tool to make the spectra files
        m_RunProgTool = New clsRunDosProgram(m_Logger, m_OutFolderPath)

		'DAC debugging
		If m_DebugLevel > 0 Then
			m_Logger.PostEntry("clsDtaGenMainProcess.MakeDTAFiles, preparing DTA creation loop, thread " _
			  & System.Threading.Thread.CurrentThread.Name, PRISM.Logging.ILogger.logMsgType.logDebug, True)
		End If

		' Loop through the requested charge states, starting first with the default charges if appropriate
		If CreateDefaultCharges Then
			LocCharge = 0
		Else
			LocCharge = ExplicitChargeStart
		End If

		'DAC debugging
		If m_DebugLevel > 0 Then
			m_Logger.PostEntry("clsDtaGenMainProcess.MakeDTAFiles, LocCharge=" & LocCharge.ToString & ", ExplicitChargeEnd=" & _
			 ExplicitChargeEnd.ToString & ", m_AbortRequested=" & m_AbortRequested.ToString, PRISM.Logging.ILogger.logMsgType.logDebug, True)
		End If

		Do While LocCharge <= ExplicitChargeEnd And Not m_AbortRequested
			If LocCharge = 0 And CreateDefaultCharges OrElse LocCharge > 0 Then

				'Set up parameters to loop through .dta creation until no more files are created
				' Limit to chunks of 20000 scans due to limitation of extract_msn.exe
				' (only used if selected in manager settings)
				LocScanStart = ScanStart

				If CBool(m_MgrParams.GetParam("UseDTALooping")) Then
					If ScanStop > (LocScanStart + 20000) Then
						LocScanStop = LocScanStart + 20000
					Else
						LocScanStop = ScanStop
					End If
				Else
					LocScanStop = ScanStop
				End If

				'Loop until no more .dta files are created or ScanStop is reached
				Do While (LocScanStart <= ScanStop)
					' Check for abort
					If m_AbortRequested Then
						m_Status = ISpectraFileProcessor.ProcessStatus.SF_ABORTING
						Exit Do
					End If

					'Set up command
					CmdStr = "-I" & IonCount & " -G1"
					If LocCharge > 0 Then
						CmdStr &= " -C" & LocCharge.ToString
					End If

					CmdStr &= " -F" & LocScanStart.ToString & " -L" & LocScanStop.ToString
					CmdStr &= " -S" & MaxIntermediateScansWhenGrouping
					CmdStr &= " -B" & MWLower & " -T" & MWUpper & " -M" & MassTol
					CmdStr &= " -D" & m_OutFolderPath & " " & RawFile

					'DAC debugging
					If m_DebugLevel > 0 Then
						m_Logger.PostEntry("clsDtaGenMainProcess.MakeDTAFiles, CmdStr=" & CmdStr, _
						  PRISM.Logging.ILogger.logMsgType.logDebug, True)
						m_Logger.PostEntry("clsDtaGenMainProcess.MakeDTAFiles, starting RunProgram, thread " _
						  & System.Threading.Thread.CurrentThread.Name, PRISM.Logging.ILogger.logMsgType.logDebug, True)
					End If

                    If Not m_RunProgTool.RunProgram(m_DtaToolNameLoc, CmdStr, "DTA_LCQ", True) Then
                        ' .RunProgram returned False
                        LogDTACreationStats("clsDtaGenmainProcess.MakeDTAFiles", Path.GetFileNameWithoutExtension(m_DtaToolNameLoc), "m_RunProgTool.RunProgram returned False")

                        m_ErrMsg = "Error running " & Path.GetFileNameWithoutExtension(m_DtaToolNameLoc)
                        Return False
                    End If

					'DAC debugging
					If m_DebugLevel > 0 Then
						m_Logger.PostEntry("clsDtaGenMainProcess.MakeDTAFiles, RunProgram complete, thread " _
						 & System.Threading.Thread.CurrentThread.Name, PRISM.Logging.ILogger.logMsgType.logDebug, True)
					End If

					'Update loopy parameters
					LocScanStart = LocScanStop + 1
					LocScanStop = LocScanStart + 20000
					If LocScanStop > ScanStop Then
						LocScanStop = ScanStop
					End If
				Loop

			End If

			If LocCharge = 0 Then
				If ExplicitChargeStart <= 0 Or ExplicitChargeEnd <= 0 Then
					Exit Do
				Else
					LocCharge = ExplicitChargeStart
				End If
			Else
				LocCharge += 1S
			End If
		Loop

		If m_AbortRequested Then
			m_Status = ISpectraFileProcessor.ProcessStatus.SF_ABORTING
		End If

		'DAC debugging
		If m_DebugLevel > 0 Then
			m_Logger.PostEntry("clsDtaGenMainProcess.MakeDTAFiles, DTA creation loop complete, thread " _
			  & System.Threading.Thread.CurrentThread.Name, PRISM.Logging.ILogger.logMsgType.logDebug, True)
		End If

		'We got this far, everything must have worked
        If m_Status = ISpectraFileProcessor.ProcessStatus.SF_ABORTING Then
            LogDTACreationStats("clsDtaGenmainProcess.MakeDTAFiles", Path.GetFileNameWithoutExtension(m_DtaToolNameLoc), "m_Status = ISpectraFileProcessor.ProcessStatus.SF_ABORTING")
            Return False

        ElseIf m_Status = ISpectraFileProcessor.ProcessStatus.SF_ERROR Then
            LogDTACreationStats("clsDtaGenmainProcess.MakeDTAFiles", Path.GetFileNameWithoutExtension(m_DtaToolNameLoc), "m_Status = ISpectraFileProcessor.ProcessStatus.SF_ERROR ")
            Return False

        Else
            Return True
        End If

	End Function

	''' <summary>
	''' Verifies at least one DTA file was created
	''' </summary>
	''' <returns>TRUE if at least 1 file created; FALSE otherwise</returns>
	''' <remarks></remarks>
	Private Function VerifyDtaCreation() As Boolean

		'Verify at least one .dta file has been created
		If CountDtaFiles() < 1 Then
			m_ErrMsg = "No dta files created"
			Return False
		Else
			Return True
		End If

	End Function

	''' <summary>
	''' Event handler for LoopWaiting event
	''' </summary>
	''' <remarks></remarks>
	Private Sub m_RunProgTool_LoopWaiting() Handles m_RunProgTool.LoopWaiting

		'Update the status file
		Dim FileList() As String = Directory.GetFiles(m_OutFolderPath, "*.dta")
		m_SpectraFileCount = FileList.GetLength(0)

		m_StatusTools.UpdateAndWrite(IStatusFile.JobStatus.STATUS_RUNNING, 0, m_SpectraFileCount)

	End Sub
#End Region

End Class
