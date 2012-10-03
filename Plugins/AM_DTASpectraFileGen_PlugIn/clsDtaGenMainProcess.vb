'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2006, Battelle Memorial Institute
' Created 06/07/2006
'
' Uses DeconMSn or ExtractMSn to create _DTA.txt file from a .Raw file

'*********************************************************************************************************

Option Strict On

Imports AnalysisManagerBase
Imports AnalysisManagerMsXmlGenPlugIn

Public Class clsDtaGenThermoRaw
	Inherits clsDtaGen

	'*********************************************************************************************************
	'This class creates DTA files using either DeconMSn.exe or ExtractMSn.exe
	'*********************************************************************************************************

#Region "Constants"
	Protected Const USE_THREADING As Boolean = True
	Protected Const DEFAULT_SCAN_STOP As Integer = 999999
#End Region

#Region "Module variables"
	Protected m_NumScans As Integer
	Protected WithEvents m_RunProgTool As clsRunDosProgram
	Private m_thThread As System.Threading.Thread

	Protected m_MaxScanInFile As Integer
	Private m_RunningExtractMSn As Boolean
	Private m_InstrumentFileName As String = String.Empty

	Private WithEvents mDTAWatcher As System.IO.FileSystemWatcher

#End Region

#Region "API Declares"
	'Used for getting dta count in spectra file via ICR2LS
	'Private Declare Function lopen Lib "kernel32" Alias "_lopen" (ByVal lpPathName As String, ByVal iReadWrite As Integer) As Integer
	'Private Declare Function lclose Lib "kernel32" Alias "_lclose" (ByVal hFile As Integer) As Integer
	'Private Declare Function XnumScans Lib "icr2ls32.dll" (ByVal FileHandle As Integer) As Integer

	'API constants
	Private Const OF_READ As Short = &H0S
	Private Const OF_READWRITE As Short = &H2S
	Private Const OF_WRITE As Short = &H1S
	Private Const OF_SHARE_COMPAT As Short = &H0S
	Private Const OF_SHARE_DENY_NONE As Short = &H40S
	Private Const OF_SHARE_DENY_READ As Short = &H30S
	Private Const OF_SHARE_DENY_WRITE As Short = &H20S
	Private Const OF_SHARE_EXCLUSIVE As Short = &H10S

	Public Const DECONMSN_FILENAME As String = "deconmsn.exe"
	Public Const EXTRACT_MSN_FILENAME As String = "extract_msn.exe"
	Public Const MSCONVERT_FILENAME As String = "msconvert.exe"
#End Region

#Region "Methods"

	Public Overrides Sub Setup(ByVal InitParams As ISpectraFileProcessor.InitializationParams)
		MyBase.Setup(InitParams)

		m_DtaToolNameLoc = ConstructDTAToolPath()

	End Sub

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

		If Not VerifyFileExists(m_DtaToolNameLoc) Then
			m_Results = ISpectraFileProcessor.ProcessResults.SF_FAILURE
			m_Status = ISpectraFileProcessor.ProcessStatus.SF_ERROR
			Return m_Status
		End If

		m_InstrumentFileName = m_Dataset & ".raw"

		'Make the DTA files (the process runs in a separate thread)
		Try
			If USE_THREADING Then
				m_thThread = New System.Threading.Thread(AddressOf MakeDTAFilesThreaded)
				m_thThread.Start()
				m_Status = ISpectraFileProcessor.ProcessStatus.SF_RUNNING
			Else
				MakeDTAFilesThreaded()
				m_Status = ISpectraFileProcessor.ProcessStatus.SF_COMPLETE
			End If

		Catch ex As Exception
			m_ErrMsg = "Error calling MakeDTAFilesThreaded"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_ErrMsg & ": " & ex.Message, ex)
			m_Status = ISpectraFileProcessor.ProcessStatus.SF_ERROR
		End Try

		Return m_Status

	End Function

	Protected Overridable Function ConstructDTAToolPath() As String

		Dim strDTAGenProgram As String
		Dim strDTAToolPath As String

		strDTAGenProgram = m_JobParams.GetJobParameter("DtaGenerator", "")

		If strDTAGenProgram.ToLower() = EXTRACT_MSN_FILENAME.ToLower() Then
			' Extract_MSn uses the lcqdtaloc folder path
			strDTAToolPath = System.IO.Path.Combine(m_MgrParams.GetParam("lcqdtaloc", ""), strDTAGenProgram)
		Else
			' DeconMSn uses the XcalDLLPath
			strDTAToolPath = System.IO.Path.Combine(m_MgrParams.GetParam("XcalDLLPath", ""), strDTAGenProgram)
		End If

		Return strDTAToolPath

	End Function

	''' <summary>
	''' Tests for existence of .raw file in specified location
	''' </summary>
	''' <param name="WorkDir">Directory where .raw file should be found</param>
	''' <param name="DSName">Name of dataset being processed</param>
	''' <returns>TRUE if file found; FALSE otherwise</returns>
	''' <remarks></remarks>
	Protected Function VerifyRawFileExists(ByVal WorkDir As String, ByVal DSName As String) As Boolean

		Dim strExtension As String = ".xyz"

		'Verifies a the data file exists in specfied directory
		Select Case m_RawDataType
			Case clsAnalysisResources.eRawDataTypeConstants.ThermoRawFile
				strExtension = clsAnalysisResources.DOT_RAW_EXTENSION
			Case clsAnalysisResources.eRawDataTypeConstants.mzXML
				strExtension = clsAnalysisResources.DOT_MZXML_EXTENSION
			Case clsAnalysisResources.eRawDataTypeConstants.mzML
				strExtension = clsAnalysisResources.DOT_MZML_EXTENSION
			Case Else
				m_ErrMsg = "Unsupported data type: " & m_RawDataType.ToString()
				Return False
		End Select

		m_JobParams.AddResultFileToSkip(DSName & strExtension)

		If System.IO.File.Exists(System.IO.Path.Combine(WorkDir, DSName & strExtension)) Then
			m_ErrMsg = String.Empty
			Return True
		Else
			strExtension = clsAnalysisResources.DOT_MGF_EXTENSION
			If System.IO.File.Exists(System.IO.Path.Combine(WorkDir, DSName & strExtension)) Then
				m_ErrMsg = String.Empty
				Return True
			Else
				m_ErrMsg = "Data file " & DSName & strExtension & " not found in working directory"
				Return False
			End If
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
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsDtaGenThermoRaw.InitSetup: Initializing DTA generator setup")
		End If

		'Do tests specfied in base class
		If Not MyBase.InitSetup Then Return False

		'Raw data file exists?
		If Not VerifyRawFileExists(m_WorkDir, m_Dataset) Then Return False 'Error message handled by VerifyRawFileExists

		'DTA creation tool exists?
		m_DtaToolNameLoc = ConstructDTAToolPath()
		If Not VerifyFileExists(m_DtaToolNameLoc) Then Return False 'Error message handled by VerifyFileExists

		'If we got to here, there was no problem
		Return True

	End Function

	''' <summary>
	''' Determines the maximum scan number in the .raw file
	''' </summary>
	''' <param name="RawFile">Data file name</param>
	''' <returns>Number of scans found</returns>
	Protected Function GetMaxScan(ByVal RawFile As String) As Integer

		'**************************************************************************************************************************************************************
		'	Alternate method of determining Max Scan using ICR2LS
		'**************************************************************************************************************************************************************
		'      'Uses ICR2LS to get the maximum number of scans in a .raw file
		'Dim FileHandle As Integer
		'Dim NumScans As Integer
		'Dim Dummy As Integer

		'FileHandle = lopen(RawFile, OF_READ)
		'If FileHandle = 0 Then Return -1 'Bad lopen
		'NumScans = XnumScans(FileHandle)
		'Dummy = lclose(FileHandle)
		'If Dummy <> 0 Then Return -1 'Bad lclose

		'Return NumScans

		'**************************************************************************************************************************************************************
		'	Alternate method of determining MaxScan using XCalibur OCX. 
		'   Possibly causes .raw file lock. 
		'**************************************************************************************************************************************************************
		Dim NumScans As Integer

		Dim XRawFile As MSFileReaderLib.MSFileReader_XRawfile
		XRawFile = New MSFileReaderLib.MSFileReader_XRawfile
		XRawFile.Open(RawFile)
		XRawFile.SetCurrentController(0, 1)
		XRawFile.GetNumSpectra(NumScans)
		'XRawFile.GetFirstSpectrumNumber(StartScan)
		'XRawFile.GetLastSpectrumNumber(StopScan)
		XRawFile.Close()

		XRawFile = Nothing
		'Pause and garbage collect to allow release of file lock on .raw file
		System.Threading.Thread.Sleep(3000)		' 3 second delay
		PRISM.Processes.clsProgRunner.GarbageCollectNow()

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
	''' This functon is called by MakeDTAFilesThreaded
	''' </summary>
	''' <returns>TRUE for success; FALSE for failure</returns>
	''' <remarks></remarks>
	Private Function MakeDTAFiles() As Boolean

		Const LOOPING_CHUNK_SIZE As Integer = 25000

		'Makes DTA files using extract_msn.exe or DeconMSn.exe
		Dim CmdStr As String
		Dim strInstrumentDataFilePath As String

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
		Dim OutDirParam As String = " -P"		'Output directory parameter, dependent on xcalibur version

		'DAC debugging
		System.Threading.Thread.CurrentThread.Name = "MakeDTAFiles"

		If m_DebugLevel >= 1 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Creating DTA files using " + System.IO.Path.GetFileName(m_DtaToolNameLoc))
		End If

		'Get the parameters from the various parameter dictionaries

		strInstrumentDataFilePath = System.IO.Path.Combine(m_WorkDir, m_InstrumentFileName)

		'Note: Defaults are used if certain parameters are not present in m_JobParams

		ScanStart = m_JobParams.GetJobParameter("ScanStart", CInt(1))
		ScanStop = m_JobParams.GetJobParameter("ScanStop", DEFAULT_SCAN_STOP)

		' Note: Set MaxIntermediateScansWhenGrouping to 0 to disable grouping
		MaxIntermediateScansWhenGrouping = m_JobParams.GetJobParameter("MaxIntermediateScansWhenGrouping", CInt(1))

		MWLower = m_JobParams.GetJobParameter("MWStart", "200")
		MWUpper = m_JobParams.GetJobParameter("MWStop", "5000")
		IonCount = m_JobParams.GetJobParameter("IonCount", "35")
		MassTol = m_JobParams.GetJobParameter("MassTol", "3")

		CreateDefaultCharges = m_JobParams.GetJobParameter("CreateDefaultCharges", True)
		ExplicitChargeStart = m_JobParams.GetJobParameter("ExplicitChargeStart", CShort(0))
		ExplicitChargeEnd = m_JobParams.GetJobParameter("ExplicitChargeEnd", CShort(0))

		'Get the maximum number of scans in the file
		Dim RawFile As String = String.Copy(strInstrumentDataFilePath)
		If System.IO.Path.GetExtension(strInstrumentDataFilePath).ToLower() <> clsAnalysisResources.DOT_RAW_EXTENSION Then
			RawFile = System.IO.Path.ChangeExtension(RawFile, clsAnalysisResources.DOT_RAW_EXTENSION)
		End If

		If System.IO.File.Exists(RawFile) Then
			m_MaxScanInFile = GetMaxScan(RawFile)
		Else
			m_MaxScanInFile = 0
		End If

		Select Case m_MaxScanInFile
			Case -1
				' Generic error getting number of scans
				m_ErrMsg = "Unknown error getting number of scans; Maxscan = " & m_MaxScanInFile.ToString
				Return False
			Case 0
				' Unable to read file; treat this is a warning
				m_ErrMsg = "Warning: unable to get maxscan; Maxscan = 0"
			Case Is > 0
				' This is normal, do nothing
			Case Else
				' This should never happen
				m_ErrMsg = "Critical error getting number of scans; Maxscan = " & m_MaxScanInFile.ToString
				Return False
		End Select

		'Verify max scan specified is in file
		If m_MaxScanInFile > 0 Then
			If ScanStop > m_MaxScanInFile Then ScanStop = m_MaxScanInFile
		End If

		'Determine max number of scans to be performed
		m_NumScans = ScanStop - ScanStart + 1

		'Setup a program runner tool to make the spectra files
		m_RunProgTool = New clsRunDosProgram(m_WorkDir)

		' Loop through the requested charge states, starting first with the default charges if appropriate
		If CreateDefaultCharges Then
			LocCharge = 0
		Else
			LocCharge = ExplicitChargeStart
		End If

		m_RunningExtractMSn = m_DtaToolNameLoc.ToLower.Contains(EXTRACT_MSN_FILENAME.ToLower)

		' Setup a FileSystemWatcher to watch for new .Dta files being created
		' We can compare the scan number of new .Dta files to the m_MaxScanInFile value to determine % complete
		mDTAWatcher = New System.IO.FileSystemWatcher(m_WorkDir, "*.dta")

		mDTAWatcher.IncludeSubdirectories = False
		mDTAWatcher.NotifyFilter = IO.NotifyFilters.FileName Or IO.NotifyFilters.CreationTime

		mDTAWatcher.EnableRaisingEvents = True

		Do While LocCharge <= ExplicitChargeEnd And Not m_AbortRequested
			If LocCharge = 0 And CreateDefaultCharges OrElse LocCharge > 0 Then

				' If we are using extract_msn.exe, then need to loop through .dta creation until no more files are created
				' Limit to chunks of LOOPING_CHUNK_SIZE scans due to limitation of extract_msn.exe
				' (only used if selected in manager settings, but "UseDTALooping" is typically set to True)

				LocScanStart = ScanStart

				If m_RunningExtractMSn AndAlso m_MgrParams.GetParam("UseDTALooping", False) Then
					If ScanStop > (LocScanStart + LOOPING_CHUNK_SIZE) Then
						LocScanStop = LocScanStart + LOOPING_CHUNK_SIZE
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

					' For ExtractMSn, -S means the number of allowed different intermediate scans for grouping (default=1), for example -S1
					' For DeconMSn, -S means the type of spectra to process, for example -SALL or -SCID

					If m_RunningExtractMSn Then
						CmdStr &= " -S" & MaxIntermediateScansWhenGrouping
					End If

					CmdStr &= " -B" & MWLower & " -T" & MWUpper & " -M" & MassTol
					CmdStr &= " -D" & m_WorkDir & " " & clsAnalysisToolRunnerBase.PossiblyQuotePath(System.IO.Path.Combine(m_WorkDir, m_InstrumentFileName))

					If m_DebugLevel >= 1 Then
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, m_DtaToolNameLoc & " " & CmdStr)
					End If

					With m_RunProgTool
						If m_RunningExtractMSn Then
							' If running Extract_MSn, then cannot cache the standard output; clsProgRunner sometimes freezes on certain datasets (e.g. QC_Shew_10_05_pt5_1_24Jun10_Earth_10-05-10)
							.CreateNoWindow = False
							.CacheStandardOutput = False
							.EchoOutputToConsole = False
						Else
							.CreateNoWindow = True
							.CacheStandardOutput = True
							.EchoOutputToConsole = True

							.WriteConsoleOutputToFile = False
							.ConsoleOutputFilePath = System.IO.Path.Combine(m_WorkDir, "DeconMSn_ConsoleOutput.txt")

							' Need to set the working directory as the same folder as DeconMSn; otherwise, may crash
							.WorkDir = System.IO.Path.GetDirectoryName(m_DtaToolNameLoc)
						End If

					End With

					If Not m_RunProgTool.RunProgram(m_DtaToolNameLoc, CmdStr, "DTA_LCQ", True) Then
						' .RunProgram returned False
						LogDTACreationStats("clsDtaGenThermoRaw.MakeDTAFiles", System.IO.Path.GetFileNameWithoutExtension(m_DtaToolNameLoc), "m_RunProgTool.RunProgram returned False")

						m_ErrMsg = "Error running " & System.IO.Path.GetFileNameWithoutExtension(m_DtaToolNameLoc)
						Return False
					End If

					If m_DebugLevel >= 2 Then
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsDtaGenThermoRaw.MakeDTAFiles, RunProgram complete, thread " _
						 & System.Threading.Thread.CurrentThread.Name)
					End If

					'Update loopy parameters
					LocScanStart = LocScanStop + 1
					LocScanStop = LocScanStart + LOOPING_CHUNK_SIZE
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

		' Disable the DTA watcher
		mDTAWatcher.EnableRaisingEvents = False

		'DAC debugging
		If m_DebugLevel >= 2 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsDtaGenThermoRaw.MakeDTAFiles, DTA creation loop complete, thread " _
			  & System.Threading.Thread.CurrentThread.Name)
		End If

		'We got this far, everything must have worked
		If m_Status = ISpectraFileProcessor.ProcessStatus.SF_ABORTING Then
			LogDTACreationStats("clsDtaGenThermoRaw.MakeDTAFiles", System.IO.Path.GetFileNameWithoutExtension(m_DtaToolNameLoc), "m_Status = ISpectraFileProcessor.ProcessStatus.SF_ABORTING")
			Return False

		ElseIf m_Status = ISpectraFileProcessor.ProcessStatus.SF_ERROR Then
			LogDTACreationStats("clsDtaGenThermoRaw.MakeDTAFiles", System.IO.Path.GetFileNameWithoutExtension(m_DtaToolNameLoc), "m_Status = ISpectraFileProcessor.ProcessStatus.SF_ERROR ")
			Return False

		Else
			Return True
		End If

	End Function

	Private Sub UpdateDTAProgress(ByVal DTAFileName As String)
		Static reDTAFile As System.Text.RegularExpressions.Regex

		Dim reMatch As System.Text.RegularExpressions.Match
		Dim intScanNumber As Integer

		If reDTAFile Is Nothing Then
			reDTAFile = New System.Text.RegularExpressions.Regex("(\d+)\.\d+\.\d+\.dta$", _
							System.Text.RegularExpressions.RegexOptions.Compiled Or _
							System.Text.RegularExpressions.RegexOptions.IgnoreCase)
		End If

		Try
			' Extract out the scan number from the DTA filename
			reMatch = reDTAFile.Match(DTAFileName)
			If reMatch.Success Then
				If Integer.TryParse(reMatch.Groups.Item(1).Value, intScanNumber) Then
					m_Progress = CSng(intScanNumber / m_MaxScanInFile * 100)
				End If
			End If
		Catch ex As Exception
			' Ignore errors here
		End Try

	End Sub

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

#End Region

#Region "Event Handlers"

	''' <summary>
	''' Event handler for LoopWaiting event
	''' </summary>
	''' <remarks></remarks>
	Private Sub m_RunProgTool_LoopWaiting() Handles m_RunProgTool.LoopWaiting

		Static dtLastDtaCountTime As System.DateTime = System.DateTime.UtcNow
		Static dtLastStatusUpdate As System.DateTime = System.DateTime.UtcNow

		' Synchronize the stored Debug level with the value stored in the database
		Const MGR_SETTINGS_UPDATE_INTERVAL_SECONDS As Integer = 300
		clsAnalysisToolRunnerBase.GetCurrentMgrSettingsFromDB(MGR_SETTINGS_UPDATE_INTERVAL_SECONDS, m_MgrParams, m_DebugLevel)

		' Count the number of .Dta files (only count the files every 10 seconds)
		If System.DateTime.UtcNow.Subtract(dtLastDtaCountTime).TotalSeconds >= 10 Then
			dtLastDtaCountTime = System.DateTime.UtcNow
			Dim FileList() As String = System.IO.Directory.GetFiles(m_WorkDir, "*.dta")
			m_SpectraFileCount = FileList.GetLength(0)
		End If

		'Update the status file (limit the updates to every 5 seconds)
		If System.DateTime.UtcNow.Subtract(dtLastStatusUpdate).TotalSeconds >= 5 Then
			dtLastStatusUpdate = System.DateTime.UtcNow
			m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.RUNNING_TOOL, m_Progress, m_SpectraFileCount, "", "", "", False)
		End If

	End Sub

	Private Sub mDTAWatcher_Created(ByVal sender As Object, ByVal e As System.IO.FileSystemEventArgs) Handles mDTAWatcher.Created
		UpdateDTAProgress(e.Name)
	End Sub

#End Region

End Class
