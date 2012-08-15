' This class creates DTA files using MGF/CDF files generated from Agilent Ion Trap MS/MS data
' It uses 
' 
' Written by Matthew Monroe for the Department of Energy (PNNL, Richland, WA)
' Started November 2005

'
'           TODO: UPDATE THIS TO USE MascotGenericFileToDTA.DLL
'   

Option Strict On

Imports AnalysisManagerBase

Public Class clsMGFtoDtaGenMainProcess
	Inherits clsDtaGen

#Region "Constants"
	Protected Const USE_THREADING As Boolean = False
#End Region

#Region "Structures"

	'Private Structure udtSpectrumProcessingOptions
	'	Public GuesstimateChargeForAllSpectra As Boolean					 ' When True, then tries to guesstimate a charge for all spectra, even those that already have a charge defined
	'	Public ForceChargeAddnForPredefined2PlusOr3Plus As Boolean		 ' When True, then always adds 2+ and 3+ charge when an existing 2+ or 3+ charge was already defined

	'	Public ThresholdIonPctForSingleCharge As Single			' Number between 0 and 100; if the percentage of ions greater than the parent ion m/z is less than this number, then the charge is definitely 1+
	'	Public ThresholdIonPctForDoubleCharge As Single			' Number between 0 and 100; if the percentage of ions greater than the parent ion m/z is greater than this number, then the charge is definitely 2+ or higher

	'	Public MaximumIonsPerSpectrum As Integer					' Set to 0 to use all data; if greater than 0, then data will be sorted on decreasing intensity and the top x ions will be retained
	'End Structure

	' The following is used to keep track of options the user might set via the interface functions
	' If a parameter file is defined in this class, then it will be passed to objFilterSpectra
	' If the parameter file has a section named "filteroptions" then any options defined there will override this values
	'Private Structure udtFilterSpectraOptionsType
	'	Public FilterSpectra As Boolean
	'	Public MinimumParentIonMZ As Single
	'	Public MinimumStandardMassSpacingIonPairs As Integer
	'	Public IonPairMassToleranceHalfWidthDa As Single
	'	Public NoiseLevelIntensityThreshold As Single
	'	Public DataPointCountToConsider As Integer
	'End Structure
#End Region

#Region "Module variables"
	Private m_thThread As System.Threading.Thread

	Protected WithEvents mMGFtoDTA As MascotGenericFileToDTA.clsMGFtoDTA

	' DTA generation options
	Private mScanStart As Integer
	Private mScanStop As Integer
	Private mMWLower As Single
	Private mMWUpper As Single

	' Spectrum processing and filtering options
	'Private mSpectrumProcessingOptions As udtSpectrumProcessingOptions
	'Private mFilterSpectraOptions As udtFilterSpectraOptionsType

#End Region

    Public Overrides Sub Setup(ByVal InitParams As ISpectraFileProcessor.InitializationParams) 
        MyBase.Setup(InitParams)

		m_DtaToolNameLoc = System.IO.Path.Combine(clsGlobal.GetAppFolderPath(), "MascotGenericFileToDTA.dll")

    End Sub

    Public Overrides Function Start() As ISpectraFileProcessor.ProcessStatus

        m_Status = ISpectraFileProcessor.ProcessStatus.SF_STARTING

        'Verify necessary files are in specified locations
        If Not InitSetup() Then
            m_Results = ISpectraFileProcessor.ProcessResults.SF_FAILURE
            m_Status = ISpectraFileProcessor.ProcessStatus.SF_ERROR
            Return m_Status
        End If

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
			m_ErrMsg = "Error calling MakeDTAFilesFromMGF: " & ex.Message
			m_Status = ISpectraFileProcessor.ProcessStatus.SF_ERROR
		End Try

        Return m_Status

    End Function

	Private Function VerifyMGFFileExists(ByVal WorkDir As String, ByVal DSName As String) As Boolean

		'Verifies a .mgf file exists in specfied directory
        If System.IO.File.Exists(System.IO.Path.Combine(WorkDir, DSName & ".mgf")) Then
            m_ErrMsg = ""
            Return True
        Else
            m_ErrMsg = "Data file " & DSName & ".mgf not found in working directory"
            Return False
        End If

	End Function

	Protected Overrides Function InitSetup() As Boolean

		'Verifies all necessary files exist in the specified locations

		'Do tests specfied in base class
		If Not MyBase.InitSetup Then Return False

        'MGF data file exists?
        If Not VerifyMGFFileExists(m_WorkDir, m_Dataset) Then Return False 'Error message handled by VerifyMGFFileExists

		'If we got to here, there was no problem
		Return True

	End Function

	Protected Sub MakeDTAFilesThreaded()

		m_Status = ISpectraFileProcessor.ProcessStatus.SF_RUNNING
		If Not MakeDTAFilesFromMGF() Then
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

	Private Function MakeDTAFilesFromMGF() As Boolean

		Dim MGFFile As String

		'Get the parameters from the various setup files
        MGFFile = System.IO.Path.Combine(m_WorkDir, m_Dataset & ".mgf")
		mScanStart = m_JobParams.GetJobParameter("ScanStart", 0)
		mScanStop = m_JobParams.GetJobParameter("ScanStop", 0)
		mMWLower = m_JobParams.GetJobParameter("MWStart", 0)
		mMWUpper = m_JobParams.GetJobParameter("MWStop", 0)

		'Run the MGF to DTA converter
		If Not ConvertMGFtoDTA(MGFFile, m_WorkDir) Then
			' Note that ConvertMGFtoDTA will have updated m_ErrMsg with the error message
			m_Results = ISpectraFileProcessor.ProcessResults.SF_FAILURE
			m_Status = ISpectraFileProcessor.ProcessStatus.SF_ERROR
			Return False
		End If

		If m_AbortRequested Then
			m_Status = ISpectraFileProcessor.ProcessStatus.SF_ABORTING
		End If

		'We got this far, everything must have worked
		If m_Status = ISpectraFileProcessor.ProcessStatus.SF_ABORTING Or m_Status = ISpectraFileProcessor.ProcessStatus.SF_ERROR Then
			Return False
		Else
			Return True
		End If

	End Function

	''' <summary>
	''' Convert .mgf file to _DTA.txt using MascotGenericFileToDTA.dll
	''' This functon is called by MakeDTAFilesThreaded
	''' </summary>
	''' <returns>TRUE for success; FALSE for failure</returns>
	''' <remarks></remarks>
	Private Function ConvertMGFtoDTA(ByVal strInputFilePathFull As String, ByVal strOutputFolderPath As String) As Boolean

		Dim blnSuccess As Boolean

		If m_DebugLevel > 0 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Converting .MGF file to _DTA.txt")
		End If

		mMGFtoDTA = New MascotGenericFileToDTA.clsMGFtoDTA()

		' Spectrum processing options
		'With mSpectrumProcessingOptions
		'	.MaximumIonsPerSpectrum = CInt(m_JobParams.GetParam("MaximumIonsPerSpectrum"))
		'End With

		'' Spectrum filtering options
		'With mFilterSpectraOptions
		'	.FilterSpectra = CBool(m_JobParams.GetParam("FilterSpectra"))
		'	.MinimumParentIonMZ = CInt(m_JobParams.GetParam("MinimumParentIonMZ"))
		'	.MinimumStandardMassSpacingIonPairs = CInt(m_JobParams.GetParam("MinimumStandardMassSpacingIonPairs"))
		'	.IonPairMassToleranceHalfWidthDa = CSng(m_JobParams.GetParam("IonPairMassToleranceHalfWidthDa"))
		'	.NoiseLevelIntensityThreshold = CInt(m_JobParams.GetParam("NoiseLevelIntensityThreshold"))
		'	.DataPointCountToConsider = CShort(m_JobParams.GetParam("DataPointCountToConsider"))
		'End With

		With mMGFtoDTA
			.CreateIndividualDTAFiles = False
			.MGFFileCommentLineStartChar = .MGFFileCommentLineStartChar
			.GuesstimateChargeForAllSpectra = m_JobParams.GetJobParameter("GuesstimateChargeForAllSpectra", False)
			.ForceChargeAddnForPredefined2PlusOr3Plus = m_JobParams.GetJobParameter("ForceChargeAddnForPredefined2PlusOr3Plus", False)

			' Note that these settings are values between 0 and 100
			.ThresholdIonPctForSingleCharge = m_JobParams.GetJobParameter("ThresholdIonPctForSingleCharge", CInt(.ThresholdIonPctForSingleCharge))
			.ThresholdIonPctForDoubleCharge = m_JobParams.GetJobParameter("ThresholdIonPctForDoubleCharge", CInt(.ThresholdIonPctForDoubleCharge))

			.FilterSpectra = m_JobParams.GetJobParameter("FilterSpectra", False)

			' Filter spectra options:
			.DiscardValidSpectra = .DiscardValidSpectra
			.IonPairMassToleranceHalfWidthDa = .IonPairMassToleranceHalfWidthDa
			.MinimumStandardMassSpacingIonPairs = .MinimumStandardMassSpacingIonPairs
			.NoiseLevelIntensityThreshold = .NoiseLevelIntensityThreshold
			.SequestParamFilePath = .SequestParamFilePath

			.LogMessagesToFile = False

			.MaximumIonsPerSpectrum = m_JobParams.GetJobParameter("MaximumIonsPerSpectrum", 0)
			.ScanToExportMinimum = mScanStart
			.ScanToExportMaximum = mScanStop
			.MinimumParentIonMZ = mMWLower

		End With

		blnSuccess = mMGFtoDTA.ProcessFile(strInputFilePathFull, strOutputFolderPath)

		If Not blnSuccess AndAlso String.IsNullOrEmpty(m_ErrMsg) Then
			m_ErrMsg = mMGFtoDTA.GetErrorMessage()
		End If

		m_SpectraFileCount = mMGFtoDTA.SpectraCountWritten
		m_Progress = 95

		Return blnSuccess

	End Function

	Private Function VerifyDtaCreation() As Boolean

		'Verify that the _DTA.txt file was created and is not empty
		Dim fiCDTAFile As System.IO.FileInfo
		fiCDTAFile = New System.IO.FileInfo(System.IO.Path.Combine(m_WorkDir, m_Dataset & "_DTA.txt"))

		If Not fiCDTAFile.Exists Then
			m_ErrMsg = "_DTA.txt file not created"
			Return False
		ElseIf fiCDTAFile.Length = 0 Then
			m_ErrMsg = "_DTA.txt file is empty"
			Return False
		Else
			Return True
		End If

	End Function

	Private Sub mMGFtoDTA_ErrorEvent(strMessage As String) Handles mMGFtoDTA.ErrorEvent
		If String.IsNullOrEmpty(m_ErrMsg) Then
			m_ErrMsg = "MGFtoDTA_Error: " & strMessage
		ElseIf m_ErrMsg.Length < 300 Then
			m_ErrMsg &= "; MGFtoDTA_Error: " & strMessage
		End If
	End Sub

End Class
