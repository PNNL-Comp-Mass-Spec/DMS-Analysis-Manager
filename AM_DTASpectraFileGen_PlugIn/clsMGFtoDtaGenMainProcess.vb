Option Strict On

Imports System.IO
Imports AnalysisManagerBase

Public Class clsMGFtoDtaGenMainProcess
    Inherits clsDtaGen

    ' This class implements the ISpectraFileProcessor interface and can be 
    ' loaded as a pluggable DLL into the DMS Analysis Manager program.  It uses class
    ' clsMsMsSpectrumFilter to filter the .DTA files present in a given folder

    ' Main processing class for simple DTA generation using using MGF/CDF files
    ' generated from Agilent Ion Trap MS/MS data
    ' 
    ' Written by Matthew Monroe for the Department of Energy (PNNL, Richland, WA)
    ' Started November 2005


#Region "Structures"

    Private Structure udtSpectrumProcessingOptions
        Public GuesstimateChargeForAllSpectra As Boolean                ' When True, then tries to guesstimate a charge for all spectra, even those that already have a charge defined
        Public ForceChargeAddnForPredefined2PlusOr3Plus As Boolean      ' When True, then always adds 2+ and 3+ charge when an existing 2+ or 3+ charge was already defined

        Public ThresholdIonPctForSingleCharge As Single       ' Number between 0 and 100; if the percentage of ions greater than the parent ion m/z is less than this number, then the charge is definitely 1+
        Public ThresholdIonPctForDoubleCharge As Single       ' Number between 0 and 100; if the percentage of ions greater than the parent ion m/z is greater than this number, then the charge is definitely 2+ or higher

        Public MaximumIonsPerSpectrum As Integer              ' Set to 0 to use all data; if greater than 0, then data will be sorted on decreasing intensity and the top x ions will be retained
    End Structure

    ' The following is used to keep track of options the user might set via the interface functions
    ' If a parameter file is defined in this class, then it will be passed to objFilterSpectra
    ' If the parameter file has a section named "filteroptions" then any options defined there will override this values
    Private Structure udtFilterSpectraOptionsType
        Public FilterSpectra As Boolean
        Public MinimumParentIonMZ As Single
        Public MinimumStandardMassSpacingIonPairs As Integer
        Public IonPairMassToleranceHalfWidthDa As Single
        Public NoiseLevelIntensityThreshold As Single
        Public DataPointCountToConsider As Integer
    End Structure
#End Region

#Region "Module variables"
    Private m_AbortRequested As Boolean = False
    Private m_thThread As System.Threading.Thread

    ' DTA generation options
    Private mScanStart As Integer
    Private mScanStop As Integer
    Private mMWLower As Single
    Private mMWUpper As Single

    ' Spectrum processing and filtering options
    Private mSpectrumProcessingOptions As udtSpectrumProcessingOptions
    Private mFilterSpectraOptions As udtFilterSpectraOptionsType

#End Region

    Public Overrides Function Abort() As ISpectraFileProcessor.ProcessStatus
        m_AbortRequested = True
    End Function

    Public Overrides Function Start() As ISpectraFileProcessor.ProcessStatus

        m_Status = ISpectraFileProcessor.ProcessStatus.SF_STARTING

        'Verify necessary files are in specified locations
        If Not InitSetup() Then
            m_Results = ISpectraFileProcessor.ProcessResults.SF_FAILURE
            m_Status = ISpectraFileProcessor.ProcessStatus.SF_ERROR
            Return m_status
        End If

        'Read the settings file
        If Not ReadSettingsFile(m_SettingsFileName, m_SourceFolderPath) Then
            m_Results = ISpectraFileProcessor.ProcessResults.SF_FAILURE
            m_Status = ISpectraFileProcessor.ProcessStatus.SF_ERROR
            Return m_status
        End If

        'Make the DTA files (the process runs in a separate thread)
        Try
            m_thThread = New System.Threading.Thread(AddressOf MakeDTAFilesThreaded)
            m_thThread.Start()
            m_status = ISpectraFileProcessor.ProcessStatus.SF_RUNNING
        Catch ex As Exception
            m_ErrMsg = "Error calling MakeDTAFilesFromMGF: " & ex.Message
            m_status = ISpectraFileProcessor.ProcessStatus.SF_ERROR
        End Try

        Return m_status

    End Function

    Private Function VerifyMGFFileExists(ByVal WorkDir As String, ByVal DSName As String) As Boolean

        'Verifies a .mgf file exists in specfied directory
        If File.Exists(Path.Combine(WorkDir, DSName & ".mgf")) Then
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

        'Misc parameters exist?
        If m_MiscParams Is Nothing Then
            m_ErrMsg = "No misc parameters specified"
            Return False
        End If

        'MGF data file exists?
        If Not VerifyMGFFileExists(m_SourceFolderPath, m_DSName) Then Return False 'Error message handled by VerifyMGFFileExists

        'If we got to here, there was no problem
        Return True

    End Function

    Protected Overridable Sub MakeDTAFilesThreaded()

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
        Const MGFTODTA_OPTIONS_SECTION As String = "MGFtoDTAOptions"
        Const MGFTODTA_SPECTRUM_FILTER_OPTIONS_SECTION As String = "MGFtoDTASpectraFilterOptions"

        Dim MGFFile As String

        Dim CreateDefaultCharges As Boolean = True
        Dim ExplicitChargeStart As Short            ' Ignored if ExplicitChargeStart = 0 or ExplicitChargeEnd = 0
        Dim ExplicitChargeEnd As Short              ' Ignored if ExplicitChargeStart = 0 or ExplicitChargeEnd = 0

        Dim LocCharge As Short
        Dim LocScanStart As Integer
        Dim LocScanStop As Integer

        'DAC debugging
        System.Threading.Thread.CurrentThread.Name = "MakeDTAFilesFromMGF"

        'Get the parameters from the various setup files
        MGFFile = Path.Combine(m_SourceFolderPath, m_DSName & ".mgf")
        mScanStart = m_Settings.GetParam("ScanControl", "ScanStart", 1)
        mScanStop = m_Settings.GetParam("ScanControl", "ScanStop", 1000000)
        mMWLower = m_Settings.GetParam("MWControl", "MWStart", 200)
        mMWUpper = m_Settings.GetParam("MWControl", "MWStop", 5000)

        CreateDefaultCharges = m_Settings.GetParam("Charges", "CreateDefaultCharges", True)
        ExplicitChargeStart = m_Settings.GetParam("Charges", "ExplicitChargeStart", 0S)
        ExplicitChargeEnd = m_Settings.GetParam("Charges", "ExplicitChargeEnd", 0S)

        ' Spectrum processing options
        With mSpectrumProcessingOptions
            .GuesstimateChargeForAllSpectra = m_settings.GetParam(MGFTODTA_OPTIONS_SECTION, "GuesstimateChargeForAllSpectra", True)
            .ForceChargeAddnForPredefined2PlusOr3Plus = m_settings.GetParam(MGFTODTA_OPTIONS_SECTION, "ForceChargeAddnForPredefined2PlusOr3Plus", False)
            .ThresholdIonPctForSingleCharge = m_settings.GetParam(MGFTODTA_OPTIONS_SECTION, "ThresholdIonPctForSingleCharge", 10)
            .ThresholdIonPctForDoubleCharge = m_settings.GetParam(MGFTODTA_OPTIONS_SECTION, "ThresholdIonPctForDoubleCharge", 25)
            .MaximumIonsPerSpectrum = m_settings.GetParam(MGFTODTA_OPTIONS_SECTION, "MaximumIonsPerSpectrum", 0)
        End With

        ' Spectrum filtering options
        With mFilterSpectraOptions
            .FilterSpectra = m_settings.GetParam(MGFTODTA_SPECTRUM_FILTER_OPTIONS_SECTION, "FilterSpectra", True)
            .MinimumParentIonMZ = m_settings.GetParam(MGFTODTA_SPECTRUM_FILTER_OPTIONS_SECTION, "MinimumParentIonMZ", 300)
            .MinimumStandardMassSpacingIonPairs = m_settings.GetParam(MGFTODTA_SPECTRUM_FILTER_OPTIONS_SECTION, "MinimumStandardMassSpacingIonPairs", 3)
            .IonPairMassToleranceHalfWidthDa = m_settings.GetParam(MGFTODTA_SPECTRUM_FILTER_OPTIONS_SECTION, "IonPairMassToleranceHalfWidthDa", CSng(0.1))
            .NoiseLevelIntensityThreshold = m_settings.GetParam(MGFTODTA_SPECTRUM_FILTER_OPTIONS_SECTION, "NoiseLevelIntensityThreshold", 0)
            .DataPointCountToConsider = m_settings.GetParam(MGFTODTA_SPECTRUM_FILTER_OPTIONS_SECTION, "DataPointCountToConsider", 50)
        End With

        'DAC debugging
        Debug.WriteLine("clsMGFtoDtaGenMainProcess.MakeDTAFilesFromMGF, preparing DTA creation loop, thread " & System.Threading.Thread.CurrentThread.Name)

        'Run the MGF to DTA converter
        If Not ConvertMGFtoDTA(MGFFile, m_OutFolderPath) Then
            ' Note that ConvertMGFtoDTA will have updated m_ErrMsg with the error message
            m_Results = ISpectraFileProcessor.ProcessResults.SF_FAILURE
            m_Status = ISpectraFileProcessor.ProcessStatus.SF_ERROR
            Return False
        End If

        ' Possibly generate the explicit charges
        ' If CreateDefaultCharges = False, then the .Dta files that do not qualify for the explicit charges will be deleted
        If Not m_AbortRequested Then
            GenerateMissingChargeStateDTAFiles(m_OutFolderPath, "*.dta", ExplicitChargeStart, ExplicitChargeEnd, CreateDefaultCharges)
        End If

        If m_AbortRequested Then
            m_Status = ISpectraFileProcessor.ProcessStatus.SF_ABORTING
        End If

        'DAC debugging
        Debug.WriteLine("clsMGFtoDtaGenMainProcess.MakeDTAFilesFromMGF, DTA creation loop complete, thread " & System.Threading.Thread.CurrentThread.Name)

        'We got this far, everything must have worked
        If m_Status = ISpectraFileProcessor.ProcessStatus.SF_ABORTING Or m_Status = ISpectraFileProcessor.ProcessStatus.SF_ERROR Then
            Return False
        Else
            Return True
        End If

    End Function

    Private Function ConvertMGFtoDTA(ByVal strInputFilePathFull As String, ByVal strOutputFolderPath As String) As Boolean
        ' strInputFilePathFull should contain the path to a .MGF file
        ' This function will create individual .DTA files in strOutputFolderPath for each spectrum in strInputFilePathFull

        Dim objMGFReader As New MsMsDataFileReader.clsMGFReader
        Dim objSpectrumFilter As MsMsSpectrumFilter.clsMsMsSpectrumFilter

        Dim srOutFile As System.IO.StreamWriter

        Dim intMsMsDataCount As Integer
        Dim strMSMSDataList() As String
        Dim udtSpectrumHeaderInfo As MsMsDataFileReader.clsMsMsDataFileReaderBaseClass.udtSpectrumHeaderInfoType

        Dim intScanNumberStartSaved As Integer
        Dim intIndex As Integer

        Dim strFileNameBase As String

        Dim blnSpectrumFound As Boolean
        Dim blnSuccess As Boolean

        Try
            If mFilterSpectraOptions.FilterSpectra Then
                objSpectrumFilter = New MsMsSpectrumFilter.clsMsMsSpectrumFilter
                With objSpectrumFilter
                    .ShowMessages = False

                    .DiscardValidSpectra = False
                    .OverwriteExistingFiles = True

                    ' Always use SpectrumFilterMode 1
                    .SpectrumFilterMode = MsMsSpectrumFilter.clsMsMsSpectrumFilter.eSpectrumFilterMode.mode1

                    .SetFilterMode1Option(MsMsSpectrumFilter.clsMsMsSpectrumFilter.FilterMode1Options.MinimumStandardMassSpacingIonPairs, mFilterSpectraOptions.MinimumStandardMassSpacingIonPairs)
                    .SetFilterMode1Option(MsMsSpectrumFilter.clsMsMsSpectrumFilter.FilterMode1Options.IonPairMassToleranceHalfWidthDa, mFilterSpectraOptions.IonPairMassToleranceHalfWidthDa)
                    .SetFilterMode1Option(MsMsSpectrumFilter.clsMsMsSpectrumFilter.FilterMode1Options.NoiseLevelIntensityThreshold, mFilterSpectraOptions.NoiseLevelIntensityThreshold)
                    .SetFilterMode1Option(MsMsSpectrumFilter.clsMsMsSpectrumFilter.FilterMode1Options.DataPointCountToConsider, mFilterSpectraOptions.DataPointCountToConsider)

                    .SettingsLoadedViaCode = True
                End With
            End If

            ' Define the charge guesstimation thresholds
            With objMGFReader
                ' Leave .CommentLineStartChar defined as the default: .CommentLineStartChar = "#"c
                .ThresholdIonPctForSingleCharge = mSpectrumProcessingOptions.ThresholdIonPctForSingleCharge
                .ThresholdIonPctForDoubleCharge = mSpectrumProcessingOptions.ThresholdIonPctForDoubleCharge
            End With

            ' Define strFileNameBase
            strFileNameBase = System.IO.Path.GetFileNameWithoutExtension(strInputFilePathFull)

            If Not m_DSName Is Nothing AndAlso m_DSName.Length > 0 Then
                strFileNameBase = String.Copy(m_DSName)
            End If

            ' Open the input file and parse it
            If Not objMGFReader.OpenFile(strInputFilePathFull) Then
                blnSuccess = False
                Exit Try
            End If

            Do
                ' Read the next available spectrum
                blnSpectrumFound = objMGFReader.ReadNextSpectrum(strMSMSDataList, intMsMsDataCount, udtSpectrumHeaderInfo)
                If blnSpectrumFound Then
                    If Not CreateDTAEntry(objMGFReader, objSpectrumFilter, strMSMSDataList, intMsMsDataCount, udtSpectrumHeaderInfo, strFileNameBase, strOutputFolderPath, srOutFile) Then
                        ' Error creating DTA entry; CreateDTAEntry should have already updated m_ErrMsg
                        ' Abort processing
                        blnSuccess = False
                        Exit Do
                    End If
                End If
            Loop While blnSpectrumFound

            blnSuccess = True
            If Not srOutFile Is Nothing Then
                srOutFile.Close()
            End If
        Catch ex As Exception
            m_ErrMsg = "Error reading input file: " & strInputFilePathFull & "; " & ex.Message
            blnSuccess = False
        End Try

        Return blnSuccess

    End Function

    Private Function CreateDTAEntry(ByRef objMGFReader As MsMsDataFileReader.clsMGFReader, ByRef objSpectrumFilter As MsMsSpectrumFilter.clsMsMsSpectrumFilter, ByVal strMSMSData() As String, ByRef intMsMsDataCount As Integer, ByVal udtSpectrumHeaderInfo As MsMsDataFileReader.clsMsMsDataFileReaderBaseClass.udtSpectrumHeaderInfoType, ByVal strFileNameBase As String, ByVal strOutputFolderPath As String, ByRef srOutFile As System.IO.StreamWriter) As Boolean
        ' Returns True if success, False if an error

        Dim intDataCount As Integer
        Dim sngIonMasses() As Single
        Dim sngIonIntensities() As Single

        Dim intChargeIndex As Integer
        Dim intDataIndex As Integer

        Dim sngParentIonMH As Single

        Dim strLineOut As String
        Dim strSplitLine() As String

        Dim sngValueData() As Single
        Dim strMsMsDataNew() As String

        Dim strPrefix, strSuffix As String
        Dim strFileName As String
        Dim strOutputFilePath As String

        Dim blnCreateEntry As Boolean
        Dim blnSuccess As Boolean

        Try
            ' Set these to True for now
            blnCreateEntry = True
            blnSuccess = True

            ' Make sure scan is between mScanStart and mScanStop

            If mScanStop > 0 AndAlso mScanStop >= mScanStart Then
                If udtSpectrumHeaderInfo.ScanNumberStart < mScanStart Or udtSpectrumHeaderInfo.ScanNumberStart > mScanStop Then
                    If udtSpectrumHeaderInfo.ScanNumberEnd < mScanStart Or udtSpectrumHeaderInfo.ScanNumberEnd > mScanStop Then
                        blnCreateEntry = False
                    End If
                End If
            End If

            If blnCreateEntry Then
                ' Populate sngIonMasses() and sngIonIntensities()
                intDataCount = objMGFReader.ParseMsMsDataList(strMSMSData, intMsMsDataCount, sngIonMasses, sngIonIntensities)

                If mFilterSpectraOptions.FilterSpectra AndAlso _
                   (udtSpectrumHeaderInfo.ParentIonMZ < mFilterSpectraOptions.MinimumParentIonMZ OrElse _
                    objSpectrumFilter.EvaluateMsMsSpectrum(sngIonMasses, sngIonIntensities) < 1) Then
                    ' Do not create an entry for this scan since parent ion m/z is too low or spectrum doesn't pass filter
                Else
                    If mSpectrumProcessingOptions.GuesstimateChargeForAllSpectra OrElse _
                       udtSpectrumHeaderInfo.ParentIonChargeCount = 0 OrElse _
                       udtSpectrumHeaderInfo.ParentIonCharges(0) = 0 Then
                        ' Guesstimating charge for all spectra or unknown charge
                        ' Determine the appropriate charge based on the parent ion m/z and the ions that are present
                        objMGFReader.GuesstimateCharge(intDataCount, sngIonMasses, sngIonIntensities, udtSpectrumHeaderInfo, _
                                                        mSpectrumProcessingOptions.GuesstimateChargeForAllSpectra, _
                                                        mSpectrumProcessingOptions.ForceChargeAddnForPredefined2PlusOr3Plus)
                    End If

                    With udtSpectrumHeaderInfo
                        If .ParentIonChargeCount = 0 OrElse .ParentIonCharges(0) = 0 Then
                            ' This code should never be reached
                            .ParentIonChargeCount = 1
                            .ParentIonCharges(0) = 1
                        End If
                    End With

                    For intChargeIndex = 0 To udtSpectrumHeaderInfo.ParentIonChargeCount - 1

                        Try
                            ' The filename consists of the base name, then a . then the StartScanNumber then a . then EndScanNumber then . then Charge then .dta
                            ' The scan numbers are zero-padded to 4 digits
                            With udtSpectrumHeaderInfo
                                strFileName = strFileNameBase & "." & .ScanNumberStart.ToString.PadLeft(4, "0"c) & "." & .ScanNumberEnd.ToString.PadLeft(4, "0"c) & "." & .ParentIonCharges(intChargeIndex).ToString & ".dta"
                            End With

                            ' Create a new .Dta file for this scan
                            strOutputFilePath = System.IO.Path.Combine(strOutputFolderPath, strFileName)
                            srOutFile = New System.IO.StreamWriter(strOutputFilePath)
                        Catch ex As Exception
                            If strOutputFilePath Is Nothing Then strOutputFilePath = strFileNameBase
                            m_ErrMsg = "Error creating DTA file: " & strOutputFilePath & "; " & ex.Message
                            blnSuccess = False
                            Exit For
                        End Try

                        With udtSpectrumHeaderInfo
                            ' MGF files display the parent ion m/z value
                            ' DTA files display the MH+ value of the parent ion, regardless of its charge
                            ' Thus, convert to MH if necessary
                            sngParentIonMH = CSng(objMGFReader.ConvoluteMass(.ParentIonMZ, .ParentIonCharges(intChargeIndex), 1))
                            srOutFile.WriteLine(Math.Round(sngParentIonMH, 3).ToString & " " & .ParentIonCharges(intChargeIndex))
                        End With

                        ' Make sure strMSMSData does not contain any tabs
                        ' Also, make sure it just contains two numbers
                        For intDataIndex = 0 To intMsMsDataCount - 1
                            strMSMSData(intDataIndex) = strMSMSData(intDataIndex).Replace(ControlChars.Tab, " ").Trim

                            ' Make sure strLineOut contains just two numbers
                            strSplitLine = strMSMSData(intDataIndex).Split(" "c)
                            If strSplitLine.Length > 2 Then
                                strMSMSData(intDataIndex) = strSplitLine(0) & " " & strSplitLine(1)
                            End If
                        Next intDataIndex

                        If mSpectrumProcessingOptions.MaximumIonsPerSpectrum > 0 AndAlso _
                           intMsMsDataCount > mSpectrumProcessingOptions.MaximumIonsPerSpectrum Then
                            ' Sort strMsMsData() on descending intensity and only keep the top mMaximumIonsPerSpectrum ions
                            ' First extract out the intensity values from strMsMsData()
                            ReDim sngValueData(intMsMsDataCount - 1)
                            For intDataIndex = 0 To intMsMsDataCount - 1
                                strSplitLine = strMSMSData(intDataIndex).Trim.Split(" "c)

                                If strSplitLine.Length >= 2 Then
                                    Try
                                        sngValueData(intDataIndex) = Single.Parse(strSplitLine(1))
                                    Catch ex As Exception
                                        sngValueData(intDataIndex) = 0
                                    End Try
                                Else
                                    sngValueData(intDataIndex) = 0
                                End If
                            Next intDataIndex

                            ' Sort sngValueData and sort strMsMsData in parallel
                            Array.Sort(sngValueData, strMSMSData)

                            ' Copy the last mMaximumIonsPerSpectrum data points from strMsMsData into strMsMsDataNew
                            ReDim strMsMsDataNew(mSpectrumProcessingOptions.MaximumIonsPerSpectrum - 1)
                            Array.Copy(strMSMSData, intMsMsDataCount - mSpectrumProcessingOptions.MaximumIonsPerSpectrum, strMsMsDataNew, 0, mSpectrumProcessingOptions.MaximumIonsPerSpectrum)

                            ' Now we need to re-sort strMsMsDataNew() on mass
                            ' However, we need to use a numeric sort, so we'll recycle sngValueData and store the masses in it
                            ReDim sngValueData(mSpectrumProcessingOptions.MaximumIonsPerSpectrum - 1)
                            For intDataIndex = 0 To mSpectrumProcessingOptions.MaximumIonsPerSpectrum - 1
                                strSplitLine = strMsMsDataNew(intDataIndex).Trim.Split(" "c)

                                If strSplitLine.Length >= 1 Then
                                    Try
                                        sngValueData(intDataIndex) = Single.Parse(strSplitLine(0))
                                    Catch ex As Exception
                                        sngValueData(intDataIndex) = 0
                                    End Try
                                Else
                                    sngValueData(intDataIndex) = 0
                                End If
                            Next intDataIndex

                            ' Sort sngValueData and sort strMsMsDataNew in parallel
                            Array.Sort(sngValueData, strMsMsDataNew)

                            ' Now write out the first .MaximumIonsPerSpectrum data points in strMsMsDataNew()
                            For intDataIndex = 0 To mSpectrumProcessingOptions.MaximumIonsPerSpectrum - 1
                                srOutFile.WriteLine(strMsMsDataNew(intDataIndex))
                            Next intDataIndex

                        Else
                            ' Write out all of the data
                            For intDataIndex = 0 To intMsMsDataCount - 1
                                srOutFile.WriteLine(strMSMSData(intDataIndex))
                            Next intDataIndex
                        End If

                        ' Add a blank line and close the file
                        srOutFile.WriteLine()
                        srOutFile.Close()

                    Next intChargeIndex

                End If
            End If

        Catch ex As Exception
            m_ErrMsg = "Error in CreateDTAEntry: " & ex.Message
            blnSuccess = False
        End Try

        Return blnSuccess

    End Function

    Private Function GenerateMissingChargeStateDTAFiles(ByVal strWorkingFolderPath As String, ByVal strFileMatchSpec As String, ByVal ExplicitChargeStart As Short, ByVal ExplicitChargeEnd As Short, ByVal RetainDefaultChargeFiles As Boolean) As Boolean
        ' Looks for DTA files in folder strInputFolderPath
        ' Generates missing charge states as directed by ExplicitChargeStart and ExplicitChargeEnd
        ' Returns True if success; false if an error

        Dim DTAFilesList() As String
        Dim blnSuccess As Boolean = False
        Dim strDTAFilepath As String

        Dim strFileDeletionMatchSpec As String

        Dim CurrentCharge As Short

        If ExplicitChargeStart = 0 Or ExplicitChargeEnd = 0 Or ExplicitChargeStart > ExplicitChargeEnd Then
            ' Do not create any explict charges; return true
            Return True
        End If

        Try
            If strFileMatchSpec Is Nothing OrElse strFileMatchSpec.Length = 0 Then
                strFileMatchSpec = "*.dta"
            End If

            ' Generate the additional charge state files for each .Dta file in strWorkingFolderPath
            For Each strDTAFilepath In System.IO.Directory.GetFiles(strWorkingFolderPath, strFileMatchSpec)
                blnSuccess = GenerateMissingChargeStateOneDTA(strDTAFilepath, ExplicitChargeStart, ExplicitChargeEnd)

                If Not blnSuccess Or m_AbortRequested Then
                    Return False
                End If
            Next strDTAFilepath

        Catch ex As Exception
            m_ErrMsg = "Error calling GenerateMissingChargeStateOneDTA: " & ex.Message
            m_status = ISpectraFileProcessor.ProcessStatus.SF_ERROR
            blnSuccess = False
        End Try


        Try
            If blnSuccess And Not RetainDefaultChargeFiles Then
                ' Search for and delete the .Dta files that are not of the desired charge states
                For CurrentCharge = 1 To 3
                    If CurrentCharge < ExplicitChargeStart OrElse CurrentCharge > ExplicitChargeEnd Then
                        strFileDeletionMatchSpec = System.IO.Path.GetFileNameWithoutExtension(strFileMatchSpec)
                        If strFileDeletionMatchSpec Is Nothing OrElse strFileDeletionMatchSpec.Length = 0 Then
                            strFileDeletionMatchSpec = "*"
                        End If

                        strFileDeletionMatchSpec &= "." & CurrentCharge.ToString & ".dta"
                        For Each strDTAFilepath In System.IO.Directory.GetFiles(strWorkingFolderPath, strFileDeletionMatchSpec)
                            System.IO.File.Delete(strDTAFilepath)
                        Next strDTAFilepath
                    End If
                Next CurrentCharge
            End If

        Catch ex As Exception
            m_ErrMsg = "Error deleting unwanted DTA files: " & ex.Message
            m_status = ISpectraFileProcessor.ProcessStatus.SF_ERROR
            blnSuccess = False
        End Try

        Return blnSuccess

    End Function

    Private Function GenerateMissingChargeStateOneDTA(ByVal strInputFilePath As String, ByVal ExplicitChargeStart As Short, ByVal ExplicitChargeEnd As Short) As Boolean
        ' This function opens a DTA file, generates the missing charges and creates the corresponding DTA files for the missing charges.
        ' Returns True if success; false if an error

        Dim ioFileInfo As System.IO.FileInfo
        Dim swOutFile() As System.IO.StreamWriter
        Dim intOutFileCount As Integer

        Dim strOutFileName As String
        Dim strOutputFilePath As String

        Dim intChargeNumber As Integer
        Dim intChargeIndex As Integer
        Dim intLineIndex As Integer

        Dim dblMZ As Double

        Dim objMsMsDataFileReader As MsMsDataFileReader.clsDtaTextFileReader
        Dim dblNewMH As Double

        Dim strMSMSDataList() As String
        Dim intMsMsDataCount As Integer
        Dim udtSpectrumHeaderInfo As MsMsDataFileReader.clsDtaTextFileReader.udtSpectrumHeaderInfoType

        Dim blnCreateEntry As Boolean

        intOutFileCount = 0
        ReDim swOutFile(ExplicitChargeEnd - ExplicitChargeStart + 1)

        Try
            objMsMsDataFileReader = New MsMsDataFileReader.clsDtaTextFileReader
            objMsMsDataFileReader.ReadSingleDtaFile(strInputFilePath, strMSMSDataList, intMsMsDataCount, udtSpectrumHeaderInfo)

            ' Lookup the charge state of the parent ion in the .Dta
            intChargeNumber = udtSpectrumHeaderInfo.ParentIonCharges(0)

            ' Lookup the m/z value
            dblMZ = CDbl(udtSpectrumHeaderInfo.ParentIonMZ.ToString())

            If intChargeNumber = 0 Or dblMZ = 0.0 Then
                ' Skip this .Dta file since invalid charge number of m/z value
                Return True
            End If

            ' First, generate the missing charges and calculate corrsponding M+H values
            '  using the ConvoluteMass function. These values are then written to the
            '  new files that are generated.
            For intChargeIndex = ExplicitChargeStart To ExplicitChargeEnd
                If intChargeIndex <> intChargeNumber Then

                    objMsMsDataFileReader = New MsMsDataFileReader.clsDtaTextFileReader
                    dblNewMH = objMsMsDataFileReader.ConvoluteMass(dblMZ, intChargeIndex, 1)

                    ' Make sure dblNewMH is between mMWLower and mMWUpper

                    blnCreateEntry = True
                    If mMWUpper > 0 AndAlso mMWUpper >= mMWLower Then
                        If dblNewMH < mMWLower Or dblNewMH > mMWUpper Then
                            blnCreateEntry = False
                        End If
                    End If

                    If blnCreateEntry Then
                        ' Generate the file name for a missing charge file
                        ' First strip off .dta
                        strOutFileName = System.IO.Path.GetFileNameWithoutExtension(strInputFilePath)

                        ' Next strip off the charge state (for example .1 or .2 or .3)
                        strOutFileName = System.IO.Path.GetFileNameWithoutExtension(strOutFileName)

                        ' Now define the full path to the new file
                        strOutputFilePath = System.IO.Path.Combine(System.IO.Path.GetDirectoryName(strInputFilePath), strOutFileName & "." & intChargeIndex.ToString & System.IO.Path.GetExtension(strInputFilePath))

                        ' Check if strOutputFilePath points to a valid file
                        ' If it doesn't, then create it
                        ioFileInfo = New System.IO.FileInfo(strOutputFilePath)
                        If Not ioFileInfo.Exists() Then
                            swOutFile(intChargeIndex - ExplicitChargeStart) = New IO.StreamWriter(strOutputFilePath)
                            ' Use the following to write out the data to the new output file
                            swOutFile(intChargeIndex - ExplicitChargeStart).WriteLine(Math.Round(dblNewMH, 4).ToString & " " & intChargeIndex.ToString)
                            intOutFileCount += 1
                        End If
                    End If
                End If
            Next intChargeIndex

            If intOutFileCount > 0 Then
                ' Next, write the mass/intensity pairs to the new DTA files
                For intLineIndex = 0 To intMsMsDataCount - 1
                    For intChargeIndex = 0 To ExplicitChargeEnd - ExplicitChargeStart
                        If Not swOutFile(intChargeIndex) Is Nothing Then
                            swOutFile(intChargeIndex).WriteLine(strMSMSDataList(intLineIndex))
                        End If
                    Next intChargeIndex
                Next intLineIndex

                ' Close the new DTA file(s)
                For intChargeIndex = 0 To ExplicitChargeEnd - ExplicitChargeStart
                    If Not swOutFile(intChargeIndex) Is Nothing Then
                        swOutFile(intChargeIndex).Close()
                        swOutFile(intChargeIndex) = Nothing
                    End If
                Next intChargeIndex
            End If

        Catch ex As Exception
            m_ErrMsg = "Error while processing DTA file: " & strInputFilePath & "; " & ex.Message
            m_status = ISpectraFileProcessor.ProcessStatus.SF_ERROR
            Return False
        End Try

        Return True

    End Function

    Private Function VerifyDtaCreation() As Boolean

        Dim DtaFiles() As String

        'Verify at least one .dta file has been created
        If CountDtaFiles < 1 Then
            m_ErrMsg = "No dta files created"
            Return False
        Else
            Return True
        End If

    End Function

End Class
