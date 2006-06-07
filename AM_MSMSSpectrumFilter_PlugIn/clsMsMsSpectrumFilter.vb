Option Strict On

' This class can be used to evaluate an MS/MS spectrum to see if it passes the given filters
' The spectrum data can be passed directly to the EvaluateMsMsSpectrum() function
' Or, this program can parse an entire _Dta.txt file, creating a new _Dta.txt file and only including those spectra that pass the filters
' Lastly, an entire folder of .Dta files can be parsed; those that fail the filter are renamed to .Bad or not copied to the output folder
'
' Written by Matthew Monroe for the Department of Energy (PNNL, Richland, WA)
' Copyright 2005, Battelle Memorial Institute
' Started November 13, 2003

Public Class clsMsMsSpectrumFilter
    Inherits clsProcessFilesBaseClass

    Public Sub New()
        MyBase.mFileDate = "October 14, 2005"
        InitializeVariables()
    End Sub

#Region "Constants and Enums"
    Private Const DTA_EXTENSION As String = ".DTA"
    Private Const DTA_TXT_EXTENSION As String = "_DTA.TXT"
    Private Const MGF_EXTENSION As String = ".MGF"

    Private Const AA_MASS_COUNT As Integer = 19

    Public Enum eFilterMsMsSpectraErrorCodes
        NoError = 0
        ReservedUnusedError = 1
        UnknownFileExtension = 2        ' This error code matches the identical code in clsMASIC
        InputFileAccessError = 4        ' This error code matches the identical code in clsMASIC
        MissingRequiredDLL = 8
        FileBackupAccessError = 16
        UserCancelledFileOverwrite = 32
        FileCopyError = 64
        FileDeleteError = 128
        SequestParamFileReadError = 256
        UnspecifiedError = -1
    End Enum

    Public Enum eSpectrumFilterMode
        NoFilter = 0
        mode1 = 1               ' Simple filter, only looks at spacing between ions
        mode2 = 2               ' Filter based on Spequal, but Sam Purvine
        mode3 = 3               ' Filter from Eric Stritmatter
    End Enum

    Public Enum eSpectrumFilterMode3_Mode
        NoFilter = 0
        mode1 = 1               ' Looks for a neutral loss of 32.6, 49 and 98
        mode2 = 2               ' Looks for a neutral loss of 32.6, 49, 98 and 80
        mode3 = 3               ' Looks for a neutral loss of 80 
        mode4 = 4               ' Looks for a specific m/z and only retain a dta if that m/z is present at a certain specified abundance
    End Enum

    Public Enum FilterMode1Options
        MinimumStandardMassSpacingIonPairs = 0
        IonPairMassToleranceHalfWidthDa = 1
        NoiseLevelIntensityThreshold = 2
        DataPointCountToConsider = 3
    End Enum

    Public Enum FilterMode2Options
        SignificantIntensityFractionBasePeak = 0
        NoiseThresholdFraction = 1
        Charge1SignificantPeakNumberThreshold = 2
        Charge2SignificantPeakNumberThreshold = 3
        TICThreshold = 4
    End Enum

    Public Enum FilterMode3Options
        BasePeakIntensityMinimum = 0
        MassToleranceHalfWidthMZ = 1
        AbundanceFactor = 2
        PeakMZForMode4 = 3
    End Enum

    Private Structure FilterMode1OptionsType
        Public MinimumStandardMassSpacingIonPairs As Integer
        Public IonPairMassToleranceHalfWidthDa As Single
        Public NoiseLevelIntensityThreshold As Single
        Public DataPointCountToConsider As Integer                ' Maximum number of data points to consider in each spectrum (filter by abundance); set to 0 to consider all of the data
    End Structure

    Private Structure FilterMode2OptionsType
        Public SignificantIntensityFractionBasePeak As Double
        Public NoiseThresholdFraction As Double
        Public Charge1SignificantPeakNumberThreshold As Integer
        Public Charge2SignificantPeakNumberThreshold As Integer
        Public TICThreshold As Double
        Public SequestParamFilePath As String                     ' Sequest Param file to read in order to look for modified amino acid masses
    End Structure

    Private Structure FilterMode3OptionsType
        Public BasePeakIntensityMinimum As Double
        Public MassToleranceHalfWidthMZ As Double
        Public AbundanceFactor As Double
        Public PeakMZForMode4 As Double
        Public Mode As eSpectrumFilterMode3_Mode
    End Structure
#End Region

#Region "Structures"

    Private Structure udtSpectrumQualityEntryType
        Public ScanNumberStart As Integer
        Public ScanNumberEnd As Integer
        Public Charge As Integer
        Public Score As Single
    End Structure

    Private Structure udtNeutralLossMassesType
        Public NeutralLossMass As Double
        Public LowerBoundMZ As Double
        Public UpperBoundMZ As Double
    End Structure
#End Region

#Region "Classwide Variables"

    'filter options variables
    Private mSpectrumFilterMode As eSpectrumFilterMode
    Private mMinimumQualityScore As Double
    Private mGenerateFilterReport As Boolean
    Private mOverwriteExistingFiles As Boolean

    Private mDiscardValidSpectra As Boolean                     ' Set to True to only keep the Invalid spectra, rather than only keeping the Valid spectra
    Private mEvaluateSpectrumQualityOnly As Boolean
    Private mDeleteBadDTAFiles As Boolean

    ' Filter Mode-specific settings
    Private mFilterMode1Options As FilterMode1OptionsType
    Private mFilterMode2Options As FilterMode2OptionsType
    Private mFilterMode3Options As FilterMode3OptionsType

    Private mAminoAcidMassList As Hashtable

    Private mLocalErrorCode As eFilterMsMsSpectraErrorCodes
    Private mErrorMessage As String

    Private tempReportFilePath As String

    Private swReportFile As System.IO.StreamWriter
    Private mReportFilePath As String
    Private mCurrentReportFileName As String

    Private mSettingsLoadedViaCode As Boolean                   ' Set to true to skip loading of settings from a parameter file in LoadParameterFileSettings
    Private mOverwriteReportFile As Boolean                     ' Set to true to re-create the spectrum quality report file
    Private mAutoCloseReportFile As Boolean                     ' This is typically set to false when processing .Dta files; True for other files

#End Region

#Region "Processing Options Interface Functions"

    Public Property AutoCloseReportFile() As Boolean
        Get
            Return mAutoCloseReportFile
        End Get
        Set(ByVal Value As Boolean)
            mAutoCloseReportFile = Value
        End Set
    End Property

    Public Property DeleteBadDTAFiles() As Boolean
        Get
            Return mDeleteBadDTAFiles
        End Get
        Set(ByVal Value As Boolean)
            mDeleteBadDTAFiles = Value
        End Set
    End Property

    Public Property DiscardValidSpectra() As Boolean
        Get
            Return mDiscardValidSpectra
        End Get
        Set(ByVal Value As Boolean)
            mDiscardValidSpectra = Value
        End Set
    End Property

    Public Property EvaluateSpectrumQualityOnly() As Boolean
        Get
            Return mEvaluateSpectrumQualityOnly
        End Get
        Set(ByVal Value As Boolean)
            mEvaluateSpectrumQualityOnly = Value
        End Set
    End Property

    Public Property GenerateFilterReport() As Boolean
        Get
            Return mGenerateFilterReport
        End Get
        Set(ByVal Value As Boolean)
            mGenerateFilterReport = Value
        End Set
    End Property

    Public ReadOnly Property LocalErrorCode() As eFilterMsMsSpectraErrorCodes
        Get
            Return mLocalErrorCode
        End Get
    End Property

    Public Property MinimumQualityScore() As Double
        Get
            Return mMinimumQualityScore
        End Get
        Set(ByVal Value As Double)
            mMinimumQualityScore = Value
        End Set
    End Property

    Public Property OverwriteExistingFiles() As Boolean
        Get
            Return mOverwriteExistingFiles
        End Get
        Set(ByVal Value As Boolean)
            mOverwriteExistingFiles = Value
        End Set
    End Property

    Public Property OverwriteReportFile() As Boolean
        Get
            Return mOverwriteReportFile
        End Get
        Set(ByVal Value As Boolean)
            mOverwriteReportFile = Value
        End Set
    End Property

    Public Property SettingsLoadedViaCode() As Boolean
        Get
            Return mSettingsLoadedViaCode
        End Get
        Set(ByVal Value As Boolean)
            mSettingsLoadedViaCode = Value
        End Set
    End Property

    Public Property FilterMode2_SequestParamFilePath() As String
        Get
            Return mFilterMode2Options.SequestParamFilePath
        End Get
        Set(ByVal Value As String)
            mFilterMode2Options.SequestParamFilePath = Value
        End Set
    End Property

    Public Property FilterMode3_Mode() As eSpectrumFilterMode3_Mode
        Get
            Return mFilterMode3Options.Mode
        End Get
        Set(ByVal Value As eSpectrumFilterMode3_Mode)
            mFilterMode3Options.Mode = Value
        End Set
    End Property

    Public Function GetFilterMode1Option(ByVal SwitchName As FilterMode1Options) As Single
        Select Case SwitchName
            Case FilterMode1Options.MinimumStandardMassSpacingIonPairs
                Return mFilterMode1Options.MinimumStandardMassSpacingIonPairs
            Case FilterMode1Options.IonPairMassToleranceHalfWidthDa
                Return mFilterMode1Options.IonPairMassToleranceHalfWidthDa
            Case FilterMode1Options.NoiseLevelIntensityThreshold
                Return mFilterMode1Options.NoiseLevelIntensityThreshold
            Case FilterMode1Options.DataPointCountToConsider
                Return mFilterMode1Options.DataPointCountToConsider
        End Select
    End Function
    Public Function GetFilterMode2Option(ByVal SwitchName As FilterMode2Options) As Double
        Select Case SwitchName
            Case FilterMode2Options.SignificantIntensityFractionBasePeak
                Return mFilterMode2Options.SignificantIntensityFractionBasePeak
            Case FilterMode2Options.NoiseThresholdFraction
                Return mFilterMode2Options.NoiseThresholdFraction
            Case FilterMode2Options.Charge1SignificantPeakNumberThreshold
                Return mFilterMode2Options.Charge1SignificantPeakNumberThreshold
            Case FilterMode2Options.Charge2SignificantPeakNumberThreshold
                Return mFilterMode2Options.Charge2SignificantPeakNumberThreshold
            Case FilterMode2Options.TICThreshold
                Return mFilterMode2Options.TICThreshold
        End Select
    End Function
    Public Function GetFilterMode3Option(ByVal SwitchName As FilterMode3Options) As Double
        Select Case SwitchName
            Case FilterMode3Options.BasePeakIntensityMinimum
                Return mFilterMode3Options.BasePeakIntensityMinimum
            Case FilterMode3Options.MassToleranceHalfWidthMZ
                Return mFilterMode3Options.MassToleranceHalfWidthMZ
            Case FilterMode3Options.AbundanceFactor
                Return mFilterMode3Options.AbundanceFactor
            Case FilterMode3Options.PeakMZForMode4
                Return mFilterMode3Options.PeakMZForMode4
        End Select
    End Function

    Public Sub SetFilterMode1Option(ByVal SwitchName As FilterMode1Options, ByVal Value As Single)
        Select Case SwitchName
            Case FilterMode1Options.MinimumStandardMassSpacingIonPairs
                mFilterMode1Options.MinimumStandardMassSpacingIonPairs = CInt(Value)
            Case FilterMode1Options.IonPairMassToleranceHalfWidthDa
                mFilterMode1Options.IonPairMassToleranceHalfWidthDa = Value
            Case FilterMode1Options.NoiseLevelIntensityThreshold
                mFilterMode1Options.NoiseLevelIntensityThreshold = Value
            Case FilterMode1Options.DataPointCountToConsider
                mFilterMode1Options.DataPointCountToConsider = CInt(Value)
        End Select
    End Sub
    Public Sub SetFilterMode2Option(ByVal SwitchName As FilterMode2Options, ByVal Value As Double)
        Select Case SwitchName
            Case FilterMode2Options.SignificantIntensityFractionBasePeak
                mFilterMode2Options.SignificantIntensityFractionBasePeak = Value
            Case FilterMode2Options.NoiseThresholdFraction
                mFilterMode2Options.NoiseThresholdFraction = Value
            Case FilterMode2Options.Charge1SignificantPeakNumberThreshold
                mFilterMode2Options.Charge1SignificantPeakNumberThreshold = CInt(Value)
            Case FilterMode2Options.Charge2SignificantPeakNumberThreshold
                mFilterMode2Options.Charge2SignificantPeakNumberThreshold = CInt(Value)
            Case FilterMode2Options.TICThreshold
                mFilterMode2Options.TICThreshold = Value
        End Select
    End Sub
    Public Sub SetFilterMode3Option(ByVal SwitchName As FilterMode3Options, ByVal Value As Double)
        Select Case SwitchName
            Case FilterMode3Options.BasePeakIntensityMinimum
                mFilterMode3Options.BasePeakIntensityMinimum = Value
            Case FilterMode3Options.MassToleranceHalfWidthMZ
                mFilterMode3Options.MassToleranceHalfWidthMZ = Value
            Case FilterMode3Options.AbundanceFactor
                mFilterMode3Options.AbundanceFactor = Value
            Case FilterMode3Options.PeakMZForMode4
                mFilterMode3Options.PeakMZForMode4 = Value
        End Select
    End Sub

    Public Property SpectrumFilterMode() As eSpectrumFilterMode
        Get
            Return mSpectrumFilterMode
        End Get
        Set(ByVal Value As eSpectrumFilterMode)
            mSpectrumFilterMode = Value
        End Set
    End Property

#End Region

    Private Function BackupFileWithRevisioning(ByVal strReportFilePath As String) As Boolean
        ' Returns True if file successfully backed up
        ' Returns False if an error

        Dim ioFile As System.IO.File

        Dim strBackupFilePath As String
        Dim strCheckPath, strCheckPathNew As String

        Dim intIndex As Integer
        Dim blnSuccess As Boolean

        ' Assume success for now
        blnSuccess = True

        Try
            ' See if any .bak files exist
            strBackupFilePath = strReportFilePath & ".bak"

            If ioFile.Exists(strBackupFilePath) Then
                ' Need to find all matching .bak? files and rename; e.g. .bak1-> .bak2, .bak2 -> .bak3, etc.
                ' Must work in reverse order
                For intIndex = 8 To 1 Step -1
                    strCheckPath = strBackupFilePath & intIndex.ToString

                    If ioFile.Exists(strCheckPath) Then
                        strCheckPathNew = strBackupFilePath & (intIndex + 1).ToString
                        If ioFile.Exists(strCheckPathNew) Then
                            ioFile.Delete(strCheckPathNew)
                        End If
                        ioFile.Move(strCheckPath, strCheckPathNew)
                    End If
                Next intIndex

                strCheckPath = strBackupFilePath
                strCheckPathNew = strBackupFilePath & "1"

                ioFile.Move(strCheckPath, strCheckPathNew)
            End If

            ioFile.Copy(strReportFilePath, strBackupFilePath, True)

        Catch ex As Exception
            SetLocalErrorCode(eFilterMsMsSpectraErrorCodes.FileBackupAccessError)
            blnSuccess = False
        End Try

        Return blnSuccess

    End Function

    Private Function CheckExistingFile(ByVal strFilePathToOverwrite As String) As Boolean
        ' Defaults blnAlwaysOverwriteFromNowOnIfOKd to false
        Return CheckExistingFile(strFilePathToOverwrite, False)
    End Function

    Private Function CheckExistingFile(ByVal strFilePathToOverwrite As String, ByVal blnAlwaysOverwriteFromNowOnIfOKd As Boolean) As Boolean
        ' Checks for existing file
        ' If present, and if mOverwriteExistingFiles = False, then asks user whether OK to overwrite
        ' Returns True if OK to proceed, False otherwise

        Dim blnProceed As Boolean
        Dim eResponse As MsgBoxResult

        blnProceed = True
        If Not mOverwriteExistingFiles Then
            Try
                If System.IO.File.Exists(strFilePathToOverwrite) Then
                    ' File already exists in destination; query user about overriding if mShowMessages = True
                    If MyBase.ShowMessages Then
                        eResponse = MsgBox("Overwrite the existing file: " & strFilePathToOverwrite, MsgBoxStyle.Question Or MsgBoxStyle.YesNoCancel Or MsgBoxStyle.DefaultButton2, "File Exists")
                    Else
                        eResponse = MsgBoxResult.No
                    End If

                    If eResponse = MsgBoxResult.Yes Then
                        ' Ok to overwrite the file; possibly always overwrite from now on (typically used when processing individual .dta files)
                        If blnAlwaysOverwriteFromNowOnIfOKd Then mOverwriteExistingFiles = True
                    Else
                        SetLocalErrorCode(eFilterMsMsSpectraErrorCodes.UserCancelledFileOverwrite)
                        blnProceed = False
                    End If
                End If
            Catch ex As Exception
                SetLocalErrorCode(eFilterMsMsSpectraErrorCodes.FileBackupAccessError)
                blnProceed = False
            End Try
        End If

        Return blnProceed

    End Function

    Private Sub CloseReportFile()
        Try
            If Not swReportFile Is Nothing Then
                swReportFile.Close()
                swReportFile = Nothing
                mReportFilePath = String.Empty
            End If
        Catch ex As Exception
            ' Ignore any errors here
        End Try
    End Sub

    ' Filter Mode 1 is based on an algorithm developed by Matthew Monroe at PNNL - eSpectrumFilterMode.mode1
    ' It looks for ion spacings that match the standard amino acid masses
    Public Function EvaluateMsMsSpectrum(ByVal sngMassList() As Single, ByVal sngIntensityList() As Single) As Single
        ' Examines the mass spectrum x,y pairs in sngMassList() and sngIntensityList()
        ' Looks for ion spacings that match the standard amino acid masses (intensity must be >= sngIntensityNoiseLevel)
        ' If the number of spacings is >= mMinimumIonSpacingMatchCount then returns a score of 1
        ' Otherwise, returns 0
        '
        ' If an error occurs, will return a score of 1, rather than marking spectrum as not passing the filter
        ' Assumes that sngMassList() is sorted ascending and sngIntensityList() is sorted parallel with it

        Dim objRangeSearch As New clsSearchRange

        Dim sngSpectrumQualityScore As Single
        Dim intIndex As Integer
        Dim intIonPairCount As Integer

        Dim intDataCount As Integer
        Dim sngWorkingMasses() As Single

        Dim intPointerArray() As Integer
        Dim intPointerArrayToSort() As Integer

        Dim sngSortedIntensityList() As Single
        Dim sngSortedMassList() As Single

        Try
            If mFilterMode1Options.MinimumStandardMassSpacingIonPairs <= 0 Then
                sngSpectrumQualityScore = 1
            ElseIf sngMassList.Length <> sngIntensityList.Length OrElse sngMassList.Length <= 0 Then
                sngSpectrumQualityScore = 0
            Else
                intDataCount = sngMassList.Length
                ReDim intPointerArray(intDataCount - 1)

                If mFilterMode1Options.NoiseLevelIntensityThreshold > 0 Then
                    ' Populate intPointerArray() using only those data points in sngMassList() that have an intensity >= sngIntensityNoiseLevel
                    intDataCount = 0
                    For intIndex = 0 To sngMassList.Length - 1
                        If sngIntensityList(intIndex) >= mFilterMode1Options.NoiseLevelIntensityThreshold Then
                            intPointerArray(intDataCount) = intIndex
                            intDataCount += 1
                        End If
                    Next intIndex

                    If intDataCount > 0 And intDataCount <> sngMassList.Length Then
                        ReDim Preserve intPointerArray(intDataCount - 1)
                    End If
                Else
                    For intIndex = 0 To sngMassList.Length - 1
                        intPointerArray(intIndex) = intIndex
                    Next intIndex
                End If

                If intDataCount > 0 And intDataCount > mFilterMode1Options.MinimumStandardMassSpacingIonPairs Then
                    If mFilterMode1Options.DataPointCountToConsider > 1 And mFilterMode1Options.DataPointCountToConsider < intDataCount Then
                        ' Sort the data by decreasing intensity
                        ' First populate intPointerArrayToSort and sngSortedIntensityList
                        ReDim intPointerArrayToSort(intDataCount - 1)
                        ReDim sngSortedIntensityList(intDataCount - 1)

                        For intIndex = 0 To intDataCount - 1
                            intPointerArrayToSort(intIndex) = intPointerArray(intIndex)
                            sngSortedIntensityList(intIndex) = sngIntensityList(intPointerArray(intIndex))
                        Next intIndex

                        ' Sort intPointerArrayToSort() parallel with sngSortedIntensityList
                        Array.Sort(sngSortedIntensityList, intPointerArrayToSort)

                        ' Reverse the order of the items in intPointerArrayToSort
                        Array.Reverse(intPointerArrayToSort)

                        If intDataCount > mFilterMode1Options.DataPointCountToConsider Then intDataCount = mFilterMode1Options.DataPointCountToConsider

                        ' Copy from intPointerArrayToSort back to intPointerArray, though only keeping the first mDataPointCountToConsider values
                        ReDim intPointerArray(intDataCount - 1)
                        ReDim sngSortedMassList(intDataCount - 1)
                        For intIndex = 0 To intDataCount - 1
                            intPointerArray(intIndex) = intPointerArrayToSort(intIndex)
                            sngSortedMassList(intIndex) = sngMassList(intPointerArrayToSort(intIndex))
                        Next intIndex

                        ' Sort intPointerArray() parallel with sngSortedMassList
                        Array.Sort(sngSortedMassList, intPointerArray)
                    End If

                    ' Populate sngWorkingMasses
                    ReDim sngWorkingMasses(intDataCount - 1)

                    For intIndex = 0 To intDataCount - 1
                        sngWorkingMasses(intIndex) = sngMassList(intPointerArray(intIndex))
                    Next intIndex

                    If Not objRangeSearch.FillWithData(sngWorkingMasses) Then
                        mErrorMessage = "Error calling objRangeSearch.FillWithData in EvaluateMsMsSpectrum (Filter Mode 1)"
                        If MyBase.ShowMessages Then
                            Debug.Assert(False, mErrorMessage)
                        Else
                            Throw New Exception(mErrorMessage)
                        End If
                    Else
                        Dim IEnum As IDictionaryEnumerator = mAminoAcidMassList.GetEnumerator
                        Do While IEnum.MoveNext
                            For intIndex = 0 To intDataCount - 1
                                If objRangeSearch.FindValueRange(sngWorkingMasses(intIndex) + CSng(IEnum.Value), mFilterMode1Options.IonPairMassToleranceHalfWidthDa) Then
                                    intIonPairCount += 1
                                    If intIonPairCount >= mFilterMode1Options.MinimumStandardMassSpacingIonPairs Then Exit Do
                                End If
                            Next intIndex
                        Loop
                    End If

                    If intIonPairCount >= mFilterMode1Options.MinimumStandardMassSpacingIonPairs Then
                        sngSpectrumQualityScore = 1
                    End If
                End If
            End If

        Catch ex As Exception
            If mErrorMessage Is Nothing OrElse mErrorMessage.Length = 0 Then
                mErrorMessage = "Error in EvaluateMsMsSpectrum: " & ex.Message
            End If
            If MyBase.ShowMessages Then
                Debug.Assert(False, mErrorMessage)
            Else
                Throw New Exception(mErrorMessage, ex)
            End If
            sngSpectrumQualityScore = 1
        End Try

        objRangeSearch = Nothing

        Return sngSpectrumQualityScore

    End Function

    ' Filter Mode 2 is based on an algorithm developed by Sam Purvine - eSpectrumFilterMode.mode2
    ' It filters out spectra that do not contain a reasonable number of peaks above a S/N threshold
    Public Function EvaluateMsMsSpectrumMode2(ByVal sngMassList() As Single, ByVal sngIntensityList() As Single, ByVal dblMZ As Double, ByVal intChargeNumber As Integer) As Double

        Dim objMassCalculator As MsMsDataFileReader.clsDtaTextFileReader
        Dim intSignificantPeakNumberCount As Integer
        Dim intCount As Integer
        Dim intArrayLength As Integer
        Dim A, B, dblNoiseThreshold, Score As Double
        Dim intensity, mass, dblIntensityThreshold As Double
        Dim temp, dblBasePeakIntensity As Double
        Dim TIC As Double
        Dim blnHQPass As Boolean
        Dim blnLQPass As Boolean

        Try
            dblBasePeakIntensity = 0.0
            TIC = 0.0
            intArrayLength = 0

            For intIndex As Integer = 0 To sngIntensityList.Length - 1
                Try
                    intensity = CDbl(sngIntensityList(intIndex))
                    intArrayLength += 1
                Catch ex As Exception
                    mErrorMessage = "Error in EvaluateMsMsSpectrumMode2 (loc 1): " & ex.Message
                    If MyBase.ShowMessages Then
                        Debug.Assert(False, mErrorMessage)
                    Else
                        Throw New Exception(mErrorMessage, ex)
                    End If
                    Exit For
                End Try

                If intensity > dblBasePeakIntensity Then
                    dblBasePeakIntensity = intensity
                End If

                TIC += intensity
            Next intIndex

            'Step 1
            dblIntensityThreshold = dblBasePeakIntensity * mFilterMode2Options.SignificantIntensityFractionBasePeak
            intSignificantPeakNumberCount = 0
            For intIndex As Integer = 0 To intArrayLength - 1
                intensity = CDbl(sngIntensityList(intIndex))
                mass = CDbl(sngMassList(intIndex))
                If sngIntensityList(intIndex) > dblIntensityThreshold AndAlso sngMassList(intIndex) > dblMZ Then
                    intSignificantPeakNumberCount += 1
                End If
            Next
            If intChargeNumber > 1 Then
                ' Charge 2+, 3+, etc.
                If intSignificantPeakNumberCount > mFilterMode2Options.Charge2SignificantPeakNumberThreshold Then
                    A = 1.0
                Else
                    A = 0.0
                End If
            Else
                ' Charge 1+
                If intSignificantPeakNumberCount < mFilterMode2Options.Charge1SignificantPeakNumberThreshold Then
                    A = 1.0
                Else
                    A = 0.0
                End If

            End If

            If A > 0 Then

                'Step 2
                B = 0.0
                If TIC > (mFilterMode2Options.TICThreshold) Then           ' 2E6
                    B = 0.25
                End If

                'Step3
                Array.Sort(sngIntensityList, 0, intArrayLength)
                intCount = 0
                dblNoiseThreshold = 0.0
                For intIndex As Integer = 0 To CInt(intArrayLength * mFilterMode2Options.NoiseThresholdFraction)
                    intensity = CDbl(sngIntensityList(intIndex))
                    dblNoiseThreshold += CDbl(intensity)
                    intCount += 1
                Next

                If intCount = 0 Then
                    dblNoiseThreshold = 1
                Else
                    dblNoiseThreshold = dblNoiseThreshold / intCount
                End If

                'Step 4
                Array.Reverse(sngIntensityList, 0, intArrayLength)
                blnHQPass = True
                For intIndex As Integer = 0 To 10
                    intensity = CDbl(sngIntensityList(intIndex))
                    If Not (intensity / dblNoiseThreshold >= 20.0) Then
                        blnHQPass = False
                        Exit For
                    End If
                Next

                If blnHQPass Then
                    B += 0.75
                Else
                    'Step 5
                    blnLQPass = True
                    For intIndex As Integer = 0 To 6
                        intensity = CDbl(sngIntensityList(intIndex))
                        If Not (intensity / dblNoiseThreshold >= 15.0) Then
                            blnLQPass = False
                            Exit For
                        End If
                    Next

                    If blnLQPass Then
                        B += 0.5
                    End If
                End If
            End If

            'Step 6
            Score = A * B

        Catch ex As Exception
            If mErrorMessage Is Nothing OrElse mErrorMessage.Length = 0 Then
                mErrorMessage = "Error in EvaluateMsMsSpectrumMode2: " & ex.Message
            End If
            If MyBase.ShowMessages Then
                Debug.Assert(False, mErrorMessage)
            Else
                Throw New Exception(mErrorMessage, ex)
            End If
        End Try

        Return Score

    End Function

    ' Filter Mode 3 is based on an algorithm developed by Eric Strittmatter at PNNL - eSpectrumFilterMode.mode3
    ' The aim of the filter is to only select spectra that are likely from phosphorylated peptides
    Public Function EvaluateMsMsSpectrumMode3(ByVal sngMassList() As Single, ByVal sngIntensityList() As Single, ByVal dblParentMZ As Double, ByVal intChargeNumber As Integer) As Boolean

        Dim intMassIndex, intIonIndex As Integer
        Dim sngMass As Single

        Dim dblBasePeakIntensity As Double
        Dim dblAbundanceThreshold As Double
        Dim dblPeakMassLowerBound, dblPeakMassUpperBound As Double

        Const NEUTRAL_LOSS_MASS_COUNT As Integer = 4
        Const NEUTRAL_LOSS_INDEX_32 As Integer = 2
        Const NEUTRAL_LOSS_INDEX_80 As Integer = 3
        Dim udtNeutralLossMasses() As udtNeutralLossMassesType

        ReDim udtNeutralLossMasses(NEUTRAL_LOSS_MASS_COUNT - 1)
        udtNeutralLossMasses(0).NeutralLossMass = 98
        udtNeutralLossMasses(1).NeutralLossMass = 49
        udtNeutralLossMasses(NEUTRAL_LOSS_INDEX_32).NeutralLossMass = 32.6
        udtNeutralLossMasses(NEUTRAL_LOSS_INDEX_80).NeutralLossMass = 80

        ' Set up search bounds for eSpectrumFilterMode3_Mode.mode1 through eSpectrumFilterMode3_Mode.mode3
        For intMassIndex = 0 To NEUTRAL_LOSS_MASS_COUNT - 1
            With udtNeutralLossMasses(intMassIndex)
                .LowerBoundMZ = dblParentMZ - .NeutralLossMass - mFilterMode3Options.MassToleranceHalfWidthMZ
                .UpperBoundMZ = dblParentMZ - .NeutralLossMass + mFilterMode3Options.MassToleranceHalfWidthMZ
            End With
        Next intMassIndex

        ' Bounds for eSpectrumFilterMode3_Mode.mode4
        With mFilterMode3Options
            dblPeakMassLowerBound = .PeakMZForMode4 - .MassToleranceHalfWidthMZ
            dblPeakMassUpperBound = .PeakMZForMode4 + .MassToleranceHalfWidthMZ
        End With

        Try
            ' Determine the base peak intensity (maximum intensity in the spectrum)
            dblBasePeakIntensity = 0.0
            For intIonIndex = 0 To sngIntensityList.Length - 1
                Try
                    If sngIntensityList(intIonIndex) > dblBasePeakIntensity Then
                        dblBasePeakIntensity = sngIntensityList(intIonIndex)
                    End If
                Catch ex As Exception
                    mErrorMessage = "Error in EvaluateMsMsSpectrumMode3 (loc 1): " & ex.Message
                    If MyBase.ShowMessages Then
                        Debug.Assert(False, mErrorMessage)
                    Else
                        Throw New Exception(mErrorMessage, ex)
                    End If
                    Exit For
                End Try
            Next intIonIndex

            If dblBasePeakIntensity < mFilterMode3Options.BasePeakIntensityMinimum Then
                Return False
            End If

            dblAbundanceThreshold = dblBasePeakIntensity * mFilterMode3Options.AbundanceFactor

            For intIonIndex = 0 To sngMassList.Length - 1

                Try
                    'testing whether the DTA file passes the filter

                    sngMass = sngMassList(intIonIndex)
                    Select Case mFilterMode3Options.Mode
                        Case eSpectrumFilterMode3_Mode.mode1
                            ' Looks for a neutral loss of 98, 49, and 32.6
                            For intMassIndex = 0 To NEUTRAL_LOSS_INDEX_32
                                If sngMass >= udtNeutralLossMasses(intMassIndex).LowerBoundMZ AndAlso sngMass <= udtNeutralLossMasses(intMassIndex).UpperBoundMZ Then
                                    If sngIntensityList(intIonIndex) >= dblAbundanceThreshold Then
                                        Return True
                                    End If
                                End If
                            Next intMassIndex

                        Case eSpectrumFilterMode3_Mode.mode2
                            ' Looks for a neutral loss of 98, 49, 32.6, and 80
                            For intMassIndex = 0 To NEUTRAL_LOSS_MASS_COUNT - 1
                                If sngMass >= udtNeutralLossMasses(intMassIndex).LowerBoundMZ AndAlso sngMass <= udtNeutralLossMasses(intMassIndex).UpperBoundMZ Then
                                    If sngIntensityList(intIonIndex) >= dblAbundanceThreshold Then
                                        Return True
                                    End If
                                End If
                            Next intMassIndex
                        Case eSpectrumFilterMode3_Mode.mode3
                            ' Looks for a neutral loss of 80
                            intMassIndex = NEUTRAL_LOSS_INDEX_80
                            If sngMass >= udtNeutralLossMasses(intMassIndex).LowerBoundMZ AndAlso sngMass <= udtNeutralLossMasses(intMassIndex).UpperBoundMZ Then
                                If sngIntensityList(intIonIndex) >= dblAbundanceThreshold Then
                                    Return True
                                End If
                            End If

                        Case eSpectrumFilterMode3_Mode.mode4
                            ' Looks for a specific m/z and only retain a dta if that m/z is present at a certain specified abundance
                            If sngMass >= dblPeakMassLowerBound AndAlso sngMass <= dblPeakMassUpperBound Then
                                If sngIntensityList(intIonIndex) >= dblAbundanceThreshold Then
                                    Return True
                                End If
                            End If

                    End Select


                Catch ex As Exception
                    mErrorMessage = "Error in EvaluateMsMsSpectrumMode3 (loc 2): " & ex.Message
                    If MyBase.ShowMessages Then
                        Debug.Assert(False, mErrorMessage)
                    Else
                        Throw New Exception(mErrorMessage, ex)
                    End If
                    Exit For
                End Try

            Next intIonIndex

            Return False

        Catch ex As Exception
            If mErrorMessage Is Nothing OrElse mErrorMessage.Length = 0 Then
                mErrorMessage = "Error in EvaluateMsMsSpectrumMode3 (loc 3): " & ex.Message
            End If
            If MyBase.ShowMessages Then
                Debug.Assert(False, mErrorMessage)
            Else
                Throw New Exception(mErrorMessage, ex)
            End If
        End Try

    End Function

    Public Overrides Function GetDefaultExtensionsToParse() As String()
        Dim strExtensionsToParse(2) As String

        strExtensionsToParse(0) = System.IO.Path.GetExtension(DTA_TXT_EXTENSION)
        strExtensionsToParse(1) = DTA_EXTENSION
        strExtensionsToParse(2) = MGF_EXTENSION

        Return strExtensionsToParse

    End Function

    Public Overrides Function GetErrorMessage() As String
        ' Returns "" if no error

        Dim strErrorMessage As String

        If MyBase.ErrorCode = clsProcessFilesBaseClass.eProcessFilesErrorCodes.LocalizedError Or _
           MyBase.ErrorCode = clsProcessFilesBaseClass.eProcessFilesErrorCodes.NoError Then
            Select Case mLocalErrorCode
                Case eFilterMsMsSpectraErrorCodes.NoError
                    strErrorMessage = ""
                Case eFilterMsMsSpectraErrorCodes.ReservedUnusedError
                    strErrorMessage = "Reserved (unused) error"
                Case eFilterMsMsSpectraErrorCodes.UnknownFileExtension
                    strErrorMessage = "Unknown file extension"
                Case eFilterMsMsSpectraErrorCodes.InputFileAccessError
                    strErrorMessage = "Input file access error"
                Case eFilterMsMsSpectraErrorCodes.MissingRequiredDLL
                    strErrorMessage = "A required DLL was missing"
                Case eFilterMsMsSpectraErrorCodes.FileBackupAccessError
                    strErrorMessage = "File backup access error"
                Case eFilterMsMsSpectraErrorCodes.UserCancelledFileOverwrite
                    strErrorMessage = "User cancelled file overwrite"
                Case eFilterMsMsSpectraErrorCodes.FileCopyError
                    strErrorMessage = "File copy error"
                Case eFilterMsMsSpectraErrorCodes.FileDeleteError
                    strErrorMessage = "File delete error"
                Case eFilterMsMsSpectraErrorCodes.SequestParamFileReadError
                    strErrorMessage = "Sequest param file read error"
                Case eFilterMsMsSpectraErrorCodes.UnspecifiedError
                    strErrorMessage = "Unspecified localized error"
                Case Else
                    ' This shouldn't happen
                    strErrorMessage = "Unknown error state"
            End Select
        Else
            strErrorMessage = MyBase.GetBaseClassErrorMessage()
        End If

        ' Append the local error message, if present
        If Not mErrorMessage Is Nothing AndAlso mErrorMessage.Length > 0 Then
            strErrorMessage &= "; " & mErrorMessage
        End If

        Return strErrorMessage

    End Function

    Private Function GetReportFileName(ByVal strInputFilePath As String) As String
        Dim strReportFileName As String

        strReportFileName = System.IO.Path.GetFileName(strInputFilePath)
        If strReportFileName.EndsWith("dta") Then
            strReportFileName = System.IO.Path.GetDirectoryName(strInputFilePath) & "\" & strReportFileName.Substring(0, strReportFileName.IndexOf(".")) & "_SpectraQuality.txt"
        ElseIf strReportFileName.EndsWith("_dta.txt") Then
            strReportFileName = System.IO.Path.GetDirectoryName(strInputFilePath) & "\" & strReportFileName.Substring(0, strReportFileName.Length - 8) & "_SpectraQuality.txt"
        Else
            strReportFileName = System.IO.Path.GetDirectoryName(strInputFilePath) & "\" & strReportFileName.Substring(0, strReportFileName.Length - 4) & "_SpectraQuality.txt"
            'ends with mgf
        End If

        Return strReportFileName

    End Function

    Private Function GetReportFileName(ByVal strInputFilePath As String, ByVal strOutputFolder As String) As String
        Dim strReportFileName As String

        If strOutputFolder Is Nothing OrElse strOutputFolder.Length = 0 Then
            strReportFileName = System.IO.Path.GetFileName(strInputFilePath)
            If strReportFileName.EndsWith("dta") Then
                strReportFileName = System.IO.Path.GetDirectoryName(strInputFilePath) & "\" & strReportFileName.Substring(0, strReportFileName.IndexOf(".")) & "_SpectraQuality.txt"
            ElseIf strReportFileName.EndsWith("_dta.txt") Then
                strReportFileName = System.IO.Path.GetDirectoryName(strInputFilePath) & "\" & strReportFileName.Substring(0, strReportFileName.Length - 8) & "_SpectraQuality.txt"
            Else
                strReportFileName = System.IO.Path.GetDirectoryName(strInputFilePath) & "\" & strReportFileName.Substring(0, strReportFileName.Length - 4) & "_SpectraQuality.txt"
                'ends with mgf
            End If
        Else
            strReportFileName = System.IO.Path.GetFileName(strInputFilePath)
            If strReportFileName.EndsWith("dta") Then
                strReportFileName = strOutputFolder & "\" & strReportFileName.Substring(0, strReportFileName.IndexOf(".")) & "_SpectraQuality.txt"
            ElseIf strReportFileName.EndsWith("_dta.txt") Then
                strReportFileName = strOutputFolder & "\" & strReportFileName.Substring(0, strReportFileName.Length - 8) & "_SpectraQuality.txt"
            Else
                strReportFileName = strOutputFolder & "\" & strReportFileName.Substring(0, strReportFileName.Length - 4) & "_SpectraQuality.txt"
                'ends with mgf
            End If

        End If

        Return strReportFileName

    End Function

    Private Sub InitializeVariables()

        'Filter options
        mSpectrumFilterMode = eSpectrumFilterMode.mode1
        mMinimumQualityScore = 0.25
        mGenerateFilterReport = True
        mOverwriteExistingFiles = True
        mOverwriteReportFile = True
        mAutoCloseReportFile = True

        mDiscardValidSpectra = False
        mDeleteBadDTAFiles = False
        mEvaluateSpectrumQualityOnly = True

        'filter mode 1
        With mFilterMode1Options
            .MinimumStandardMassSpacingIonPairs = 2
            .IonPairMassToleranceHalfWidthDa = 0.2
            .NoiseLevelIntensityThreshold = 500
            .DataPointCountToConsider = 50
        End With

        'filter mode 2
        With mFilterMode2Options
            .SignificantIntensityFractionBasePeak = 0.05
            .NoiseThresholdFraction = 0.5
            .Charge1SignificantPeakNumberThreshold = 2
            .Charge2SignificantPeakNumberThreshold = 2
            .TICThreshold = 2000000.0
            .SequestParamFilePath = String.Empty
        End With

        'filter mode 3
        With mFilterMode3Options
            .BasePeakIntensityMinimum = 5000
            .MassToleranceHalfWidthMZ = 0.7
            .AbundanceFactor = 0.5
            .PeakMZForMode4 = 500.0
            .Mode = eSpectrumFilterMode3_Mode.mode1
        End With

        ' Populate mAminoAcidMassList with the amino acids
        InitializeAminoAcidMassList(mAminoAcidMassList)

        mLocalErrorCode = eFilterMsMsSpectraErrorCodes.NoError

        mReportFilePath = String.Empty

    End Sub

    Private Sub InitializeAminoAcidMassList(ByRef htMassList As Hashtable)
        ' Note: The amino acid masses are monoisotopic masses, and are the standard AA mass minus H2O
        ' Using the ! symbol to force them to be stored as single precision numbers

        If htMassList Is Nothing Then htMassList = New Hashtable

        htMassList.Clear()
        htMassList.Add("G", 57.02146!)
        htMassList.Add("A", 71.03711!)
        htMassList.Add("S", 87.03203!)      ' Note that the Sequest params file lists this mass as 87.02303, which is incorrect
        htMassList.Add("P", 97.05276!)
        htMassList.Add("V", 99.06841!)
        htMassList.Add("T", 101.047676!)
        htMassList.Add("C", 103.009186!)
        htMassList.Add("L", 113.084061!)
        htMassList.Add("N", 114.042923!)
        htMassList.Add("D", 115.026939!)
        htMassList.Add("Q", 128.058578!)
        htMassList.Add("K", 128.094955!)
        htMassList.Add("E", 129.042587!)
        htMassList.Add("M", 131.040482!)
        htMassList.Add("H", 137.058914!)
        htMassList.Add("F", 147.0684!)
        htMassList.Add("R", 156.1011!)
        htMassList.Add("Y", 163.063324!)
        htMassList.Add("W", 186.079315!)

    End Sub

    Public Function LoadParameterFileSettings(ByVal strParameterFilePath As String) As Boolean
        ' Returns True if no error; otherwise, returns False
        ' If strParameterFilePath is blank, then returns True since this isn't an error

        Const FILTER_OPTIONS_SECTION As String = "FilterOptions"
        Const FILTER_MODE1 As String = "FilterMode1"
        Const FILTER_MODE2 As String = "FilterMode2"
        Const FILTER_MODE3 As String = "FilterMode3"

        Dim objSettingsFile As New PRISM.Files.XmlSettingsFileAccessor
        Dim ioFile As System.IO.File
        Dim ioPath As System.IO.Path

        If mSettingsLoadedViaCode Then Return True

        Try

            If strParameterFilePath Is Nothing OrElse strParameterFilePath.Length = 0 Then
                ' No parameter file specified; nothing to load
                Return True
            End If

            If Not ioFile.Exists(strParameterFilePath) Then
                ' See if strParameterFilePath points to a file in the same directory as the application
                strParameterFilePath = ioPath.Combine(ioPath.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location), ioPath.GetFileName(strParameterFilePath))
                If Not ioFile.Exists(strParameterFilePath) Then
                    MyBase.SetBaseClassErrorCode(clsProcessFilesBaseClass.eProcessFilesErrorCodes.ParameterFileNotFound)
                    mErrorMessage = MyBase.GetBaseClassErrorMessage()
                    Return False
                End If
            End If

            If objSettingsFile.LoadSettings(strParameterFilePath) Then
                If Not objSettingsFile.SectionPresent(FILTER_OPTIONS_SECTION) Then
                    mErrorMessage = "The node '<section name=""" & FILTER_OPTIONS_SECTION & """> was not found in the parameter file: " & strParameterFilePath
                    If MyBase.ShowMessages Then
                        MsgBox(mErrorMessage, MsgBoxStyle.Exclamation Or MsgBoxStyle.OKOnly, "Invalid File")
                    End If
                    MyBase.SetBaseClassErrorCode(clsProcessFilesBaseClass.eProcessFilesErrorCodes.InvalidParameterFile)
                    Return False
                Else
                    Me.SpectrumFilterMode = CType(objSettingsFile.GetParam(FILTER_OPTIONS_SECTION, "FilterMode", CInt(Me.SpectrumFilterMode)), eSpectrumFilterMode)
                    Me.MinimumQualityScore = objSettingsFile.GetParam(FILTER_OPTIONS_SECTION, "MinimumQualityScore", Me.MinimumQualityScore)
                    Me.GenerateFilterReport = objSettingsFile.GetParam(FILTER_OPTIONS_SECTION, "GenerateFilterReport", Me.GenerateFilterReport)
                    Me.OverwriteExistingFiles = objSettingsFile.GetParam(FILTER_OPTIONS_SECTION, "OverwriteExistingFiles", Me.OverwriteExistingFiles)
                    Me.DiscardValidSpectra = objSettingsFile.GetParam(FILTER_OPTIONS_SECTION, "DiscardValidSpectra", Me.DiscardValidSpectra)
                    Me.EvaluateSpectrumQualityOnly = objSettingsFile.GetParam(FILTER_OPTIONS_SECTION, "EvaluateSpectrumQualityOnly", Me.EvaluateSpectrumQualityOnly)
                End If

                If Not objSettingsFile.SectionPresent(FILTER_MODE1) Then
                    If SpectrumFilterMode = eSpectrumFilterMode.mode1 Then
                        mErrorMessage = "The node '<section name=""" & FILTER_MODE1 & """> was not found in the parameter file: " & strParameterFilePath
                        If MyBase.ShowMessages Then
                            MsgBox(mErrorMessage, MsgBoxStyle.Exclamation Or MsgBoxStyle.OKOnly, "Invalid File")
                        End If
                        MyBase.SetBaseClassErrorCode(clsProcessFilesBaseClass.eProcessFilesErrorCodes.InvalidParameterFile)
                        Return False
                    End If
                Else
                    With mFilterMode1Options
                        .MinimumStandardMassSpacingIonPairs = objSettingsFile.GetParam(FILTER_MODE1, "MinimumStandardMassSpacingIonPairs", .MinimumStandardMassSpacingIonPairs)
                        .IonPairMassToleranceHalfWidthDa = objSettingsFile.GetParam(FILTER_MODE1, "IonPairMassToleranceHalfWidthDa", .IonPairMassToleranceHalfWidthDa)
                        .NoiseLevelIntensityThreshold = objSettingsFile.GetParam(FILTER_MODE1, "NoiseLevelIntensityThreshold", .NoiseLevelIntensityThreshold)
                        .DataPointCountToConsider = objSettingsFile.GetParam(FILTER_MODE1, "DataPointCountToConsider", .DataPointCountToConsider)
                    End With
                End If

                If Not objSettingsFile.SectionPresent(FILTER_MODE2) Then
                    If SpectrumFilterMode = eSpectrumFilterMode.mode2 Then
                        mErrorMessage = "The node '<section name=""" & FILTER_MODE2 & """> was not found in the parameter file: " & strParameterFilePath
                        If MyBase.ShowMessages Then
                            MsgBox(mErrorMessage, MsgBoxStyle.Exclamation Or MsgBoxStyle.OKOnly, "Invalid File")
                        End If
                        MyBase.SetBaseClassErrorCode(clsProcessFilesBaseClass.eProcessFilesErrorCodes.InvalidParameterFile)
                        Return False
                    End If
                Else
                    With mFilterMode2Options
                        .SignificantIntensityFractionBasePeak = objSettingsFile.GetParam(FILTER_MODE2, "SignificantIntensityFractionBasePeak", .SignificantIntensityFractionBasePeak)
                        .NoiseThresholdFraction = objSettingsFile.GetParam(FILTER_MODE2, "NoiseThresholdFraction", .NoiseThresholdFraction)
                        .Charge1SignificantPeakNumberThreshold = objSettingsFile.GetParam(FILTER_MODE2, "Charge1SignificantPeakNumberThreshold", .Charge1SignificantPeakNumberThreshold)
                        .Charge2SignificantPeakNumberThreshold = objSettingsFile.GetParam(FILTER_MODE2, "Charge2SignificantPeakNumberThreshold", .Charge2SignificantPeakNumberThreshold)
                        .TICThreshold = objSettingsFile.GetParam(FILTER_MODE2, "TICThreshold", .TICThreshold)
                    End With
                End If

                If Not objSettingsFile.SectionPresent(FILTER_MODE3) Then
                    If SpectrumFilterMode = eSpectrumFilterMode.mode3 Then
                        mErrorMessage = "The node '<section name=""" & FILTER_MODE3 & """> was not found in the parameter file: " & strParameterFilePath
                        If MyBase.ShowMessages Then
                            MsgBox(mErrorMessage, MsgBoxStyle.Exclamation Or MsgBoxStyle.OKOnly, "Invalid File")
                        End If
                        MyBase.SetBaseClassErrorCode(clsProcessFilesBaseClass.eProcessFilesErrorCodes.InvalidParameterFile)
                        Return False
                    End If
                Else
                    With mFilterMode3Options
                        .BasePeakIntensityMinimum = objSettingsFile.GetParam(FILTER_MODE3, "BasePeakIntensityMinimum", .BasePeakIntensityMinimum)
                        .MassToleranceHalfWidthMZ = objSettingsFile.GetParam(FILTER_MODE3, "MassToleranceHalfWidthMZ", .MassToleranceHalfWidthMZ)
                        .AbundanceFactor = objSettingsFile.GetParam(FILTER_MODE3, "AbundanceFactor", .AbundanceFactor)
                        .PeakMZForMode4 = objSettingsFile.GetParam(FILTER_MODE3, "PeakMZ", .PeakMZForMode4)
                        .Mode = CType(objSettingsFile.GetParam(FILTER_MODE3, "Mode", CType(.Mode, Integer)), eSpectrumFilterMode3_Mode)
                    End With
                End If
            End If

        Catch ex As Exception
            mErrorMessage = "Error in LoadParameterFileSettings: " & ex.Message
            If MyBase.ShowMessages Then
                MsgBox(mErrorMessage, MsgBoxStyle.Exclamation Or MsgBoxStyle.OKOnly, "Error")
            Else
                Throw New System.Exception(mErrorMessage, ex)
            End If
            Return False
        End Try

        Return True

    End Function

    'Private Function MedianIntensityDTATextOrMGF(ByRef objFileReader As MsMsDataFileReader.clsMsMsDataFileReaderBaseClass, ByVal strInputFilePath As String, ByRef blnSuccess As Boolean) As Single
    '    ' Pre-read the entire file to determine the median intensity of all of the data

    '    Dim sngMedianIntensity As Single
    '    Dim blnSpectrumFound As Boolean

    '    Dim alMSMSData As New ArrayList
    '    Dim udtSpectrumHeaderInfo As MsMsDataFileReader.clsMsMsDataFileReaderBaseClass.udtSpectrumHeaderInfoType

    '    Dim sngMassList() As Single
    '    Dim sngIntensityList() As Single
    '    Dim intDataCount As Integer
    '    Dim intProcessCount As Integer

    '    Dim sngGlobalIntensityList() As Single

    '    Dim intIndex, intGlobalDataCount, intMidpointIndex As Integer

    '    sngMedianIntensity = 0
    '    ReDim sngGlobalIntensityList(0)
    '    intGlobalDataCount = 0

    '    Try
    '        ' Open the input file and parse it
    '        If Not objFileReader.OpenFile(strInputFilePath) Then
    '            SetLocalErrorCode(eFilterMsMsSpectraErrorCodes.InputFileAccessError)
    '            blnSuccess = False
    '            Exit Try
    '        End If

    '        intProcessCount = 0
    '        Console.WriteLine("  Determining median data intensity for file")
    '        Console.Write("  ")

    '        Do

    '            ' Look for the next spectrum
    '            blnSpectrumFound = objFileReader.ReadNextSpectrum(alMSMSData, udtSpectrumHeaderInfo)
    '            If blnSpectrumFound Then
    '                ' Populate sngMassList and sngIntensityList
    '                intDataCount = objFileReader.ParseMsMsDataList(alMSMSData, sngMassList, sngIntensityList)

    '                ' Append the data to sngGlobalIntensityList
    '                ReDim Preserve sngGlobalIntensityList(intGlobalDataCount + intDataCount)

    '                For intIndex = 0 To intDataCount - 1
    '                    sngGlobalIntensityList(intGlobalDataCount + intIndex) = sngIntensityList(intIndex)
    '                Next intIndex
    '                intGlobalDataCount += intDataCount

    '                intProcessCount += 1
    '                If intProcessCount Mod 100 = 0 Then
    '                    Console.Write(".")
    '                End If

    '            End If
    '        Loop While blnSpectrumFound

    '        If intProcessCount >= 100 Then Console.WriteLine()

    '        objFileReader.CloseFile()
    '        blnSuccess = True

    '        ' Find the median
    '        If intGlobalDataCount > 0 Then
    '            ReDim Preserve sngGlobalIntensityList(intGlobalDataCount - 1)

    '            Array.Sort(sngGlobalIntensityList)

    '            intMidpointIndex = CInt(intGlobalDataCount / 2)
    '            If intMidpointIndex < 0 Then intMidpointIndex = 0
    '            sngMedianIntensity = sngGlobalIntensityList(intMidpointIndex)

    '        Else
    '            sngMedianIntensity = 0
    '        End If

    '    Catch ex As Exception
    '        SetLocalErrorCode(eFilterMsMsSpectraErrorCodes.InputFileAccessError)
    '        blnSuccess = False
    '    End Try

    '    Return sngMedianIntensity
    'End Function

    Public Sub ModifyStandardAminoAcidMass(ByVal strAminoAcidSymbolOneLetter As String, ByVal sngModMass As Single)

        Dim htStandardMasses As New Hashtable
        InitializeAminoAcidMassList(htStandardMasses)

        Try
            strAminoAcidSymbolOneLetter = strAminoAcidSymbolOneLetter.ToUpper
            If mAminoAcidMassList.ContainsKey(strAminoAcidSymbolOneLetter) Then
                mAminoAcidMassList(strAminoAcidSymbolOneLetter) = CSng(htStandardMasses(strAminoAcidSymbolOneLetter)) + sngModMass
            End If
        Catch ex As Exception
            mErrorMessage = "Error in ModifyStandardAminoAcidMass: " & ex.Message
            If MyBase.ShowMessages Then
                Debug.Assert(False, mErrorMessage)
            Else
                Throw New Exception(mErrorMessage, ex)
            End If
        End Try

    End Sub

    Private Function ParseSequestParamFile(ByVal strSequestParamFilePath As String) As Boolean
        ' Parse a Sequest Param file to determine any modified amino acid masses

        Dim ioFile As System.IO.FileInfo
        Dim strSequestParamFilePathFull As String

        Dim srInFile As System.IO.StreamReader
        Dim strLineIn As String
        Dim intCharLoc As Integer

        Dim strAminoAcidSymbolOneLetter As String
        Dim sngModMass As Single

        Dim blnSuccess As Boolean

        blnSuccess = False
        Try
            ' Obtain the full path to the input file
            ioFile = New System.IO.FileInfo(strSequestParamFilePath)
            strSequestParamFilePathFull = ioFile.FullName

            If Not System.IO.File.Exists(strSequestParamFilePathFull) Then
                SetLocalErrorCode(eFilterMsMsSpectraErrorCodes.SequestParamFileReadError)
                blnSuccess = False
                mErrorMessage = "Sequest param file not found " & strSequestParamFilePathFull
                If MyBase.ShowMessages Then
                    MsgBox(mErrorMessage, MsgBoxStyle.Exclamation Or MsgBoxStyle.OKOnly, "Missing file")
                End If
            Else
                srInFile = New System.IO.StreamReader(strSequestParamFilePathFull)

                Do While srInFile.Peek() >= 0
                    strLineIn = srInFile.ReadLine
                    If Not strLineIn Is Nothing AndAlso strLineIn.Trim.Length > 0 Then
                        strLineIn = strLineIn.Trim

                        ' Look for lines similar to "add_G_Glycine = 0.0000"
                        If strLineIn.StartsWith("add_") Then
                            strLineIn = strLineIn.Substring(4)
                            If strLineIn.Substring(1, 1) = "_" Then
                                ' Amino acid modification found

                                strAminoAcidSymbolOneLetter = strLineIn.Substring(0, 1).ToUpper

                                If mAminoAcidMassList.ContainsKey(strAminoAcidSymbolOneLetter) Then
                                    strLineIn = strLineIn.Substring(2)

                                    intCharLoc = strLineIn.IndexOf("=")
                                    If intCharLoc > 0 Then
                                        strLineIn = strLineIn.Substring(intCharLoc + 1).Trim

                                        intCharLoc = strLineIn.IndexOf(" ")
                                        If intCharLoc > 0 Then
                                            strLineIn = strLineIn.Substring(0, intCharLoc - 1).Trim

                                            If IsNumeric(strLineIn) Then
                                                sngModMass = CSng(strLineIn)

                                                If sngModMass <> 0 Then
                                                    ModifyStandardAminoAcidMass(strAminoAcidSymbolOneLetter, sngModMass)
                                                End If

                                            End If
                                        End If
                                    End If
                                End If
                            End If
                        End If
                    End If
                Loop
                srInFile.Close()

                blnSuccess = True
            End If
        Catch ex As Exception
            ' Error
            SetLocalErrorCode(eFilterMsMsSpectraErrorCodes.SequestParamFileReadError)
            blnSuccess = False

            mErrorMessage = "Error reading the Sequest param file: " & ex.Message
            If MyBase.ShowMessages Then
                MsgBox(mErrorMessage, MsgBoxStyle.Exclamation Or MsgBoxStyle.OKOnly, "Error")
            Else
                Throw New Exception(mErrorMessage, ex)
            End If
        End Try

        Return blnSuccess

    End Function

    Private Function ProcessDtaFile(ByVal strInputFilePath As String, ByVal strOutputFolderPath As String) As Boolean
        ' Processes a single Dta file
        ' Returns True if success, False if failure
        '
        ' If strOutputFolderPath is empty or is the same folder as strInputFilePath's folder, then if the
        '   spectrum fails the filter, then the file is renamed to .dta.old
        ' Otherwise if strOutputFolderPath points to another folder, then if the spectrum passes the filter,
        '   then it is copied to the output folder

        Dim ioPath As System.IO.Path
        Dim ioFile As System.IO.File

        Dim objDtaTextFileReader As New MsMsDataFileReader.clsDtaTextFileReader

        Dim strMSMSDataList() As String
        Dim intMsMsDataCount As Integer
        Dim udtSpectrumHeaderInfo As MsMsDataFileReader.clsMsMsDataFileReaderBaseClass.udtSpectrumHeaderInfoType

        Dim intDataCount As Integer
        Dim sngMassList() As Single
        Dim sngIntensityList() As Single

        Dim blnSuccess As Boolean

        Dim strNewFilePath As String
        Dim blnValidOutputFolder As Boolean
        Dim dblSpectrumQualityScore As Double

        Dim DirectoryName As String

        Static intFilesProcessed As Integer = 0
        Static intFileCount As Integer = 0
        Static intFileCountFolderName As String = String.Empty

        blnSuccess = True

        Try

            blnValidOutputFolder = ValidateOutputFolder(strInputFilePath, strOutputFolderPath)
            DirectoryName = ioPath.GetDirectoryName(strInputFilePath)

            ' Only lookup the number of .Dta files on the first call to this function
            If intFileCount = 0 OrElse intFileCountFolderName Is Nothing OrElse intFileCountFolderName <> DirectoryName Then
                intFileCountFolderName = String.Copy(DirectoryName)
                intFileCount = System.IO.Directory.GetFiles(intFileCountFolderName, "*.dta").GetLength(0)
                intFilesProcessed = 0
            End If

            intFilesProcessed += 1

            If intFileCount > 0 Then
                UpdateProgress("Filtering DTAs: File " & intFilesProcessed.ToString & " of " & intFileCount.ToString, intFilesProcessed / CSng(intFileCount) * 100)
            Else
                UpdateProgress("Filtering DTAs: File " & intFilesProcessed.ToString, 0)
            End If


            If objDtaTextFileReader.ReadSingleDtaFile(strInputFilePath, strMSMSDataList, intMsMsDataCount, udtSpectrumHeaderInfo) Then
                ' Populate sngMassList and sngIntensityList
                intDataCount = objDtaTextFileReader.ParseMsMsDataList(strMSMSDataList, intMsMsDataCount, sngMassList, sngIntensityList)

                ' Call EvaluateMsMsSpectrum()
                If intDataCount > 0 Then
                    Select Case mSpectrumFilterMode
                        Case eSpectrumFilterMode.mode2
                            dblSpectrumQualityScore = EvaluateMsMsSpectrumMode2(sngMassList, sngIntensityList, udtSpectrumHeaderInfo.ParentIonMZ, udtSpectrumHeaderInfo.ParentIonCharges(0))
                        Case eSpectrumFilterMode.mode3
                            If EvaluateMsMsSpectrumMode3(sngMassList, sngIntensityList, udtSpectrumHeaderInfo.ParentIonMZ, udtSpectrumHeaderInfo.ParentIonCharges(0)) Then
                                ' Spectrum passed filter; guarantee that it's score value is greater than the minimum
                                dblSpectrumQualityScore = mMinimumQualityScore + 1
                            Else
                                ' Spectrum failed filter; guarantee that it's score value is less than the minimum
                                dblSpectrumQualityScore = mMinimumQualityScore - 1
                            End If
                        Case Else
                            ' Includes eSpectrumFilterMode.mode1
                            dblSpectrumQualityScore = CDbl(EvaluateMsMsSpectrum(sngMassList, sngIntensityList))
                    End Select
                Else
                    dblSpectrumQualityScore = -1
                End If

                If mEvaluateSpectrumQualityOnly Or mGenerateFilterReport Then
                    ' Add a new row to the report file
                    Report(GetReportFileName(strInputFilePath, strOutputFolderPath), udtSpectrumHeaderInfo.ScanNumberStart, udtSpectrumHeaderInfo.ScanNumberEnd, udtSpectrumHeaderInfo.ParentIonCharges(0), dblSpectrumQualityScore)
                End If

                If Not mEvaluateSpectrumQualityOnly Then
                    'if we want to filter out the spectra
                    If (Not mDiscardValidSpectra And dblSpectrumQualityScore >= mMinimumQualityScore) Or _
                           (mDiscardValidSpectra And dblSpectrumQualityScore < mMinimumQualityScore) Then

                        '(Not mDiscardValidSpectra And dblSpectrumQualityScore >= mMinimumQualityScore)--> keeps the spectra that passed the filter
                        '(mDiscardValidSpectra And dblSpectrumQualityScore < mMinimumQualityScore)--> keeps the spectra that does not pass the filter

                        'spectra that we want to keep
                        If blnValidOutputFolder Then
                            'if the user provided a valid output folder then we move .dta files to the output folder:
                            strNewFilePath = ioPath.Combine(strOutputFolderPath, ioPath.GetFileName(strInputFilePath))

                            If Not CheckExistingFile(strNewFilePath, True) Then
                                Exit Try
                            Else
                                Try
                                    ioFile.Copy(strInputFilePath, strNewFilePath)
                                Catch ex As Exception
                                    mErrorMessage = "Error copying " & strInputFilePath & " to " & strNewFilePath
                                    SetLocalErrorCode(eFilterMsMsSpectraErrorCodes.FileCopyError)
                                    If MyBase.ShowMessages Then
                                        Debug.Assert(False, mErrorMessage)
                                    Else
                                        Throw New Exception(mErrorMessage, ex)
                                    End If
                                End Try
                            End If
                        Else
                            'file passed filter and since no output folder was provided we keep it in its present folder
                        End If
                    Else
                        'spectra that we do not want to keep
                        If blnValidOutputFolder Then
                            'we do nothing
                            'we do not want to move the unwanted spectra to the output folder 
                        Else
                            If mDeleteBadDTAFiles Then
                                'no output folder was provided and mDeleteBadDTAFiles = True, so delete the .dta file
                                Try
                                    ioFile.Delete(strInputFilePath)
                                Catch ex As Exception
                                    mErrorMessage = "Error deleting " & strInputFilePath
                                    SetLocalErrorCode(eFilterMsMsSpectraErrorCodes.FileDeleteError)
                                    If MyBase.ShowMessages Then
                                        Debug.Assert(False, mErrorMessage)
                                    Else
                                        Throw New Exception(mErrorMessage, ex)
                                    End If
                                End Try
                            Else
                                'no output folder was provided so we rename the unwanted spectra to .bad since mDeleteBadDTAFiles = False
                                strNewFilePath = ioPath.ChangeExtension(strInputFilePath, ".bad")

                                If Not CheckExistingFile(strNewFilePath, True) Then
                                    Exit Try
                                Else
                                    Try
                                        ioFile.Copy(strInputFilePath, strNewFilePath)

                                        Try
                                            ioFile.Delete(strInputFilePath)
                                        Catch ex As Exception
                                            mErrorMessage = "Error deleting " & strInputFilePath
                                            SetLocalErrorCode(eFilterMsMsSpectraErrorCodes.FileDeleteError)
                                            If MyBase.ShowMessages Then
                                                Debug.Assert(False, mErrorMessage)
                                            Else
                                                Throw New Exception(mErrorMessage, ex)
                                            End If
                                        End Try

                                    Catch ex As Exception
                                        mErrorMessage = "Error copying " & strInputFilePath & " to " & strNewFilePath
                                        SetLocalErrorCode(eFilterMsMsSpectraErrorCodes.FileCopyError)
                                        If MyBase.ShowMessages Then
                                            Debug.Assert(False, mErrorMessage)
                                        Else
                                            Throw New Exception(mErrorMessage, ex)
                                        End If
                                    End Try
                                End If

                            End If
                        End If
                    End If
                End If
            End If


        Catch ex As Exception
            mErrorMessage = "Error in ProcessDtaFile: " & ex.Message
            SetLocalErrorCode(eFilterMsMsSpectraErrorCodes.InputFileAccessError)
            If MyBase.ShowMessages Then
                Debug.Assert(False, mErrorMessage)
            Else
                Throw New Exception(mErrorMessage, ex)
            End If
            blnSuccess = False
        End Try

        Return blnSuccess

    End Function

    Private Function ProcessDtaTxtFile(ByVal strInputFilePath As String, ByVal strOutputFolderPath As String) As Boolean

        Dim blnCombineIdenticalSpectra As Boolean
        blnCombineIdenticalSpectra = False

        Dim objDtaTextFileReader As MsMsDataFileReader.clsMsMsDataFileReaderBaseClass
        objDtaTextFileReader = New MsMsDataFileReader.clsDtaTextFileReader(False)

        ProcessDtaTxtFile = ProcessDTATextOrMGF(objDtaTextFileReader, strInputFilePath, strOutputFolderPath)

    End Function

    Private Function ProcessMascotGenericFile(ByVal strInputFilePath As String, ByVal strOutputFolderPath As String) As Boolean
        Dim objMGFReader As MsMsDataFileReader.clsMsMsDataFileReaderBaseClass
        objMGFReader = New MsMsDataFileReader.clsMGFReader

        ProcessMascotGenericFile = ProcessDTATextOrMGF(objMGFReader, strInputFilePath, strOutputFolderPath)

    End Function

    Private Function ProcessDTATextOrMGF(ByRef objFileReader As MsMsDataFileReader.clsMsMsDataFileReaderBaseClass, ByVal strInputFilePath As String, ByVal strOutputFolderPath As String) As Boolean

        Dim ioPath As System.IO.Path

        Dim srOutFile As System.IO.StreamWriter

        Dim strMSMSDataList() As String
        Dim udtSpectrumHeaderInfo As MsMsDataFileReader.clsMsMsDataFileReaderBaseClass.udtSpectrumHeaderInfoType

        Dim intDataCount, intProcessCount, intMsMsDataCount As Integer
        Dim sngMassList() As Single
        Dim sngIntensityList() As Single

        Dim strMostRecentSpectrumText As String
        Dim strOutputFilePath As String
        Dim blnValidOutputFolder As Boolean

        Dim blnProceed, blnSuccess As Boolean
        Dim blnSpectrumFound As Boolean
        Dim dblSpectrumQualityScore As Double

        Dim intSpectraRead As Integer
        Dim intProgressPercentComplete As Integer

        Try

            blnValidOutputFolder = ValidateOutputFolder(strInputFilePath, strOutputFolderPath)

            If blnValidOutputFolder Then
                ' Note that we do not create backups of files to be overwritten if an Output Folder is defined
                strOutputFilePath = ioPath.Combine(strOutputFolderPath, ioPath.GetFileName(strInputFilePath))

                blnProceed = CheckExistingFile(strOutputFilePath)
                If Not blnProceed Then Exit Try

                blnProceed = True
            Else
                ' Create a backup copy of the input file, unless mEvaluateSpectrumQualityOnly = True
                If mEvaluateSpectrumQualityOnly Then
                    strOutputFilePath = String.Empty
                    blnProceed = True
                Else
                    blnProceed = BackupFileWithRevisioning(strInputFilePath)

                    ' Switch around the filenames as needed
                    strOutputFilePath = strInputFilePath
                    strInputFilePath = strInputFilePath & ".bak"
                End If
            End If

            If Not blnProceed Then Exit Try

            If Not mEvaluateSpectrumQualityOnly Then
                ' Create the output file
                srOutFile = New IO.StreamWriter(strOutputFilePath)

                ' Write a blank line to the start of the output file
                srOutFile.WriteLine()
            End If

            ''' Pre-read the entire file to determine the median intensity of all of the data
            ''sngMedianIntensity = MedianIntensityDTATextOrMGF(objFileReader, strInputFilePath, blnSuccess)
            ''If sngMedianIntensity > 0 Then mNoiseLevelIntensityThreshold = sngMedianIntensity

            ' Open the input file and parse it
            If Not objFileReader.OpenFile(strInputFilePath) Then
                SetLocalErrorCode(eFilterMsMsSpectraErrorCodes.InputFileAccessError)
                blnSuccess = False
                Exit Try
            End If

            intProcessCount = 0
            Console.Write("  ")

            Do
                Try

                    ' Look for the next spectrum
                    blnSpectrumFound = objFileReader.ReadNextSpectrum(strMSMSDataList, intMsMsDataCount, udtSpectrumHeaderInfo)
                    intSpectraRead += 1

                    If intSpectraRead Mod 25 = 0 Then
                        ' Update the label with the progress
                        intProgressPercentComplete = CInt(Math.Round(objFileReader.ProgressPercentComplete(), 0))
                        UpdateProgress("Filtering Concatinated File: " & intProgressPercentComplete.ToString & " % complete", intProgressPercentComplete)
                    End If

                    If blnSpectrumFound Then
                        ' Populate sngMassList and sngIntensityList
                        intDataCount = objFileReader.ParseMsMsDataList(strMSMSDataList, intMsMsDataCount, sngMassList, sngIntensityList)

                        ' Call EvaluateMsMsSpectrum()
                        If intDataCount > 0 Then
                            Select Case mSpectrumFilterMode
                                Case eSpectrumFilterMode.mode2
                                    dblSpectrumQualityScore = EvaluateMsMsSpectrumMode2(sngMassList, sngIntensityList, udtSpectrumHeaderInfo.ParentIonMZ, udtSpectrumHeaderInfo.ParentIonCharges(0))
                                Case eSpectrumFilterMode.mode3
                                    If EvaluateMsMsSpectrumMode3(sngMassList, sngIntensityList, udtSpectrumHeaderInfo.ParentIonMZ, udtSpectrumHeaderInfo.ParentIonCharges(0)) Then
                                        dblSpectrumQualityScore = mMinimumQualityScore + 1
                                    Else
                                        dblSpectrumQualityScore = mMinimumQualityScore - 1
                                    End If
                                Case Else
                                    ' Includes eSpectrumFilterMode.mode1
                                    dblSpectrumQualityScore = CDbl(EvaluateMsMsSpectrum(sngMassList, sngIntensityList))
                            End Select


                            If mEvaluateSpectrumQualityOnly Then
                                'Just send a report to the report file
                                If blnValidOutputFolder Then
                                    Report(GetReportFileName(strOutputFilePath), udtSpectrumHeaderInfo.ScanNumberStart, udtSpectrumHeaderInfo.ScanNumberEnd, udtSpectrumHeaderInfo.ParentIonCharges(0), dblSpectrumQualityScore)
                                Else
                                    Report(GetReportFileName(strInputFilePath), udtSpectrumHeaderInfo.ScanNumberStart, udtSpectrumHeaderInfo.ScanNumberEnd, udtSpectrumHeaderInfo.ParentIonCharges(0), dblSpectrumQualityScore)
                                End If


                            Else
                                If mGenerateFilterReport Then
                                    Report(GetReportFileName(strOutputFilePath), udtSpectrumHeaderInfo.ScanNumberStart, udtSpectrumHeaderInfo.ScanNumberEnd, udtSpectrumHeaderInfo.ParentIonCharges(0), dblSpectrumQualityScore)
                                End If

                                If Not mDiscardValidSpectra Then
                                    If dblSpectrumQualityScore > mMinimumQualityScore Or mEvaluateSpectrumQualityOnly = True Then
                                        ' Valid spectrum, so write to the output file

                                        If Not srOutFile Is Nothing Then
                                            strMostRecentSpectrumText = objFileReader.GetMostRecentSpectrumFileText
                                            srOutFile.Write(strMostRecentSpectrumText)

                                            If Not strMostRecentSpectrumText.EndsWith(ControlChars.NewLine & ControlChars.NewLine) Then
                                                srOutFile.WriteLine()
                                            End If
                                        End If
                                    End If
                                Else
                                    'discard the valid spectra and write to the file the invalid spectra
                                    If dblSpectrumQualityScore > mMinimumQualityScore Or mEvaluateSpectrumQualityOnly = True Then
                                        ' Valid spectrum we ignore it
                                    Else
                                        ' Invalid spectrum, so write to the output file
                                        If Not srOutFile Is Nothing Then
                                            strMostRecentSpectrumText = objFileReader.GetMostRecentSpectrumFileText
                                            srOutFile.Write(strMostRecentSpectrumText)

                                            If Not strMostRecentSpectrumText.EndsWith(ControlChars.NewLine & ControlChars.NewLine) Then
                                                srOutFile.WriteLine()
                                            End If
                                        End If
                                    End If
                                End If

                            End If

                        Else
                            ' Not a valid spectrum; discard the data (if EvaluateSpectrumQualityOnly = False)
                        End If

                        intProcessCount += 1
                        If intProcessCount Mod 100 = 0 Then
                            Console.Write(".")
                        End If
                    End If
                Catch ex As Exception

                End Try

                If mAbortProcessing Then Exit Do
            Loop While blnSpectrumFound

            If intProcessCount >= 100 Then Console.WriteLine()

            objFileReader.CloseFile()
            If Not srOutFile Is Nothing Then
                srOutFile.Close()
            End If

            blnSuccess = True

        Catch ex As Exception
            mErrorMessage = "Error in ProcessDTATextOrMGF: " & ex.Message
            SetLocalErrorCode(eFilterMsMsSpectraErrorCodes.InputFileAccessError)
            If MyBase.ShowMessages Then
                Debug.Assert(False, mErrorMessage)
            Else
                Throw New Exception(mErrorMessage, ex)
            End If
            blnSuccess = False
        End Try

        Return blnSuccess

    End Function

    ' Main processing function (utilizes EvaluateMsMsSpectrum)
    Public Overloads Overrides Function ProcessFile(ByVal strInputFilePath As String, ByVal strOutputFolderPath As String, ByVal strParameterFilePath As String, ByVal blnResetErrorCode As Boolean) As Boolean
        ' Returns True if success, False if failure
        ' This function can process .Dta, .MGF, or _Dta.txt files

        Dim ioFile As System.IO.FileInfo
        Dim ioPath As System.IO.Path

        Dim strInputFilePathFull As String

        Dim blnSuccess As Boolean

        ' Make sure the required DLLs are present in the working directory
        If Not ValidateRequiredDLLs Then
            Return False
        End If

        If blnResetErrorCode Then
            SetLocalErrorCode(eFilterMsMsSpectraErrorCodes.NoError)
        End If

        If Not LoadParameterFileSettings(strParameterFilePath) Then
            mErrorMessage = "Parameter file load error: " & strParameterFilePath
            If MyBase.ShowMessages Then
                MsgBox(mErrorMessage, MsgBoxStyle.Exclamation Or MsgBoxStyle.OKOnly, "Error")
            End If

            Console.WriteLine(mErrorMessage)
            If MyBase.ErrorCode = clsProcessFilesBaseClass.eProcessFilesErrorCodes.NoError Then
                MyBase.SetBaseClassErrorCode(clsProcessFilesBaseClass.eProcessFilesErrorCodes.InvalidParameterFile)
            End If
            Return False
        End If

        Try
            If strInputFilePath Is Nothing OrElse strInputFilePath.Length = 0 Then
                Console.WriteLine("Input file name is empty")
                MyBase.SetBaseClassErrorCode(clsProcessFilesBaseClass.eProcessFilesErrorCodes.InvalidInputFilePath)
            Else

                If Not CleanupFilePaths(strInputFilePath, strOutputFolderPath) Then
                    MyBase.SetBaseClassErrorCode(clsProcessFilesBaseClass.eProcessFilesErrorCodes.FilePathError)
                Else
                    Try

                        If Not mFilterMode2Options.SequestParamFilePath Is Nothing AndAlso mFilterMode2Options.SequestParamFilePath.Length > 0 Then
                            blnSuccess = ParseSequestParamFile(mFilterMode2Options.SequestParamFilePath)
                            If Not blnSuccess Then Exit Try
                        End If

                        ' Obtain the full path to the input file
                        ioFile = New System.IO.FileInfo(strInputFilePath)
                        strInputFilePathFull = ioFile.FullName

                        If ioPath.GetExtension(strInputFilePathFull).ToUpper = DTA_EXTENSION Then
                            blnSuccess = ProcessDtaFile(strInputFilePathFull, strOutputFolderPath)
                        ElseIf strInputFilePathFull.ToUpper.EndsWith(DTA_TXT_EXTENSION) Then
                            Console.WriteLine("Parsing " & System.IO.Path.GetFileName(strInputFilePath))
                            Console.WriteLine()
                            Console.WriteLine("Filtering MsMs Spectra: ")
                            blnSuccess = ProcessDtaTxtFile(strInputFilePathFull, strOutputFolderPath)
                        ElseIf strInputFilePathFull.ToUpper.EndsWith(MGF_EXTENSION) Then
                            Console.WriteLine("Parsing " & System.IO.Path.GetFileName(strInputFilePath))
                            Console.WriteLine()
                            Console.WriteLine("Filtering MsMs Spectra: ")
                            blnSuccess = ProcessMascotGenericFile(strInputFilePathFull, strOutputFolderPath)
                        Else
                            ' Unknown file extension
                            mErrorMessage = "Unknown file extension: " & ioPath.GetExtension(strInputFilePathFull)
                            SetLocalErrorCode(eFilterMsMsSpectraErrorCodes.UnknownFileExtension)
                            blnSuccess = False
                        End If

                        If Not blnSuccess Then
                            SetLocalErrorCode(eFilterMsMsSpectraErrorCodes.InputFileAccessError, True)
                        End If

                        If mAutoCloseReportFile Then CloseReportFile()

                    Catch ex As Exception
                        mErrorMessage = "Error calling ProcessDtaFile or ProcessDtaTxtFile: " & ex.message
                        If MyBase.ShowMessages Then
                            MsgBox(mErrorMessage, MsgBoxStyle.Exclamation Or MsgBoxStyle.OKOnly, "Error")
                        Else
                            Throw New System.Exception(mErrorMessage, ex)
                        End If
                    End Try
                End If
            End If
        Catch ex As Exception
            mErrorMessage = "Error in ProcessFile: " & ex.Message
            If MyBase.ShowMessages Then
                MsgBox(mErrorMessage, MsgBoxStyle.Exclamation Or MsgBoxStyle.OKOnly, "Error")
            Else
                Throw New System.Exception(mErrorMessage, ex)
            End If
        End Try

        Return blnSuccess

    End Function

    Private Sub Report(ByVal strReportFileName As String, ByVal ScanNumberStart As Integer, ByVal ScanNumberEnd As Integer, ByVal Charge As Integer, ByVal QualityScore As Double)
        Dim blnWriteHeaders As Boolean

        If Not (mCurrentReportFileName Is Nothing) AndAlso mCurrentReportFileName <> strReportFileName Then
            mOverwriteReportFile = True
            'sorting the previous report file
            SortSpectrumQualityTextFile()
        End If

        If mOverwriteReportFile Then
            mOverwriteReportFile = False
            If System.IO.File.Exists(strReportFileName) Then
                'deleting old report file to create a new one
                System.IO.File.Delete(strReportFileName)
            End If
        End If

        ' Check if strReportFileName points to an existing file
        blnWriteHeaders = Not System.IO.File.Exists(strReportFileName)

        If swReportFile Is Nothing OrElse mReportFilePath <> strReportFileName Then
            CloseReportFile()
            swReportFile = New IO.StreamWriter(strReportFileName, True)
            mReportFilePath = String.Copy(strReportFileName)
            mCurrentReportFileName = String.Copy(strReportFileName)
        End If

        If blnWriteHeaders Then
            ' Use the following to write out the data to the new output file
            swReportFile.WriteLine("Scan Number Start" & ControlChars.Tab & "Scan Number End" & ControlChars.Tab & "Charge" & ControlChars.Tab & "Quality Score")
        End If

        swReportFile.WriteLine(ScanNumberStart.ToString & ControlChars.Tab & _
                               ScanNumberEnd.ToString & ControlChars.Tab & _
                               Charge.ToString & ControlChars.Tab & _
                               QualityScore.ToString)


    End Sub

    Private Sub SetLocalErrorCode(ByVal eNewErrorCode As eFilterMsMsSpectraErrorCodes)
        SetLocalErrorCode(eNewErrorCode, False)
    End Sub

    Private Sub SetLocalErrorCode(ByVal eNewErrorCode As eFilterMsMsSpectraErrorCodes, ByVal blnLeaveExistingErrorCodeUnchanged As Boolean)

        If blnLeaveExistingErrorCodeUnchanged AndAlso mLocalErrorCode <> eFilterMsMsSpectraErrorCodes.NoError Then
            ' An error code is already defined; do not change it
        Else
            mLocalErrorCode = eNewErrorCode

            If eNewErrorCode = eFilterMsMsSpectraErrorCodes.NoError Then
                If MyBase.ErrorCode = clsProcessFilesBaseClass.eProcessFilesErrorCodes.LocalizedError Then
                    MyBase.SetBaseClassErrorCode(clsProcessFilesBaseClass.eProcessFilesErrorCodes.NoError)
                End If
            Else
                MyBase.SetBaseClassErrorCode(clsProcessFilesBaseClass.eProcessFilesErrorCodes.LocalizedError)
            End If
        End If

    End Sub

    Public Sub SortSpectrumQualityTextFile()

        Const DIM_CHUNK_SIZE As Integer = 1000

        Dim intLinesRead As Integer

        Dim intEntryCount As Integer
        Dim udtSpectrumQualityList() As udtSpectrumQualityEntryType

        Dim srInFile As System.IO.StreamReader
        Dim swOutFile As System.io.StreamWriter

        Dim strHeaderLine As String

        Dim intIndex As Integer
        Dim strLineIn As String
        Dim strSplitLine As String()

        ReDim udtSpectrumQualityList(DIM_CHUNK_SIZE - 1)

        Try
            ' Make sure the report file is closed
            CloseReportFile()

            ' Do not continue if mCurrentReportFileName is nothing
            If mCurrentReportFileName Is Nothing OrElse mCurrentReportFileName.Length = 0 Then
                Exit Sub
            End If

            ' Open the file and read in the lines
            srInFile = New System.IO.StreamReader(mCurrentReportFileName)

            intEntryCount = 0
            intLinesRead = 0
            While srInFile.Peek <> -1
                strLineIn = srInFile.ReadLine
                intLinesRead += 1

                If intLinesRead = 1 Then
                    strHeaderLine = String.Copy(strLineIn)
                Else
                    strSplitLine = strLineIn.Split(ControlChars.Tab)

                    If intEntryCount >= udtSpectrumQualityList.Length Then
                        ReDim Preserve udtSpectrumQualityList(udtSpectrumQualityList.Length + DIM_CHUNK_SIZE - 1)
                    End If

                    If strSplitLine.Length >= 4 Then
                        With udtSpectrumQualityList(intEntryCount)
                            .ScanNumberStart = CInt(strSplitLine(0))
                            .ScanNumberEnd = CInt(strSplitLine(1))
                            .Charge = CInt(strSplitLine(2))
                            .Score = CInt(strSplitLine(3))
                        End With

                        intEntryCount += 1
                    End If
                End If

            End While

            ' Close the file
            srInFile.Close()

            If intEntryCount > 0 Then

                ' Sort the data
                Dim iQualityListComparerClass As New SpectrumQualityListComparerClass
                Array.Sort(udtSpectrumQualityList, 0, intEntryCount - 1, iQualityListComparerClass)
                iQualityListComparerClass = Nothing

                ' Overwrite the file with the sorted values
                swOutFile = New IO.StreamWriter(mCurrentReportFileName, False)

                swOutFile.WriteLine(strHeaderLine)
                For intIndex = 0 To intEntryCount - 2
                    swOutFile.WriteLine(udtSpectrumQualityList(intIndex).ScanNumberStart.ToString & ControlChars.Tab & _
                                            udtSpectrumQualityList(intIndex).ScanNumberEnd.ToString & ControlChars.Tab & _
                                            udtSpectrumQualityList(intIndex).Charge.ToString & ControlChars.Tab & _
                                            udtSpectrumQualityList(intIndex).Score.ToString)
                Next intIndex
                swOutFile.Close()
            End If

        Catch ex As Exception
            If MyBase.ShowMessages Then
                Debug.Assert(False, "Error in SortSpectrumQualityTextFile: " & ex.Message)
            End If
        End Try


    End Sub

    Public Sub UpdateAminoAcidMass(ByVal strAminoAcidSymbolOneLetter As String, ByVal sngNewMass As Single)

        Try
            strAminoAcidSymbolOneLetter = strAminoAcidSymbolOneLetter.ToUpper
            If mAminoAcidMassList.ContainsKey(strAminoAcidSymbolOneLetter) Then
                mAminoAcidMassList(strAminoAcidSymbolOneLetter) = sngNewMass
            End If
        Catch ex As Exception
            If MyBase.ShowMessages Then
                Debug.Assert(False, "Error in UpdateAminoAcidMass: " & ex.Message)
            End If
        End Try

    End Sub

    Private Function ValidateOutputFolder(ByRef strInputFilePath As String, ByRef strOutputFolderPath As String) As Boolean
        ' Returns True if strOutputFolderPath points to a valid folder
        ' A valid folder does not equal strInputFilePath's folder
        '
        ' If an error occurs, then sets the Base Class error code to eProcessFilesErrorCodes.InvalidOutputFolderPath

        Dim ioPath As System.IO.Path
        Dim ioFolder As System.IO.Directory

        Dim blnValidOutputFolder As Boolean

        Try
            ' Check if strOutputFolderPath is defined and is not equal to strInputFilePath's folder
            blnValidOutputFolder = False
            If Not strOutputFolderPath Is Nothing AndAlso strOutputFolderPath.Length > 0 Then
                strOutputFolderPath = ioPath.GetFullPath(strOutputFolderPath)
                If ioFolder.Exists(strOutputFolderPath) Then
                    If strOutputFolderPath.ToLower <> ioPath.GetDirectoryName(strInputFilePath).ToLower Then
                        blnValidOutputFolder = True
                    End If
                End If
            End If

        Catch ex As Exception
            blnValidOutputFolder = False
            MyBase.SetBaseClassErrorCode(clsProcessFilesBaseClass.eProcessFilesErrorCodes.InvalidOutputFolderPath)
        End Try

        Return blnValidOutputFolder

    End Function

    Private Function ValidateRequiredDLLs() As Boolean

        Dim strRequiredDLLs() As String = New String() {"MsMsDataFileReader.dll", "PRISM.dll"}

        Dim intIndex As Integer
        Dim strFilePath As String
        Dim strCurrentFolderPath As String

        ' Make sure the required DLLs are present

        For intIndex = 0 To strRequiredDLLs.Length - 1
            strCurrentFolderPath = System.IO.Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location)
            strFilePath = System.IO.Path.Combine(strCurrentFolderPath, strRequiredDLLs(intIndex))
            If Not System.IO.File.Exists(strFilePath) Then
                mErrorMessage = "DLL not found: " & strFilePath
                SetLocalErrorCode(eFilterMsMsSpectraErrorCodes.MissingRequiredDLL)
                If MyBase.ShowMessages Then
                    Debug.Assert(False, mErrorMessage)
                End If
                Return False
            End If
        Next intIndex

        Return True

    End Function

    Protected Overrides Sub Finalize()
        CloseReportFile()
        MyBase.Finalize()
    End Sub

    ' IComparer class to allow comparison of entries in a spectrum quality list text file
    Private Class SpectrumQualityListComparerClass
        Implements IComparer

        Public Function Compare(ByVal x As Object, ByVal y As Object) As Integer Implements System.Collections.IComparer.Compare

            Dim udtEntry1, udtEntry2 As udtSpectrumQualityEntryType

            udtEntry1 = CType(x, udtSpectrumQualityEntryType)
            udtEntry2 = CType(y, udtSpectrumQualityEntryType)

            If udtEntry1.ScanNumberStart > udtEntry2.ScanNumberStart Then
                Return 1
            ElseIf udtEntry1.ScanNumberStart < udtEntry2.ScanNumberStart Then
                Return -1
            Else
                If udtEntry1.ScanNumberEnd > udtEntry2.ScanNumberEnd Then
                    Return 1
                ElseIf udtEntry1.ScanNumberEnd < udtEntry2.ScanNumberEnd Then
                    Return -1
                Else
                    If udtEntry1.Charge > udtEntry2.Charge Then
                        Return 1
                    ElseIf udtEntry1.Charge < udtEntry2.Charge Then
                        Return -1
                    Else
                        Return 0
                    End If
                End If
            End If

        End Function
    End Class

End Class
