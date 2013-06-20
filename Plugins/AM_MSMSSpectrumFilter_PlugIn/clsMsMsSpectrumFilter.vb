Option Strict On

' This class can be used to evaluate an MS/MS spectrum to see if it passes the given filters
' The spectrum data can be passed directly to the EvaluateMsMsSpectrum() function
' Or, this program can parse an entire _Dta.txt file, creating a new _Dta.txt file and only including those spectra that pass the filters
' Lastly, an entire folder of .Dta files can be parsed; those that fail the filter are renamed to .Bad or not copied to the output folder
'
' Written by Matthew Monroe for the Department of Energy (PNNL, Richland, WA)
' Copyright 2005, Battelle Memorial Institute.  All Rights Reserved.
' Started November 13, 2003

Public Class clsMsMsSpectrumFilter
    Inherits clsProcessFilesBaseClass


    Public Sub New()
        MyBase.mFileDate = "November 22, 2010"
        InitializeVariables()
    End Sub

#Region "Constants and Enums"
    Protected Const FINNIGAN_DATAFILE_INFO_SCANNER As String = "Finnigan_Datafile_Info_Scanner.exe"

    Protected Const SCAN_STATS_EX_COL_COLLISION_MODE As String = "Collision Mode"

    Protected Const TRACE_LOG_ENABLED As Boolean = False

    Protected Const DTA_EXTENSION As String = ".DTA"
    Protected Const DTA_TXT_EXTENSION As String = "_DTA.TXT"
    Protected Const FHT_TXT_EXTENSION As String = "_FHT.TXT"
    Protected Const MGF_EXTENSION As String = ".MGF"

    Public Const MASS_PHOSPHORYLATION As Double = 97.9768968        ' H3PO4
    Public Const MASS_WATER As Double = 18.0105642                  ' H2O, 15.9949141 + 2*1.0078246
    Public Const MASS_PROTON As Double = 1.00727649               ' Note that this is the mass of hydrogen minus the mass of one electron

    Public Const DEFAULT_STANDARDMASSSPACING_1PLUS_MINIMUM As Integer = 2        ' 2
    Public Const DEFAULT_STANDARDMASSSPACING_1PLUS_MAXIMUM As Integer = 12       ' 4

    Public Const DEFAULT_STANDARDMASSSPACING_2PLUS_MINIMUM As Integer = 3        ' 3
    Public Const DEFAULT_STANDARDMASSSPACING_2PLUS_MAXIMUM As Integer = 20       ' 7

    Public Const DEFAULT_STANDARDMASSSPACING_3PLUS_MINIMUM As Integer = 5        ' 5
    Public Const DEFAULT_STANDARDMASSSPACING_3PLUS_MAXIMUM As Integer = 38       ' 13

    Public Const DEFAULT_STANDARDMASSSPACING_4PLUS_MINIMUM As Integer = 7        ' 7
    Public Const DEFAULT_STANDARDMASSSPACING_4PLUS_MAXIMUM As Integer = 50       ' 17

    Public Const DEFAULT_IONFILTER_PRECURSOR_TOLERANCE_MZ As Double = 3.1
    Public Const DEFAULT_IONFILTER_PRECURSOR_NL_CLEANING_WINDOW_DA As Double = 60

    Public Const DEFAULT_MODE4_REPORTER_ION_MZs As String = "204.0871934, 300.130787, 503.2101566"
    Public Const DEFAULT_MODE4_PARENT_ION_MASS_LOSS_DA As Single = 502.2023

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
        mode2 = 2               ' Filter based on Spequal, by Sam Purvine
        mode3 = 3               ' Phosphorylation Neutral Loss filter
        mode4 = 4               ' Look for reporter ions at specific m/z values
    End Enum

    Public Enum FilterMode1Options
        MinimumStandardMassSpacingIonPairs = 0          ' Note: MinimumStandardMassSpacingIonPairs is no longer valid; use GetFilterMode1MassSpacingOption instead
        IonPairMassToleranceHalfWidthDa = 1
        NoiseLevelIntensityThreshold = 2
        DataPointCountToConsider = 3
        SignalToNoiseThreshold = 4
        EnableSegmentedSignalToNoise = 5
        SegmentedSignalToNoiseMZWidth = 6
        UseLogIntensity = 7
    End Enum

    Public Enum FilterMode1MassSpacingOption
        MinimumStandardMassSpacingCount = 0
        MaximumStandardMassSpacingCount = 1
    End Enum

    Public Enum FilterMode2Options
        SignificantIntensityFractionBasePeak = 0
        NoiseThresholdFraction = 1
        Charge1SignificantPeakNumberThreshold = 2
        Charge2SignificantPeakNumberThreshold = 3
        TICScoreThreshold = 4
        HighQualitySNThreshold = 5
        HighQualitySNPeakCount = 6
        ModerateQualitySNThreshold = 7
        ModerateQualitySNPeakCount = 8
        LowQualitySNThreshold = 9
        LowQualitySNPeakCount = 10
        ComputeFractionalScores = 11
    End Enum

    Public Enum FilterMode3Options
        BasePeakIntensityMinimum = 0
        MassToleranceHalfWidthMZ = 1
        NLAbundanceThresholdFractionMax = 2
    End Enum

    ' The ScanStats file has additional columns, but these are the only ones we care about
    Protected Const SCANSTATS_COL_COUNT As Integer = 3
    Protected Enum eScanStatsColumns
        ScanNumber
        ScanType            ' aka MSLevel
        ScanTypeName
    End Enum

    Protected Const SCANSTATS_EX_COL_COUNT As Integer = 9
    Protected Enum eScanStatsExColumns
        Dataset
        ScanNumber
        IonInjectionTime
        ScanSegment
        ScanEvent
        ChargeState
        MonoisotopicMZ
        CollisionMode
        ScanFilterText
    End Enum

    ' Column names in the ScanStats file that we need to read; other columns are ignored
    'Protected Const SCANSTATS_COL_DATASET As String = "Dataset"
    Protected Const SCANSTATS_COL_SCAN_NUM As String = "ScanNumber"
    Protected Const SCANSTATS_COL_SCAN_TYPE As String = "ScanType"
	Public Const SCANSTATS_COL_SCAN_TYPE_NAME As String = "ScanTypeName"

    ' Column names in the ScanStatsEx file that we need to read; other columns are ignored
    Protected Const SCANSTATS_COL_ION_INJECTION_TIME As String = "Ion Injection Time (ms)"
    Protected Const SCANSTATS_COL_SCAN_SEGMENT As String = "Scan Segment"
    Protected Const SCANSTATS_COL_SCAN_EVENT As String = "Scan Event"
    Protected Const SCANSTATS_COL_CHARGE_STATE As String = "Charge State"
    Protected Const SCANSTATS_COL_MONOISOTOPIC_MZ As String = "Monoisotopic M/Z"
    Protected Const SCANSTATS_COL_COLLISION_MODE As String = "Collision Mode"
    Protected Const SCANSTATS_COL_SCAN_FILTER_TEXT As String = "Scan Filter Text"

    Public Const TEXT_MATCH_TYPE_CONTAINS As String = "Contains"
    Public Const TEXT_MATCH_TYPE_EXACT As String = "Exact"
    Public Const TEXT_MATCH_TYPE_REGEX As String = "RegEx"

    Public Enum eTextMatchTypeConstants
        Contains = 0
        Exact = 1
        RegEx = 2
    End Enum

    Protected Enum eInputFileModeConstants
        IndividualDTAs = 0
        ConcatenatedDTA = 1
        MGF = 2
    End Enum
#End Region

#Region "Structures"

    Protected Structure FilterMode1MassSpacingSettingsType
        Public Minimum As Integer
        Public Maximum As Integer
    End Structure

    Protected Structure FilterMode1OptionsType
        Public StandardMassSpacingCounts() As FilterMode1MassSpacingSettingsType       ' Ranges from 0 to 3, corresponding to 1+, 2+, 3+, and 4+ or higher
        Public IonPairMassToleranceHalfWidthDa As Single
        Public NoiseLevelIntensityThreshold As Single
        Public DataPointCountToConsider As Integer                ' Maximum number of data points to consider in each spectrum (filter by abundance); set to 0 to consider all of the data
        Public SignalToNoiseThreshold As Single
        Public EnableSegmentedSignalToNoise As Boolean
        Public SegmentedSignalToNoiseMZWidth As Integer
        Public UseLogIntensity As Boolean
        Public Sub Initialize()
            ReDim StandardMassSpacingCounts(3)
            StandardMassSpacingCounts(0).Minimum = DEFAULT_STANDARDMASSSPACING_1PLUS_MINIMUM
            StandardMassSpacingCounts(0).Maximum = DEFAULT_STANDARDMASSSPACING_1PLUS_MAXIMUM

            StandardMassSpacingCounts(1).Minimum = DEFAULT_STANDARDMASSSPACING_2PLUS_MINIMUM
            StandardMassSpacingCounts(1).Maximum = DEFAULT_STANDARDMASSSPACING_2PLUS_MAXIMUM

            StandardMassSpacingCounts(2).Minimum = DEFAULT_STANDARDMASSSPACING_3PLUS_MINIMUM
            StandardMassSpacingCounts(2).Maximum = DEFAULT_STANDARDMASSSPACING_3PLUS_MAXIMUM

            StandardMassSpacingCounts(3).Minimum = DEFAULT_STANDARDMASSSPACING_4PLUS_MINIMUM
            StandardMassSpacingCounts(3).Maximum = DEFAULT_STANDARDMASSSPACING_4PLUS_MAXIMUM
        End Sub
    End Structure

    Protected Structure FilterMode2OptionsType
        Public SignificantIntensityFractionBasePeak As Single
        Public NoiseThresholdFraction As Single
        Public Charge1SignificantPeakNumberThreshold As Integer
        Public Charge2SignificantPeakNumberThreshold As Integer
        Public TICScoreThreshold As Single
        Public HighQualitySNThreshold As Single
        Public HighQualitySNPeakCount As Integer
        Public ModerateQualitySNThreshold As Single
        Public ModerateQualitySNPeakCount As Integer
        Public LowQualitySNThreshold As Single
        Public LowQualitySNPeakCount As Integer
        Public ComputeFractionalScores As Boolean
        Public SequestParamFilePath As String                     ' Sequest Param file to read in order to look for modified amino acid masses
    End Structure

    Protected Structure FilterMode3OptionsType
        Public BasePeakIntensityMinimum As Double               ' Minimum base peak intensity, in counts
        Public MassToleranceHalfWidthMZ As Double
        Public NLAbundanceThresholdFractionMax As Double        ' Number between 0 and 1, specifying the value to multiply the BPI by to determine minimum intensity when examining neutral loss ions
        Public LimitToChargeSpecificIons As Boolean             ' When true, then only considers the ions appropriate for the charge state associated with the spectrum
        Public ConsiderWaterLoss As Boolean                     ' When true, then looks for loss of water in addition to loss of Phosphate (98)
        Public SpecificMZLosses As String                       ' Comma separated list of specific m/z loss values to search for.  When defined (and non-zero) then only looks for parent ion losses matching these m/z values
    End Structure

    ''' <summary>
    ''' Filter mode 4 looks for the presence of 1 or more reporter ions at specific m/z values
    ''' If the minimum number of ions are present (above an intensity threhsold) then the spectrum is kept
    ''' </summary>
    ''' <remarks></remarks>
    Protected Structure FilterMode4OptionsType
        Public ReporterIonMZs As String                         ' Comma-separated list of m/z values to look for
        Public ReporterIonMinimumIntensity As Single            ' Minimum intensity (ion counts) that the ion must be present at in order to be counted when checking ReporterIonMatchCountMinimum
        Public ReporterIonToleranceHalfWidthMZ As Single        ' Search tolerance (+/- this value)
        Public RemoveReporterIons As Boolean                    ' If true, then the reporter ions are removed from the spectrum
        Public PrecursorIonMassLossDa As Single                 ' Will change the mass of the parent ion listed for each spectrum by this amount (correcting for charge, as needed).  This is needed if gas phase chemistry causes the precursor ion to lose a functional group, thereby decreasing the effective mass of the precursor
        Public ReporterIonMatchCountMinimum As Integer
        Public ReporterIonMaxIntensityThreshold As Single       ' At least one of the reporter ions must have an intensity at/above this value in order for the spectrum to pass the filter
        Public RemoveMassCorrectedPrecursorIons As Boolean      ' If true, then looks for ions at the new m/z that the precursor should have, given the precursor mass minus PrecursorIonMassLossDa; will look at the charge state of the precursor ion and at the charge one smaller than the precursor ion
        Public PrecursorIonMinimumCharge As Short
    End Structure

    Protected Structure IonFilterOptionsType
        Public RemovePrecursor As Boolean
        Public RemoveChargeReducedPrecursors As Boolean
        Public PrecursorCleaningToleranceMZ As Double
        Public RemoveNeutralLossesFromChargeReducedPrecursors As Boolean
		Public NeutralLossCleaningWindowDa As Double
		Public CondenseData As Boolean							' If True, then condenses regions where adjacent m/z values have an intensity of 0
		Public CondenseDataMaxIonCount As Integer				' If > 0, then will only keep the top CondenseDataMaxIonCount points in each mass spectrum (sorted by descending intensity)
	End Structure

    ' Scores:
    ' 1) IonPairCount
    ' 2) IonPairCountScore (computed using StandardSpacingMinimum and StandardSpacingMaximum for the given charge state)
    ' 3) PercentMassSpaceMatched
    ' 4) PercentAbundantPeaksWithMassDiffMatches
    ' 5) Max Sequence Tag Length

    Public Structure udtSpectrumQualityScoreType
        ' SpectrumQualityScore is populated differently depending on the filter mode:
        ' If FilterMode1, then equals PercentAbundantPeaksWithMassDiffMatches
        ' If FilterMode2, then equals FilterMode2Score
        ' If FilterMode3, then set to 1 if it has phosphorylation signatures, and 0 if not
        ' If FilterMode4, then set to 1 if the reporter ions are present, and 0 if not
        Public SpectrumQualityScore As Single
        Public IonPairCount As Integer
        Public IonPairCountScore As Single
        Public PercentMassSpaceMatched As Single
        Public PercentAbundantPeaksWithMassDiffMatches As Single
        Public SequenceTagLengthMax As Integer
        Public AbundantPeaksSumSquares As Double
        Public SequenceTagLongest As String
        Public FilterMode2Score As Single
        Public Sub Initialize()
            SpectrumQualityScore = 0
            IonPairCount = 0
            IonPairCountScore = 0
            PercentMassSpaceMatched = 0
            PercentAbundantPeaksWithMassDiffMatches = 0
            SequenceTagLengthMax = 0
            AbundantPeaksSumSquares = 0
            SequenceTagLongest = String.Empty
            FilterMode2Score = 0
        End Sub
    End Structure

    Protected Structure udtAminoAcidSpacingStatsType
        Public AminoAcidSymbol As Char
        Public MassDifference As Single
        Public MassDifferenceTheoretical As Single
        Public ChargeState As Integer
        Public DataPointIndexHeavy As Integer           ' The mass of the heavier member of the pair
        Public DataPointIndexLight As Integer           ' The mass of the lighter member of the pair
        Public AdjacentSpacingIndexPointer As Integer   ' -1 if no adjacent member
        Public SequenceTagLength As Integer             ' Number of contiguous amino acids that can be formed by stepping through the list of AASpacings found using AdjacentSpacingIndexPointer
        Public SequenceTagScore As Double               ' Sum of the squares of the S/N values for each peak in the pair
    End Structure

    Protected Structure udtSpectrumQualityEntryType
        Public ScanNumberStart As Integer
        Public ScanNumberEnd As Integer
        Public Charge As Integer
        Public Additional As String
    End Structure

    Protected Structure udtMode2ScoreDetailsType
        Public IntensityThresholdForSignificantPeaks As Single
        Public SignificantPeakNumberCount As Integer
        Public SignificantPeakCountIsValidForChargeState As Boolean
        Public NoiseLevel As Single
        Public TICScore As Single
        Public HighQualitySNThresholdScore As Single
        Public ModerateQualitySNThresholdScore As Single
        Public LowQualitySNThresholdScore As Single
        Public QualityScore As Single
    End Structure

    ''Protected Structure udtNeutralLossMassesType
    ''    Public NeutralLossMass As Double
    ''    Public LowerBoundMZ As Double
    ''    Public UpperBoundMZ As Double
    ''    Public SearchMassCode As SearchMassSpecsClass.eSearchMassCodeConstants
    ''End Structure

    Public Structure udtIonMatchStatsType
        Public BPI As Single
        ' Used by both Mode 3 and Mode 4
        ' For Mode 3, use SearchMassSpecsClass.eSearchMassCodeConstants to determine which index corresponds to which neutral loss value
        ' For Mode 4, the order corresponds to the order of numbers in mFiltermode4Options.ReporterIonMZs
        Public IonIntensitiesNormalized() As Single     ' Values between 0 and 100, computed via peak intensity / BPI * 100

        Public Sub Clear()
            BPI = 0
            If Not IonIntensitiesNormalized Is Nothing Then
                For intIndex As Integer = 0 To IonIntensitiesNormalized.Length - 1
                    IonIntensitiesNormalized(intIndex) = 0
                Next
            End If
        End Sub
    End Structure

    Protected Structure udtScanStatsInfoType
        Public ScanNumber As Integer
        Public MSLevel As Integer
        Public ScanTypeName As String
    End Structure

    Protected Structure udtExtendedStatsInfoType
        Public ScanNumber As Integer
        Public IonInjectionTime As String
        Public ScanSegment As String
        Public ScanEvent As String
        Public ChargeState As String            ' Only defined for LTQ-Orbitrap datasets and only for fragmentation spectra where the instrument could determine the charge and m/z
        Public MonoisotopicMZ As String         ' Only defined for LTQ-Orbitrap datasets and only for fragmentation spectra where the instrument could determine the charge and m/z
        Public CollisionMode As String
        Public ScanFilterText As String
    End Structure
#End Region

#Region "Classwide Variables"

    'filter options variables
    Private mSpectrumFilterMode As eSpectrumFilterMode
    Private mMinimumQualityScore As Single
    Private mGenerateFilterReport As Boolean
    Private mIncludeNLStatsOnFilterReport As Boolean            ' When True, then includes additional columns on the filter report (for filter modes 3 and 4)
    Private mOverwriteExistingFiles As Boolean

    Private mDiscardValidSpectra As Boolean                     ' Set to True to only keep the Invalid spectra, rather than only keeping the Valid spectra
    Private mEvaluateSpectrumQualityOnly As Boolean             ' When True, then generates a filter report but doesn't create any output files
    Private mDeleteBadDTAFiles As Boolean

    Private mMSLevelFilter As Integer                           ' If 0 then keeps all spectra; if 2 then only keeps MS2 spectra; if 3, then only keeps MS3 spectra.  Only works with Finnigan .Raw files and requires that the .Raw file be available

    Private mScanTypeFilter As String = String.Empty            ' If empty, then keeps all spectra; if defined, then only keeps spectra with this scan type (e.g. MSn, HMSn, CID-MSn, CID-HMSn, etc.)
    Private mScanTypeMatchType As eTextMatchTypeConstants       ' Affects the method used to compare mScanTypeFilter to the actual scan type

    Private mMSCollisionModeFilter As String = String.Empty     ' If empty, then keeps all spectra; if defined, then only keeps spectra with this collision mode (e.g. cid, etd, etc.)
    Private mMSCollisionModeMatchType As eTextMatchTypeConstants    ' Affects the method used to compare mMSCollisionModeFilter to the actual collision mode
	Private mMinimumIonCount As Integer

    Private mIonFilterOptions As IonFilterOptionsType

    ' Filter Mode-specific settings
    Private mFilterMode1Options As FilterMode1OptionsType
    Private mFilterMode2Options As FilterMode2OptionsType
    Private mFilterMode3Options As FilterMode3OptionsType
    Private mFilterMode4Options As FilterMode4OptionsType

    Private mAminoAcidMassList As Hashtable

    Private mLocalErrorCode As eFilterMsMsSpectraErrorCodes
    Private mErrorMessage As String

    Private swReportFile As System.IO.StreamWriter
    Private mReportFilePath As String
    Private mCurrentReportFileName As String

    Private mSettingsLoadedViaCode As Boolean                   ' Set to true to skip loading of settings from a parameter file in LoadParameterFileSettings
    Private mOverwriteReportFile As Boolean                     ' Set to true to re-create the spectrum quality report file
    Private mAutoCloseReportFile As Boolean                     ' This is typically set to false when processing .Dta files; True for other files

    Private mMaximumProgressUpdateIntervalSeconds As Single     ' Maximum time, in seconds, between calling UpdateProgress() to report the progress via Event ProgressChanged (and optionally to the log file)

    Private mScanStatsInfoLoaded As Boolean                 ' When True, then mScanStatsInfo() and mScanStatsPointer are valid
    Private mScanStatsInfo() As udtScanStatsInfoType
    Private mScanStatsPointer As Hashtable                  ' Hash table with scan number as the key and an index of the given scan in mScanStatsInfo() as the value

    Private mExtendedStatsInfoLoaded As Boolean                 ' When True, then mExtendedStatsInfo() and mExtendedStatsPointer are valid
    Private mExtendedStatsInfo() As udtExtendedStatsInfoType
    Private mExtendedStatsPointer As Hashtable                  ' Hash table with scan number as the key and an index of the given scan in mExtendedStatsInfo() as the value

    Private mDebugMode As Boolean
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

    Public Property DebugModeEnabled() As Boolean
        Get
            Return mDebugMode
        End Get
        Set(ByVal Value As Boolean)
            mDebugMode = Value
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

    Public Property IncludeNLStatsOnFilterReport() As Boolean
        Get
            Return mIncludeNLStatsOnFilterReport
        End Get
        Set(ByVal Value As Boolean)
            mIncludeNLStatsOnFilterReport = Value
        End Set
    End Property

    Public ReadOnly Property LocalErrorCode() As eFilterMsMsSpectraErrorCodes
        Get
            Return mLocalErrorCode
        End Get
    End Property

    Public Property MaximumProgressUpdateIntervalSeconds() As Single
        Get
            Return mMaximumProgressUpdateIntervalSeconds
        End Get
        Set(ByVal value As Single)
            If value < 0.2 Then value = 0.2
            mMaximumProgressUpdateIntervalSeconds = value
        End Set
    End Property
    Public Property MinimumQualityScore() As Single
        Get
            Return mMinimumQualityScore
        End Get
        Set(ByVal Value As Single)
            mMinimumQualityScore = Value
        End Set
    End Property

    Public Property MSLevelFilter() As Integer
        Get
            Return mMSLevelFilter
        End Get
        Set(ByVal Value As Integer)
            mMSLevelFilter = Value
        End Set
    End Property

    Public Property MSCollisionModeFilter() As String
        Get
            If mMSCollisionModeFilter Is Nothing Then
                Return String.Empty
            Else
                Return mMSCollisionModeFilter
            End If
        End Get
        Set(ByVal value As String)
            If Not value Is Nothing Then
                mMSCollisionModeFilter = String.Copy(value)
            End If
        End Set
    End Property

    Public Property MSCollisionModeMatchType() As String
        Get
            Return TextMatchTypeCodeToString(mMSCollisionModeMatchType)
        End Get
        Set(ByVal value As String)
            ' Value should be: Exact, Contains, or RegEx
            ' If blank, will set to "Contains"
            mMSCollisionModeMatchType = TextMatchTypeStringToCode(value)
        End Set
    End Property

    Public Property MSCollisionModeMatchTypeCode() As eTextMatchTypeConstants
        Get
            Return mMSCollisionModeMatchType
        End Get
        Set(ByVal value As eTextMatchTypeConstants)
            mMSCollisionModeMatchType = value
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

    Public Property FilterMode3_ConsiderWaterLoss() As Boolean
        Get
            Return mFilterMode3Options.ConsiderWaterLoss
        End Get
        Set(ByVal Value As Boolean)
            mFilterMode3Options.ConsiderWaterLoss = Value
        End Set
    End Property

    Public Property FilterMode3_LimitToChargeSpecificIons() As Boolean
        Get
            Return mFilterMode3Options.LimitToChargeSpecificIons
        End Get
        Set(ByVal Value As Boolean)
            mFilterMode3Options.LimitToChargeSpecificIons = Value
        End Set
    End Property

    Public Property FilterMode3_SpecificMZLosses() As String
        Get
            If Not mFilterMode3Options.SpecificMZLosses Is Nothing Then
                Return mFilterMode3Options.SpecificMZLosses
            Else
                Return String.Empty
            End If
        End Get
        Set(ByVal Value As String)
            If Not Value Is Nothing Then
                mFilterMode3Options.SpecificMZLosses = String.Copy(Value)
            Else
                mFilterMode3Options.SpecificMZLosses = String.Empty
            End If
        End Set
    End Property

    Public Property FilterMode4_ReporterIonMZs() As String
        Get
            If Not mFilterMode4Options.ReporterIonMZs Is Nothing Then
                Return mFilterMode4Options.ReporterIonMZs
            Else
                Return String.Empty
            End If
        End Get
        Set(ByVal value As String)
            If Not value Is Nothing Then
                mFilterMode4Options.ReporterIonMZs = String.Copy(value)
            Else
                mFilterMode4Options.ReporterIonMZs = String.Empty
            End If
        End Set
    End Property

    Public Property FilterMode4_ReporterIonMinimumIntensity() As Single
        Get
            Return mFilterMode4Options.ReporterIonMinimumIntensity
        End Get
        Set(ByVal value As Single)
            mFilterMode4Options.ReporterIonMinimumIntensity = value
        End Set
    End Property

    Public Property FilterMode4_ReporterIonToleranceHalfWidthMZ() As Single
        Get
            Return mFilterMode4Options.ReporterIonToleranceHalfWidthMZ
        End Get
        Set(ByVal value As Single)
            mFilterMode4Options.ReporterIonToleranceHalfWidthMZ = value
        End Set
    End Property

    Public Property FilterMode4_RemoveReporterIons() As Boolean
        Get
            Return mFilterMode4Options.RemoveReporterIons
        End Get
        Set(ByVal value As Boolean)
            mFilterMode4Options.RemoveReporterIons = value
        End Set
    End Property

    Public Property FilterMode4_PrecursorIonMassLossDa() As Single
        Get
            Return mFilterMode4Options.PrecursorIonMassLossDa
        End Get
        Set(ByVal value As Single)
            mFilterMode4Options.PrecursorIonMassLossDa = value
        End Set
    End Property

    Public Property FilterMode4_ReporterIonMatchCountMinimum() As Integer
        Get
            Return mFilterMode4Options.ReporterIonMatchCountMinimum
        End Get
        Set(ByVal value As Integer)
            mFilterMode4Options.ReporterIonMatchCountMinimum = value
        End Set
    End Property

    Public Property FilterMode4_ReporterIonMaxIntensityThreshold() As Single
        Get
            Return mFilterMode4Options.ReporterIonMaxIntensityThreshold
        End Get
        Set(ByVal value As Single)
            mFilterMode4Options.ReporterIonMaxIntensityThreshold = value
        End Set
    End Property

    Public Property FilterMode4_RemoveMassCorrectedPrecursorIons() As Boolean
        Get
            Return mFilterMode4Options.RemoveMassCorrectedPrecursorIons
        End Get
        Set(ByVal value As Boolean)
            mFilterMode4Options.RemoveMassCorrectedPrecursorIons = value
        End Set
    End Property

    Public Property FilterMode4_PrecursorIonMinimumCharge() As Short
        Get
            Return mFilterMode4Options.PrecursorIonMinimumCharge
        End Get
        Set(ByVal value As Short)
            mFilterMode4Options.PrecursorIonMinimumCharge = value
        End Set
    End Property

    Public Property IonFilter_RemovePrecursor() As Boolean
        Get
            Return mIonFilterOptions.RemovePrecursor
        End Get
        Set(ByVal value As Boolean)
            mIonFilterOptions.RemovePrecursor = value
        End Set
    End Property

    Public Property IonFilter_RemoveChargeReducedPrecursors() As Boolean
        Get
            Return mIonFilterOptions.RemoveChargeReducedPrecursors
        End Get
        Set(ByVal value As Boolean)
            mIonFilterOptions.RemoveChargeReducedPrecursors = value
        End Set
    End Property

    Public Property IonFilter_RemoveNeutralLossesFromChargeReducedPrecursors() As Boolean
        Get
            Return mIonFilterOptions.RemoveNeutralLossesFromChargeReducedPrecursors
        End Get
        Set(ByVal value As Boolean)
            mIonFilterOptions.RemoveNeutralLossesFromChargeReducedPrecursors = value
        End Set
    End Property

    Public Property IonFilter_PrecursorCleaningToleranceMZ() As Double
        Get
            Return mIonFilterOptions.PrecursorCleaningToleranceMZ
        End Get
        Set(ByVal value As Double)
            mIonFilterOptions.PrecursorCleaningToleranceMZ = value
        End Set
    End Property

    Public Property IonFilter_NeutralLossCleaningWindowDa() As Double
        Get
            Return mIonFilterOptions.NeutralLossCleaningWindowDa
        End Get
        Set(ByVal value As Double)
            mIonFilterOptions.NeutralLossCleaningWindowDa = value
        End Set
    End Property

    Public Property IonFilter_CondenseData() As Boolean
        Get
            Return mIonFilterOptions.CondenseData
        End Get
        Set(ByVal value As Boolean)
            mIonFilterOptions.CondenseData = value
        End Set
    End Property

    Public Property IonFilter_CondenseDataMaxIonCount() As Integer
        Get
            Return mIonFilterOptions.CondenseDataMaxIonCount
        End Get
        Set(ByVal value As Integer)
			mIonFilterOptions.CondenseDataMaxIonCount = value
        End Set
    End Property

	Public Property MinimumIonCount() As Integer
		Get
			Return mMinimumIonCount
		End Get
		Set(value As Integer)
			mMinimumIonCount = value
		End Set
	End Property

	Public Function GetFilterMode1MassSpacingOption(ByVal intCharge As Integer, ByVal SwitchName As FilterMode1MassSpacingOption) As Integer
		Try
			Dim intChargeIndex As Integer = intCharge - 1

			If intChargeIndex >= 0 And intChargeIndex < mFilterMode1Options.StandardMassSpacingCounts.Length Then
				Select Case SwitchName
					Case FilterMode1MassSpacingOption.MinimumStandardMassSpacingCount
						Return mFilterMode1Options.StandardMassSpacingCounts(intChargeIndex).Minimum
					Case FilterMode1MassSpacingOption.MaximumStandardMassSpacingCount
						Return mFilterMode1Options.StandardMassSpacingCounts(intChargeIndex).Maximum
					Case Else
						Return -1
				End Select
			Else
				Return -1
			End If
		Catch ex As Exception
			Return -1
		End Try
	End Function

	Public Function GetFilterMode1Option(ByVal SwitchName As FilterMode1Options) As Single
		Select Case SwitchName
			Case FilterMode1Options.MinimumStandardMassSpacingIonPairs
				' This option is no longer valid; use GetFilterMode1MassSpacingOption instead
				Return -1
			Case FilterMode1Options.IonPairMassToleranceHalfWidthDa
				Return mFilterMode1Options.IonPairMassToleranceHalfWidthDa
			Case FilterMode1Options.NoiseLevelIntensityThreshold
				Return mFilterMode1Options.NoiseLevelIntensityThreshold
			Case FilterMode1Options.DataPointCountToConsider
				Return mFilterMode1Options.DataPointCountToConsider
			Case FilterMode1Options.SignalToNoiseThreshold
				Return mFilterMode1Options.SignalToNoiseThreshold
			Case FilterMode1Options.EnableSegmentedSignalToNoise
				Return FilterModeOptionBoolToSng(mFilterMode1Options.EnableSegmentedSignalToNoise)
			Case FilterMode1Options.SegmentedSignalToNoiseMZWidth
				Return mFilterMode1Options.SegmentedSignalToNoiseMZWidth
			Case FilterMode1Options.UseLogIntensity
				Return FilterModeOptionBoolToSng(mFilterMode1Options.UseLogIntensity)
		End Select
	End Function

	Public Function GetFilterMode2Option(ByVal SwitchName As FilterMode2Options) As Single
		Select Case SwitchName
			Case FilterMode2Options.SignificantIntensityFractionBasePeak
				Return mFilterMode2Options.SignificantIntensityFractionBasePeak
			Case FilterMode2Options.NoiseThresholdFraction
				Return mFilterMode2Options.NoiseThresholdFraction
			Case FilterMode2Options.Charge1SignificantPeakNumberThreshold
				Return mFilterMode2Options.Charge1SignificantPeakNumberThreshold
			Case FilterMode2Options.Charge2SignificantPeakNumberThreshold
				Return mFilterMode2Options.Charge2SignificantPeakNumberThreshold
			Case FilterMode2Options.TICScoreThreshold
				Return mFilterMode2Options.TICScoreThreshold
			Case FilterMode2Options.HighQualitySNThreshold
				Return mFilterMode2Options.HighQualitySNThreshold
			Case FilterMode2Options.HighQualitySNPeakCount
				Return mFilterMode2Options.HighQualitySNPeakCount
			Case FilterMode2Options.ModerateQualitySNThreshold
				Return mFilterMode2Options.ModerateQualitySNThreshold
			Case FilterMode2Options.ModerateQualitySNPeakCount
				Return mFilterMode2Options.ModerateQualitySNPeakCount
			Case FilterMode2Options.LowQualitySNThreshold
				Return mFilterMode2Options.LowQualitySNThreshold
			Case FilterMode2Options.LowQualitySNPeakCount
				Return mFilterMode2Options.LowQualitySNPeakCount
			Case FilterMode2Options.ComputeFractionalScores
				Return FilterModeOptionBoolToSng(mFilterMode2Options.ComputeFractionalScores)
		End Select
	End Function

	Public Function GetFilterMode3Option(ByVal SwitchName As FilterMode3Options) As Double
		Select Case SwitchName
			Case FilterMode3Options.BasePeakIntensityMinimum
				Return mFilterMode3Options.BasePeakIntensityMinimum
			Case FilterMode3Options.MassToleranceHalfWidthMZ
				Return mFilterMode3Options.MassToleranceHalfWidthMZ
			Case FilterMode3Options.NLAbundanceThresholdFractionMax
				Return mFilterMode3Options.NLAbundanceThresholdFractionMax
		End Select
	End Function

	Public Sub SetFilterMode1MassSpacingOption(ByVal intCharge As Integer, ByVal SwitchName As FilterMode1MassSpacingOption, ByVal Value As Integer)
		Dim intChargeIndex As Integer = intCharge - 1

		If intChargeIndex >= 0 And intChargeIndex < mFilterMode1Options.StandardMassSpacingCounts.Length Then
			Select Case SwitchName
				Case FilterMode1MassSpacingOption.MinimumStandardMassSpacingCount
					mFilterMode1Options.StandardMassSpacingCounts(intChargeIndex).Minimum = Value
				Case FilterMode1MassSpacingOption.MaximumStandardMassSpacingCount
					mFilterMode1Options.StandardMassSpacingCounts(intChargeIndex).Maximum = Value
				Case Else
					' Ignore the setting
			End Select
		End If
	End Sub

	Public Sub SetFilterMode1Option(ByVal SwitchName As FilterMode1Options, ByVal Value As Single)
		Select Case SwitchName
			Case FilterMode1Options.MinimumStandardMassSpacingIonPairs
				' This option is no longer valid; use SetFilterMode1MassSpacingOption instead
			Case FilterMode1Options.IonPairMassToleranceHalfWidthDa
				mFilterMode1Options.IonPairMassToleranceHalfWidthDa = Value
			Case FilterMode1Options.NoiseLevelIntensityThreshold
				mFilterMode1Options.NoiseLevelIntensityThreshold = Value
			Case FilterMode1Options.DataPointCountToConsider
				mFilterMode1Options.DataPointCountToConsider = CInt(Value)
			Case FilterMode1Options.SignalToNoiseThreshold
				mFilterMode1Options.SignalToNoiseThreshold = Value
			Case FilterMode1Options.EnableSegmentedSignalToNoise
				mFilterMode1Options.EnableSegmentedSignalToNoise = FilterModeOptionSngToBool(Value)
			Case FilterMode1Options.SegmentedSignalToNoiseMZWidth
				mFilterMode1Options.SegmentedSignalToNoiseMZWidth = CInt(Value)
			Case FilterMode1Options.UseLogIntensity
				mFilterMode1Options.UseLogIntensity = FilterModeOptionSngToBool(Value)
		End Select
	End Sub

	Public Property ScanTypeFilter() As String
		Get
			If mScanTypeFilter Is Nothing Then
				Return String.Empty
			Else
				Return mScanTypeFilter
			End If
		End Get
		Set(ByVal value As String)
			If Not value Is Nothing Then
				mScanTypeFilter = String.Copy(value)
			End If
		End Set
	End Property

	Public Property ScanTypeMatchType() As String
		Get
			Return TextMatchTypeCodeToString(mScanTypeMatchType)
		End Get
		Set(ByVal value As String)
			' Value should be: Exact, Contains, or RegEx
			' If blank, will set to "Contains"
			mScanTypeMatchType = TextMatchTypeStringToCode(value)
		End Set
	End Property

	Public Property ScanTypeMatchTypeCode() As eTextMatchTypeConstants
		Get
			Return mScanTypeMatchType
		End Get
		Set(ByVal value As eTextMatchTypeConstants)
			mScanTypeMatchType = value
		End Set
	End Property


	Public Sub SetFilterMode2Option(ByVal SwitchName As FilterMode2Options, ByVal Value As Single)
		Select Case SwitchName
			Case FilterMode2Options.SignificantIntensityFractionBasePeak
				mFilterMode2Options.SignificantIntensityFractionBasePeak = Value
			Case FilterMode2Options.NoiseThresholdFraction
				mFilterMode2Options.NoiseThresholdFraction = Value
			Case FilterMode2Options.Charge1SignificantPeakNumberThreshold
				mFilterMode2Options.Charge1SignificantPeakNumberThreshold = CInt(Value)
			Case FilterMode2Options.Charge2SignificantPeakNumberThreshold
				mFilterMode2Options.Charge2SignificantPeakNumberThreshold = CInt(Value)
			Case FilterMode2Options.TICScoreThreshold
				mFilterMode2Options.TICScoreThreshold = Value
			Case FilterMode2Options.HighQualitySNThreshold
				mFilterMode2Options.HighQualitySNThreshold = Value
			Case FilterMode2Options.HighQualitySNPeakCount
				mFilterMode2Options.HighQualitySNPeakCount = CInt(Value)
			Case FilterMode2Options.ModerateQualitySNThreshold
				mFilterMode2Options.ModerateQualitySNThreshold = Value
			Case FilterMode2Options.ModerateQualitySNPeakCount
				mFilterMode2Options.ModerateQualitySNPeakCount = CInt(Value)
			Case FilterMode2Options.LowQualitySNThreshold
				mFilterMode2Options.LowQualitySNThreshold = Value
			Case FilterMode2Options.LowQualitySNPeakCount
				mFilterMode2Options.LowQualitySNPeakCount = CInt(Value)
			Case FilterMode2Options.ComputeFractionalScores
				mFilterMode2Options.ComputeFractionalScores = FilterModeOptionSngToBool(Value)
		End Select
	End Sub

	Public Sub SetFilterMode3Option(ByVal SwitchName As FilterMode3Options, ByVal Value As Double)
		Select Case SwitchName
			Case FilterMode3Options.BasePeakIntensityMinimum
				mFilterMode3Options.BasePeakIntensityMinimum = Value
			Case FilterMode3Options.MassToleranceHalfWidthMZ
				mFilterMode3Options.MassToleranceHalfWidthMZ = Value
			Case FilterMode3Options.NLAbundanceThresholdFractionMax
				mFilterMode3Options.NLAbundanceThresholdFractionMax = Value
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

	Private Sub AppendToSearchMZList(ByRef objSearchMassSpecs As System.Collections.Generic.List(Of SearchMassSpecsClass), _
									 ByVal dblNewMass As Double, _
									 ByVal eSearchMassCode As SearchMassSpecsClass.eSearchMassCodeConstants)

		Const DEFAULT_TOLERANCE_HALFWIDTH_MZ As Double = 0.5

		AppendToSearchMZList(objSearchMassSpecs, dblNewMass, DEFAULT_TOLERANCE_HALFWIDTH_MZ, eSearchMassCode)
	End Sub

	Private Sub AppendToSearchMZList(ByRef objSearchMassSpecs As System.Collections.Generic.List(Of SearchMassSpecsClass), _
									 ByVal dblNewMass As Double, _
									 ByVal dblSearchToleranceHalfWidthMZ As Double, _
									 ByVal eSearchMassCode As SearchMassSpecsClass.eSearchMassCodeConstants)

		Dim objNewEntry As New SearchMassSpecsClass

		With objNewEntry
			.SearchMass = dblNewMass
			.SearchMassCode = eSearchMassCode
			.LowerBoundMZ = .SearchMass - dblSearchToleranceHalfWidthMZ
			.UpperBoundMZ = .SearchMass + dblSearchToleranceHalfWidthMZ
			.Matched = False
			.IonIntensity = 0
		End With

		objSearchMassSpecs.Add(objNewEntry)

	End Sub

	Private Sub AppendReportLine(ByVal strReportFileName As String, _
								 ByVal ScanNumberStart As Integer, _
								 ByVal ScanNumberEnd As Integer, _
								 ByVal Charge As Integer, _
								 ByVal udtSpectrumQualityScore As udtSpectrumQualityScoreType, _
								 ByVal sngPrecursorMZ As Single, _
								 ByVal sngBPI As Single, _
								 ByVal blnIncludeNLStats As Boolean, _
								 ByVal blnIncludeReporterIonStats As Boolean, _
								 ByRef udtIonMatchStats As udtIonMatchStatsType, _
								 ByVal blnIonFilteringEnabled As Boolean, _
								 ByVal intPositiveDataCountBeforeFilter As Integer, _
								 ByVal intPositiveDataCountAfterFilter As Integer)

		' This needs to be static because it is only updated when we write the headers (or if it is zero and blnIncludeReporterIonStats = True)
		Static intReporterIonMZCount As Integer = 0

		Dim chTab As Char = ControlChars.Tab

		Dim blnWriteHeaders As Boolean
		Dim strLineOut As String

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
			strLineOut = "Scan_Number_Start" & chTab & _
						 "Scan_Number_End" & chTab & _
						 "Charge" & chTab & _
						 "Quality_Score" & chTab & _
						 "Precursor_MZ" & chTab & _
						 "BPI"

			If blnIonFilteringEnabled Then
				strLineOut &= chTab & _
							  "Positive_Count_Before_Filter" & chTab & _
							  "Positive_Count_After_Filter"
			End If

			If mSpectrumFilterMode = eSpectrumFilterMode.mode1 Or mSpectrumFilterMode = eSpectrumFilterMode.mode2 Then
				' Include the extended headers for Filter Mode 1
				strLineOut &= chTab & _
							  "IonPair_Count" & chTab & _
							  "IonPair_Score" & chTab & _
							  "PercentMassSpaceMatched" & chTab & _
							  "PercentAbundantPeaksWithMassDiffMatches" & chTab & _
							  "SequenceTagLengthMax" & chTab & _
							  "SequenceTag" & chTab & _
							  "AbundantPeaksSumSquares" & chTab & _
							  "FilterMode2Score"

			End If

			If blnIncludeNLStats Then
				strLineOut &= chTab & "NL98" & _
							  chTab & "NL49_2+" & _
							  chTab & "NL33_3+" & _
							  chTab & "NL65_3+"


				If mFilterMode3Options.ConsiderWaterLoss Then
					strLineOut &= chTab & "NL116_1+" & _
								  chTab & "NL58_2+" & _
								  chTab & "NL107_2+" & _
								  chTab & "NL39_3+" & _
								  chTab & "NL71_3+"
				End If
			End If

			If blnIncludeReporterIonStats Then
				Dim dblReporterIonMZs As New System.Collections.Generic.List(Of Double)

				If GetFilterMode4ReporterIons(dblReporterIonMZs) Then
					intReporterIonMZCount = dblReporterIonMZs.Count

					For Each dblItem As Double In dblReporterIonMZs
						strLineOut &= chTab & "ReporterIon_" & Math.Round(dblItem, 0).ToString()
					Next

				End If

				strLineOut &= chTab & "ReporterIon_Count" & chTab & "ReporterIon_Sum"
			End If

			swReportFile.WriteLine(strLineOut)
		End If

		strLineOut = ScanNumberStart.ToString & chTab & _
					 ScanNumberEnd.ToString & chTab & _
					 Charge.ToString & chTab & _
					 udtSpectrumQualityScore.SpectrumQualityScore.ToString & chTab & _
					 Math.Round(sngPrecursorMZ, 4).ToString & chTab & _
					 Math.Round(sngBPI, 1).ToString


		If blnIonFilteringEnabled Then
			strLineOut &= chTab & _
						  intPositiveDataCountBeforeFilter.ToString & chTab & _
						  intPositiveDataCountAfterFilter.ToString
		End If


		If mSpectrumFilterMode = eSpectrumFilterMode.mode1 Or mSpectrumFilterMode = eSpectrumFilterMode.mode2 Then
			' Include the extended score data
			With udtSpectrumQualityScore
				strLineOut &= chTab & _
							 .IonPairCount & chTab & _
							 .IonPairCountScore & chTab & _
							 .PercentMassSpaceMatched & chTab & _
							 .PercentAbundantPeaksWithMassDiffMatches & chTab & _
							 .SequenceTagLengthMax & chTab & _
							 .SequenceTagLongest & chTab & _
							 .AbundantPeaksSumSquares.ToString("0") & chTab & _
							 .FilterMode2Score
			End With
		End If

		If blnIncludeNLStats Then
			With udtIonMatchStats
				strLineOut &= chTab & Math.Round(.IonIntensitiesNormalized(SearchMassSpecsClass.eSearchMassCodeConstants.NL98), 0).ToString & _
							  chTab & Math.Round(.IonIntensitiesNormalized(SearchMassSpecsClass.eSearchMassCodeConstants.NL49), 0).ToString & _
							  chTab & Math.Round(.IonIntensitiesNormalized(SearchMassSpecsClass.eSearchMassCodeConstants.NL33), 0).ToString & _
							  chTab & Math.Round(.IonIntensitiesNormalized(SearchMassSpecsClass.eSearchMassCodeConstants.NL65), 0).ToString

				If mFilterMode3Options.ConsiderWaterLoss Then
					strLineOut &= chTab & Math.Round(.IonIntensitiesNormalized(SearchMassSpecsClass.eSearchMassCodeConstants.NL116), 0).ToString & _
							  chTab & Math.Round(.IonIntensitiesNormalized(SearchMassSpecsClass.eSearchMassCodeConstants.NL58), 0).ToString & _
							  chTab & Math.Round(.IonIntensitiesNormalized(SearchMassSpecsClass.eSearchMassCodeConstants.NL107), 0).ToString & _
							  chTab & Math.Round(.IonIntensitiesNormalized(SearchMassSpecsClass.eSearchMassCodeConstants.NL39), 0).ToString & _
							  chTab & Math.Round(.IonIntensitiesNormalized(SearchMassSpecsClass.eSearchMassCodeConstants.NL71), 0).ToString
				End If
			End With
		End If

		If blnIncludeReporterIonStats Then
			If intReporterIonMZCount = 0 Then
				Dim dblReporterIonMZs As New System.Collections.Generic.List(Of Double)
				If GetFilterMode4ReporterIons(dblReporterIonMZs) Then
					intReporterIonMZCount = dblReporterIonMZs.Count
				End If
			End If

			Dim intPositiveReporterIonCount As Integer = 0
			Dim sngIonIntensitySum As Single = 0

			With udtIonMatchStats
				If Not .IonIntensitiesNormalized Is Nothing Then
					For intIndex As Integer = 0 To .IonIntensitiesNormalized.Length - 1
						If intIndex >= intReporterIonMZCount Then
							Exit For
						End If

						strLineOut &= chTab & Math.Round(.IonIntensitiesNormalized(intIndex), 1).ToString

						If .IonIntensitiesNormalized(intIndex) > 0 Then
							intPositiveReporterIonCount += 1
							sngIonIntensitySum += .IonIntensitiesNormalized(intIndex)
						End If
					Next
				End If
			End With

			strLineOut &= chTab & intPositiveReporterIonCount.ToString() & chTab & Math.Round(sngIonIntensitySum, 1).ToString
		End If

		swReportFile.WriteLine(strLineOut)

	End Sub

	''' <summary>
	''' Makes a copy (or renames a file), naming the backup copy OriginalFileName.bak.  If other .Bak files already exist, they are renamed before strFileToBackup is backed up
	''' </summary>
	''' <param name="strFileToBackup">File to backup</param>
	''' <param name="blnDuplicateFile">When true, then makes a copy of the original file.  Otherwise, just renames the original file to .bak</param>
	''' <returns></returns>
	''' <remarks></remarks>
	Private Function BackupFileWithRevisioning(ByVal strFileToBackup As String, ByVal blnDuplicateFile As Boolean) As Boolean
		' Returns True if file successfully backed up
		' Returns False if an error

		Dim strNewFilePath As String
		Dim strCheckPath, strCheckPathNew As String

		Dim intIndex As Integer
		Dim blnSuccess As Boolean

		' Assume success for now
		blnSuccess = True

		Try
			' See if any .bak files exist
			strNewFilePath = strFileToBackup & ".bak"

			If System.IO.File.Exists(strNewFilePath) Then
				' Need to find all matching .bak? files and rename; e.g. .bak1-> .bak2, .bak2 -> .bak3, etc.
				' Must work in reverse order
				For intIndex = 8 To 1 Step -1
					strCheckPath = strNewFilePath & intIndex.ToString

					If System.IO.File.Exists(strCheckPath) Then
						strCheckPathNew = strNewFilePath & (intIndex + 1).ToString
						If System.IO.File.Exists(strCheckPathNew) Then
							System.IO.File.Delete(strCheckPathNew)
						End If
						System.IO.File.Move(strCheckPath, strCheckPathNew)
					End If
				Next intIndex

				strCheckPath = String.Copy(strNewFilePath)
				strCheckPathNew = strNewFilePath & "1"

				System.IO.File.Move(strCheckPath, strCheckPathNew)
			End If

			If blnDuplicateFile Then
				System.IO.File.Copy(strFileToBackup, strNewFilePath, True)
			Else
				If System.IO.File.Exists(strNewFilePath) Then
					System.IO.File.Delete(strNewFilePath)
				End If
				System.IO.File.Move(strFileToBackup, strNewFilePath)
			End If
			LogMessage("Renamed " & System.IO.Path.GetFileName(strFileToBackup) & " to " & strNewFilePath)

		Catch ex As Exception
			ShowErrorMessage("Error backing up " & strFileToBackup & ": " & ex.Message)
			SetLocalErrorCode(eFilterMsMsSpectraErrorCodes.FileBackupAccessError)
			blnSuccess = False
		End Try

		Return blnSuccess

	End Function

	Private Function CalculateTargetBinIndex(ByVal sngValue As Single, ByVal sngBinnedDataMinimum As Single, ByVal intMaxIndex As Integer, ByVal sngPrecision As Single) As Integer
		Dim intTargetIndex As Integer

		' Round sngValue to the nearest sngPrecision and determine the target index to store in a binned array
		intTargetIndex = CInt(Math.Round(sngValue * (1 / sngPrecision), 0)) - CInt(sngBinnedDataMinimum / sngPrecision)

		If intTargetIndex < 0 Then
			' This shouldn't happen 
			intTargetIndex = 0
		ElseIf intTargetIndex > intMaxIndex Then
			' This shouldn't normally happen, but is possible in sngValue is larger than the maximum binned value
			intTargetIndex = intMaxIndex
		End If

		Return intTargetIndex
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
					LogMessage("File exists: " & strFilePathToOverwrite)

					' File already exists in destination; query user about overriding if mShowMessages = True
					If MyBase.ShowMessages Then
						eResponse = MsgBox("Overwrite the existing file: " & strFilePathToOverwrite, MsgBoxStyle.Question Or MsgBoxStyle.YesNoCancel Or MsgBoxStyle.DefaultButton2, "File Exists")
					Else
						eResponse = MsgBoxResult.No
					End If

					If eResponse = MsgBoxResult.Yes Then
						' Ok to overwrite the file; possibly always overwrite from now on (typically used when processing individual .dta files)
						If blnAlwaysOverwriteFromNowOnIfOKd Then mOverwriteExistingFiles = True
						LogMessage("  file will be overwritten")
					Else
						LogMessage("  existing file will not be overwritten")
						SetLocalErrorCode(eFilterMsMsSpectraErrorCodes.UserCancelledFileOverwrite)
						blnProceed = False
					End If
				End If
			Catch ex As Exception
				ShowErrorMessage("Error checking for file at " & strFilePathToOverwrite & ": " & ex.Message)
				SetLocalErrorCode(eFilterMsMsSpectraErrorCodes.FileBackupAccessError)
				blnProceed = False
			End Try
		End If

		Return blnProceed

	End Function

	Public Shared Function CheckForExistingScanStatsFiles(ByVal FolderPath As String, ByVal BaseDatasetName As String) As Boolean

		Dim strScanStatsFilePath As String
		Dim strScanStatsExFilePath As String

		strScanStatsFilePath = ConstructScanStatsFilePath(FolderPath, BaseDatasetName)
		strScanStatsExFilePath = ConstructScanStatsExFilePath(FolderPath, BaseDatasetName)

		If System.IO.File.Exists(strScanStatsFilePath) Then
			If System.IO.File.Exists(strScanStatsExFilePath) Then
				Return True
			End If
		End If

		Return False

	End Function

	Private Function CheckIonFilteringEnabled() As Boolean

		If mIonFilterOptions.RemovePrecursor OrElse _
		   mIonFilterOptions.RemoveChargeReducedPrecursors OrElse _
		   mIonFilterOptions.RemoveNeutralLossesFromChargeReducedPrecursors OrElse _
		   mIonFilterOptions.CondenseData OrElse _
		   Me.SpectrumFilterMode = eSpectrumFilterMode.mode4 Then
			Return True
		Else
			Return False
		End If

	End Function

	Protected Function CondenseData(ByRef intDataCount As Integer, _
									ByRef sngMassList() As Single, _
									ByRef sngIntensityList() As Single, _
									ByRef intPositiveDataCountAfterFilter As Integer) As Boolean

		Dim intIndex As Integer
		Dim intTargetIndex As Integer

		Dim intZeroIntensityPointsToKeep As Integer
		Dim intZeroIntensityPointsKept As Integer

		Dim blnSpectralDataUpdated As Boolean
		Dim blnZeroIntensityLineFound As Boolean
		Dim blnKeepPoint As Boolean

		blnSpectralDataUpdated = False
		intPositiveDataCountAfterFilter = 0

		Try

			If Not mIonFilterOptions.CondenseData Then
				' Nothing to filter
				blnSpectralDataUpdated = False
			Else

				' Step through the data
				' Look for data points with an intensity value of 0
				' When found, either set blnZeroIntensityLineFound to true, or skip the line if blnZeroIntensityLineFound is already true

				intTargetIndex = 0
				blnZeroIntensityLineFound = False

				intIndex = 0
				Do While intIndex < intDataCount - 1
					If intTargetIndex <> intIndex Then
						sngMassList(intTargetIndex) = sngMassList(intIndex)
						sngIntensityList(intTargetIndex) = sngIntensityList(intIndex)
					End If
					intTargetIndex += 1

					If sngIntensityList(intIndex) = 0 AndAlso Not blnZeroIntensityLineFound Then
						' This data point has an intensity of 0
						' Advance intIndex to one item before the next non-zero intensity value
						Do
							intIndex += 1
						Loop While intIndex < intDataCount - 2 AndAlso sngIntensityList(intIndex + 1) = 0
						blnZeroIntensityLineFound = True
					Else
						intIndex += 1
						blnZeroIntensityLineFound = False
					End If
				Loop

				' Update intDataCount
				If intDataCount > intTargetIndex Then
					blnSpectralDataUpdated = True
					intDataCount = intTargetIndex
				End If

				If mIonFilterOptions.CondenseDataMaxIonCount > 0 Then
					If intDataCount > mIonFilterOptions.CondenseDataMaxIonCount Then
						' Only keep the top mIonFilterOptions.CondenseDataMaxIonCount points
						' Determine the intensity threshold that would give us mIonFilterOptions.CondenseDataMaxIonCount points

						Dim sngIntensityListSorted() As Single
						Dim sngThreshold As Single

						ReDim sngIntensityListSorted(intDataCount - 1)

						Array.Copy(sngIntensityList, sngIntensityListSorted, intDataCount)

						Array.Sort(sngIntensityListSorted)

						sngThreshold = sngIntensityListSorted(intDataCount - mIonFilterOptions.CondenseDataMaxIonCount)

						If sngThreshold = 0 Then
							' When the threshold is zero, then need to compute the number of
							' zero-intensity data points that we should keep

							intPositiveDataCountAfterFilter = 0
							For intIndex = 0 To intDataCount - 1
								If sngIntensityList(intIndex) > 0 Then
									intPositiveDataCountAfterFilter += 1
								End If
							Next

							intZeroIntensityPointsToKeep = mIonFilterOptions.CondenseDataMaxIonCount - intPositiveDataCountAfterFilter
						End If

						' Now step through the data and only keep the points with an intensity >= sngThreshold
						intTargetIndex = 0
						intZeroIntensityPointsKept = 0

						For intIndex = 0 To intDataCount - 1
							blnKeepPoint = False
							If sngThreshold = 0 Then
								If sngIntensityList(intIndex) > 0 Then
									blnKeepPoint = True
								Else
									If intZeroIntensityPointsKept < intZeroIntensityPointsToKeep Then
										blnKeepPoint = True
										intZeroIntensityPointsKept += 1
									End If
								End If
							Else
								If sngIntensityList(intIndex) >= sngThreshold Then
									blnKeepPoint = True
								End If
							End If

							If blnKeepPoint Then
								sngMassList(intTargetIndex) = sngMassList(intIndex)
								sngIntensityList(intTargetIndex) = sngIntensityList(intIndex)
								intTargetIndex += 1
							End If
						Next

						' Update intDataCount
						If intDataCount > intTargetIndex Then
							blnSpectralDataUpdated = True
							intDataCount = intTargetIndex
						End If

					End If
				End If
			End If

			' Count the number of positive intensity values now that the filter has been applied
			intPositiveDataCountAfterFilter = 0
			For intIndex = 0 To intDataCount - 1
				If sngIntensityList(intIndex) > 0 Then
					intPositiveDataCountAfterFilter += 1
				End If
			Next intIndex

		Catch ex As Exception
			Throw New Exception("Error in CondenseData: " & ex.Message, ex)
		End Try

		Return blnSpectralDataUpdated

	End Function

	Public Shared Function ConstructScanStatsFilePath(ByVal FolderPath As String, ByVal BaseDatasetName As String) As String
		Return System.IO.Path.Combine(FolderPath, BaseDatasetName & "_ScanStats.txt")
	End Function

	Public Shared Function ConstructScanStatsExFilePath(ByVal FolderPath As String, ByVal BaseDatasetName As String) As String
		Return System.IO.Path.Combine(FolderPath, BaseDatasetName & "_ScanStatsEx.txt")
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


	Protected Function ComputeBPIAndTIC(ByVal sngValues() As Single, ByRef sngBPI As Single, ByRef dblTIC As Double) As Boolean
		If sngValues Is Nothing Then
			Return False
		Else
			Return ComputeBPIAndTIC(sngValues, sngValues.Length - 1, sngBPI, dblTIC)
		End If
	End Function

	Protected Function ComputeBPIAndTIC(ByVal sngValues() As Single, ByVal intIndexMax As Integer, ByRef sngBPI As Single, ByRef dblTIC As Double) As Boolean
		Dim intIndex As Integer
		Dim blnSuccess As Boolean

		sngBPI = 0
		dblTIC = 0

		If sngValues Is Nothing OrElse sngValues.Length <= 0 OrElse intIndexMax < 0 Then
			' Zero-length array
			blnSuccess = False
		Else
			If intIndexMax > sngValues.Length - 1 Then
				intIndexMax = sngValues.Length - 1
			End If

			sngBPI = Integer.MinValue

			For intIndex = 0 To intIndexMax
				If sngValues(intIndex) > sngBPI Then
					sngBPI = sngValues(intIndex)
				End If
				dblTIC += sngValues(intIndex)
			Next intIndex

			blnSuccess = True
		End If

		Return blnSuccess

	End Function

	Protected Function ComputeSpectrumNoiseLevel(ByVal sngIntensityList() As Single, ByVal intArrayLength As Integer) As clsBaselineNoiseEstimator.udtBaselineNoiseStatsType
		Const FRACTION_LOW_INTENSITY_DATA_TO_AVERAGE As Single = 0.75

		Return ComputeSpectrumNoiseLevel(sngIntensityList, intArrayLength, FRACTION_LOW_INTENSITY_DATA_TO_AVERAGE)
	End Function

	Protected Function ComputeSpectrumNoiseLevel(ByRef sngIntensityList() As Single, ByVal intArrayLength As Integer, ByVal sngFractionLowIntensityDataToAverage As Single) As clsBaselineNoiseEstimator.udtBaselineNoiseStatsType

		Static objBaselineNoiseEstimator As clsBaselineNoiseEstimator
		If objBaselineNoiseEstimator Is Nothing Then
			objBaselineNoiseEstimator = New clsBaselineNoiseEstimator
		End If

		Dim udtBaselineNoiseOptions As clsBaselineNoiseEstimator.udtBaselineNoiseOptionsType
		Dim udtBaselineNoiseStats As clsBaselineNoiseEstimator.udtBaselineNoiseStatsType

		Dim blnSuccess As Boolean

		udtBaselineNoiseOptions.InitializeToDefaults()

		With udtBaselineNoiseOptions
			.BaselineNoiseMode = clsBaselineNoiseEstimator.eNoiseThresholdModes.TrimmedMedianByAbundance
			.TrimmedMeanFractionLowIntensityDataToExamine = sngFractionLowIntensityDataToAverage
		End With

		blnSuccess = objBaselineNoiseEstimator.ComputeNoiseLevel(sngIntensityList, intArrayLength, udtBaselineNoiseOptions, udtBaselineNoiseStats)

		If Not blnSuccess Then
			With udtBaselineNoiseStats
				.NoiseLevel = 0
				.NoiseStDev = 0
				.PointsUsed = 0
			End With
		End If

		Return udtBaselineNoiseStats

	End Function

	Protected Function ComputeSpectrumNoiseLevelMultiSegment(ByRef sngMassList() As Single, ByRef sngIntensityList() As Single, ByVal intArrayLength As Integer, ByVal intSegmentedSignalToNoiseMZWidth As Integer, ByRef udtBaselineNoiseStatSegments() As clsBaselineNoiseEstimator.udtBaselineNoiseStatSegmentsType) As Boolean
		Const FRACTION_LOW_INTENSITY_DATA_TO_AVERAGE As Single = 0.75

		Return ComputeSpectrumNoiseLevelMultiSegment(sngMassList, sngIntensityList, intArrayLength, intSegmentedSignalToNoiseMZWidth, FRACTION_LOW_INTENSITY_DATA_TO_AVERAGE, udtBaselineNoiseStatSegments)
	End Function

	Protected Function ComputeSpectrumNoiseLevelMultiSegment(ByRef sngMassList() As Single, ByRef sngIntensityList() As Single, ByVal intArrayLength As Integer, ByVal intSegmentedSignalToNoiseMZWidth As Integer, ByVal sngFractionLowIntensityDataToAverage As Single, ByRef udtBaselineNoiseStatSegments() As clsBaselineNoiseEstimator.udtBaselineNoiseStatSegmentsType) As Boolean

		Static objBaselineNoiseEstimator As clsBaselineNoiseEstimator
		If objBaselineNoiseEstimator Is Nothing Then
			objBaselineNoiseEstimator = New clsBaselineNoiseEstimator
		End If

		Dim udtBaselineNoiseOptions As clsBaselineNoiseEstimator.udtBaselineNoiseOptionsType

		Dim blnSuccess As Boolean

		udtBaselineNoiseOptions.InitializeToDefaults()

		With udtBaselineNoiseOptions
			.BaselineNoiseMode = clsBaselineNoiseEstimator.eNoiseThresholdModes.SegmentedTrimmedMedianByAbundance
			If sngFractionLowIntensityDataToAverage > 0 Then
				.TrimmedMeanFractionLowIntensityDataToExamine = sngFractionLowIntensityDataToAverage
			End If
			If intSegmentedSignalToNoiseMZWidth > 0 Then
				.SegmentedTrimmedMedianTargetSegmentWidthX = intSegmentedSignalToNoiseMZWidth
			End If
		End With

		blnSuccess = objBaselineNoiseEstimator.ComputeMultiSegmentNoiseLevel(sngMassList, sngIntensityList, intArrayLength, udtBaselineNoiseOptions, udtBaselineNoiseStatSegments)

		Return blnSuccess

	End Function

	Protected Function ConstructSequenceTag(ByVal udtAASpacingMatches() As udtAminoAcidSpacingStatsType, ByVal intMatchIndex As Integer) As String
		Dim strSequenceTag As String
		Dim intIndexPointer As Integer

		strSequenceTag = udtAASpacingMatches(intMatchIndex).AminoAcidSymbol
		intIndexPointer = intMatchIndex

		Do
			intIndexPointer = udtAASpacingMatches(intIndexPointer).AdjacentSpacingIndexPointer
			If intIndexPointer >= 0 Then
				strSequenceTag &= udtAASpacingMatches(intIndexPointer).AminoAcidSymbol
			End If
		Loop While intIndexPointer >= 0

		Return strSequenceTag

	End Function

	Public Shared Function ConvoluteMass(ByVal dblMassMZ As Double, ByVal intCurrentCharge As Integer, Optional ByVal intDesiredCharge As Integer = 1) As Double
		' Converts dblMassMZ to the MZ that would appear at the given intDesiredCharge
		' If intCurrentCharge = 0, then assumes dblMassMZ is the neutral, monoisotopic mass
		' To return the neutral mass, set intDesiredCharge to 0

		Dim dblNewMZ As Double

		Try
			If intCurrentCharge = intDesiredCharge Then
				dblNewMZ = dblMassMZ
			Else
				If intCurrentCharge = 1 Then
					dblNewMZ = dblMassMZ
				ElseIf intCurrentCharge > 1 Then
					' Convert dblMassMZ to M+H
					dblNewMZ = (dblMassMZ * intCurrentCharge) - MASS_PROTON * (intCurrentCharge - 1)
				ElseIf intCurrentCharge = 0 Then
					' Convert dblMassMZ (which is neutral) to M+H and store in dblNewMZ
					dblNewMZ = dblMassMZ + MASS_PROTON
				Else
					' Negative charges are not supported; return 0
					Return 0
				End If

				If intDesiredCharge > 1 Then
					dblNewMZ = (dblNewMZ + MASS_PROTON * (intDesiredCharge - 1)) / intDesiredCharge
				ElseIf intDesiredCharge = 1 Then
					' Return M+H, which is currently stored in dblNewMZ
				ElseIf intDesiredCharge = 0 Then
					' Return the neutral mass
					dblNewMZ -= MASS_PROTON
				Else
					' Negative charges are not supported; return 0
					dblNewMZ = 0
				End If
			End If
		Catch ex As Exception
			' Error occurred
			dblNewMZ = 0
		End Try

		Return dblNewMZ

	End Function

	Protected Function CountPointsPassingSN(ByRef sngIntensityList() As Single, ByVal sngNoiseLevel As Single, ByVal sngSNThreshold As Single, ByVal intPeakCountThreshold As Integer, ByVal sngScoreMaximum As Single, ByRef sngComputedScore As Single) As Boolean
		' This function examines the data in sngIntensityList() to see if at least intPeakCountThreshold points
		'  have a signal-to-noise value >= sngSNThreshold
		' If the minimum number pass, then sets sngComputedScore to sngScoreMaximum and returns True
		' If the minimum number do not pass, then sets sngComputedScore to a value between 0 and sngScoreMaximum and returns False
		'
		' This function assumes sngIntensityList has been sorted descending

		Dim intIndex As Integer
		Dim intPassCount As Integer

		intPassCount = 0
		intIndex = sngIntensityList.Length - 1
		Do While intIndex >= 0
			If sngIntensityList(intIndex) / sngNoiseLevel >= sngSNThreshold Then
				intPassCount += 1

				If intPassCount >= intPeakCountThreshold Then
					Exit Do
				End If
			Else
				Exit Do
			End If

			intIndex -= 1
		Loop

		If intPassCount >= intPeakCountThreshold Then
			sngComputedScore = sngScoreMaximum
			Return True
		Else
			sngComputedScore = (intPassCount / CSng(intPeakCountThreshold)) * sngScoreMaximum
			Return False
		End If

	End Function

	''' <summary>
	''' </summary>
	''' <param name="sngMassList"></param>
	''' <param name="sngIntensityList"></param>
	''' <param name="udtSpectrumHeaderInfo">Passed by Ref since EvaluateMsMsSpectrumMode4 will update the parent ion mass values</param>
	''' <param name="sngBPI"></param>
	''' <param name="blnIncludeNLStats"></param>
	''' <param name="blnIncludeReporterIonStats"></param>
	''' <param name="udtIonMatchStats"></param>
	''' <returns></returns>
	''' <remarks></remarks>
	Protected Function EvaluateMsMsSpectrumStart(ByRef sngMassList() As Single, ByRef sngIntensityList() As Single, _
												 ByRef udtSpectrumHeaderInfo As MsMsDataFileReader.clsMsMsDataFileReaderBaseClass.udtSpectrumHeaderInfoType, _
												 ByRef sngBPI As Single, _
												 ByRef blnIncludeNLStats As Boolean, _
												 ByRef blnIncludeReporterIonStats As Boolean, _
												 ByRef udtIonMatchStats As udtIonMatchStatsType) As udtSpectrumQualityScoreType

		Dim strSpectrumID As String
		Dim udtSpectrumQualityScore As udtSpectrumQualityScoreType = New udtSpectrumQualityScoreType
		Dim blnPassesFilter As Boolean

		With udtSpectrumHeaderInfo
			strSpectrumID = .ScanNumberStart & "." & .ScanNumberEnd & "." & .ParentIonCharges(0)
		End With

		udtSpectrumQualityScore.Initialize()

		blnIncludeNLStats = False
		blnIncludeReporterIonStats = False

		Select Case mSpectrumFilterMode
			Case eSpectrumFilterMode.mode1, eSpectrumFilterMode.mode2
				udtSpectrumQualityScore = EvaluateMsMsSpectrum(sngMassList, sngIntensityList, _
															   udtSpectrumHeaderInfo.ParentIonCharges(0), _
															   udtSpectrumHeaderInfo.ParentIonMH, _
															   udtSpectrumHeaderInfo.ParentIonMZ, _
															   sngBPI, _
															   strSpectrumID)

			Case eSpectrumFilterMode.mode3
				blnPassesFilter = EvaluateMsMsSpectrumMode3(sngMassList, sngIntensityList, _
															udtSpectrumHeaderInfo.ParentIonMZ, _
															udtSpectrumHeaderInfo.ParentIonCharges(0), _
															udtIonMatchStats, _
															udtSpectrumQualityScore.SpectrumQualityScore)
				sngBPI = udtIonMatchStats.BPI
				blnIncludeNLStats = mIncludeNLStatsOnFilterReport

			Case eSpectrumFilterMode.mode4
				blnPassesFilter = EvaluateMsMsSpectrumMode4(sngMassList, sngIntensityList, _
															udtSpectrumHeaderInfo, _
															udtIonMatchStats, _
															udtSpectrumQualityScore.SpectrumQualityScore)
				sngBPI = udtIonMatchStats.BPI
				blnIncludeReporterIonStats = True

			Case Else
				' Includes eSpectrumFilterMode.NoFilter
				If mMinimumQualityScore <= 1 Then
					udtSpectrumQualityScore.SpectrumQualityScore = 1
				Else
					udtSpectrumQualityScore.SpectrumQualityScore = mMinimumQualityScore
				End If
		End Select

		Return udtSpectrumQualityScore
	End Function

	' This function computes the stats for both Filter Mode 1 and Filter Mode 2, returning a structure of type udtSpectrumQualityScoreType
	' The .SpectrumQualityScore value in that structure will be based on either FilterMode1 or FilterMode2, depending on mSpectrumFilterMode

	Public Function EvaluateMsMsSpectrum(ByVal sngMassList() As Single, ByVal sngIntensityList() As Single, ByVal intCharge As Integer, ByVal sngParentIonMH As Single, ByVal dblParentIonMZ As Double, ByRef sngBPI As Single, Optional ByVal strSpectrumID As String = "") As udtSpectrumQualityScoreType
		Const chTab As Char = ControlChars.Tab

		Dim dblTIC As Double
		Dim udtSpectrumQualityScore As udtSpectrumQualityScoreType = New udtSpectrumQualityScoreType
		Dim udtMode2ScoreDetails As udtMode2ScoreDetailsType

		Dim srSpectrumFile As System.IO.StreamWriter

		udtSpectrumQualityScore = EvaluateMsMsSpectrumMode1(sngMassList, sngIntensityList, intCharge, sngParentIonMH, sngBPI, dblTIC, strSpectrumID)

		If mFilterMode1Options.UseLogIntensity Then
			' Need to re-compute BPI and TIC since the ones returned by EvaluateMsMsSpectrumMode1 are log-based
			ComputeBPIAndTIC(sngIntensityList, sngBPI, dblTIC)
		End If

		' Now compute the Mode2 Quality Score
		' Since EvaluateMsMsSpectrumMode1 already computed the BPI and TIC, 
		'  we call the version of EvaluateMsMsSpectrumMode2() that takes both and assumes they are valid
		udtMode2ScoreDetails = EvaluateMsMsSpectrumMode2(sngMassList, sngIntensityList, dblParentIonMZ, intCharge, sngBPI, dblTIC)

		udtSpectrumQualityScore.FilterMode2Score = udtMode2ScoreDetails.QualityScore

		If mDebugMode Then
			srSpectrumFile = OpenDebugFile(strSpectrumID, True)

			srSpectrumFile.WriteLine("Filter Mode 2 Score Summary")

			With udtMode2ScoreDetails
				srSpectrumFile.WriteLine("IntensityThresholdForSignificantPeaks" & chTab & .IntensityThresholdForSignificantPeaks)
				srSpectrumFile.WriteLine("SignificantPeakNumberCount" & chTab & .SignificantPeakNumberCount)
				srSpectrumFile.WriteLine("SignificantPeakCountIsValidForChargeState" & chTab & .SignificantPeakCountIsValidForChargeState)
				srSpectrumFile.WriteLine("NoiseLevel" & chTab & .NoiseLevel)
				srSpectrumFile.WriteLine("TICScore" & chTab & .TICScore)
				srSpectrumFile.WriteLine("HighQualitySNThresholdScore" & chTab & .HighQualitySNThresholdScore)
				srSpectrumFile.WriteLine("ModerateQualitySNThresholdScore" & chTab & .ModerateQualitySNThresholdScore)
				srSpectrumFile.WriteLine("LowQualitySNThresholdScore" & chTab & .LowQualitySNThresholdScore)
				srSpectrumFile.WriteLine("QualityScore" & chTab & .QualityScore)
			End With

			srSpectrumFile.Close()
		End If

		' Update .SpectrumQualityScore based on mSpectrumFilterMode
		If mSpectrumFilterMode = eSpectrumFilterMode.mode1 Then
			udtSpectrumQualityScore.SpectrumQualityScore = udtSpectrumQualityScore.PercentAbundantPeaksWithMassDiffMatches
		ElseIf mSpectrumFilterMode = eSpectrumFilterMode.mode2 Then
			udtSpectrumQualityScore.SpectrumQualityScore = udtSpectrumQualityScore.FilterMode2Score
		End If

		Return udtSpectrumQualityScore

	End Function

	' Filter Mode 1 is based on an algorithm developed by Matthew Monroe at PNNL - eSpectrumFilterMode.mode1
	' It looks for ion spacings that match the standard amino acid masses

	Public Function EvaluateMsMsSpectrumMode1(ByVal sngMassList() As Single, ByVal sngIntensityList() As Single, ByVal intCharge As Integer, ByVal sngParentIonMH As Single, ByRef sngBPI As Single, ByRef dblTIC As Double, Optional ByVal strSpectrumID As String = "") As udtSpectrumQualityScoreType
		' Examines the mass spectrum x,y pairs in sngMassList() and sngIntensityList()
		' Looks for ion spacings that match the standard amino acid masses (intensity must be >= .NoiseLevelIntensityThreshold and S/N must be >= .SignalToNoiseThreshold)
		' For 3+ or higher spectra, also looks for spacings for 2+ fragmentation peaks
		' If the number of spacings is >= mMaximumIonSpacingMatchCount then returns a score of 1
		' If the number of spacings is between mMaximumIonSpacingMatchCount and mMinimumIonSpacingMatchCount then returns a score between 0 and 1
		' Otherwise, returns 0
		'
		' If an error occurs, will return a score of 1, rather than marking spectrum as not passing the filter
		' Assumes that sngMassList() is sorted ascending and sngIntensityList() is sorted parallel with it

		Dim chTab As Char = ControlChars.Tab

		Dim objRangeSearch As New clsSearchRange

		Dim blnPointUsed As Boolean
		Dim blnMatchFound As Boolean

		Dim blnSuccess As Boolean

		Dim srSpectrumFile As System.IO.StreamWriter = Nothing

		Dim udtSpectrumQualityScore As udtSpectrumQualityScoreType = New udtSpectrumQualityScoreType

		Dim intIndex As Integer
		Dim intIndexMatch As Integer

		Dim intAASpacingCandidateIndex As Integer
		Dim intAASpacingMatchIndex As Integer

		Dim intBestCandidateIndex As Integer
		Dim intBestSequenceTagLength As Integer
		Dim dblBestSequenceTagScore As Double

		Dim intSegmentIndex As Integer
		Dim intIndexMid As Integer

		Dim sngIntensityListScaled() As Single
		Dim sngNoiseLevelIntensityThresholdScaled As Single
		Dim sngSNThresholdScaled As Single

		Dim intDataCount As Integer
		Dim sngWorkingMasses() As Single
		Dim sngWorkingMassSignalToNoise() As Single
		Dim sngWorkingSNMaximum As Single

		Dim sngMassDiffTheoretical As Single
		Dim sngCurrentPointNoiseLevel As Single

		Dim sngMatchingMassDiffTheoretical As Single
		Dim blnMatchIs2Plus As Boolean

		Dim intPointerArray() As Integer
		Dim intPointerArrayToSort() As Integer

		Dim sngSortedSignalToNoiseList() As Single
		Dim sngSortedMassList() As Single
		Dim sngSignalToNoiseList() As Single

		Dim intStandardSpacingMinimum As Integer
		Dim intStandardSpacingMaximum As Integer

		Dim intAASpacingMatchCandidateCount As Integer
		Dim udtAASpacingMatchCandidates() As udtAminoAcidSpacingStatsType

		Dim intAASpacingMatchCount As Integer
		Dim udtAASpacingMatches() As udtAminoAcidSpacingStatsType

		Const BINNING_PRECISION As Single = 0.5
		Dim bytBinnedData() As Byte
		ReDim bytBinnedData(0)

		Dim intBinIndex As Integer
		Dim intBinCountUsed As Integer

		Dim intIndexWithLongestSequenceTag As Integer

		Dim udtBaselineNoiseStatSegments() As clsBaselineNoiseEstimator.udtBaselineNoiseStatSegmentsType = Nothing

		Dim dblProductSumWorkingMasses As Double
		Dim dblProductSumAAMatches As Double

		Try
			udtSpectrumQualityScore.Initialize()
			sngBPI = 0
			dblTIC = 0

			' Define the minimum and maximum standard spacings based on the charge state
			If intCharge < 1 Then
				' Invalid (or unknown) charge; set to 2
				intCharge = 2
			ElseIf intCharge > mFilterMode1Options.StandardMassSpacingCounts.Length Then
				' Charge higher than we have defined standard spacings for
				' Set to the maximum charge defined by .StandardMassSpacingCounts()
				intCharge = mFilterMode1Options.StandardMassSpacingCounts.Length
			End If

			intStandardSpacingMinimum = mFilterMode1Options.StandardMassSpacingCounts(intCharge - 1).Minimum
			intStandardSpacingMaximum = mFilterMode1Options.StandardMassSpacingCounts(intCharge - 1).Maximum

			If intStandardSpacingMaximum < intStandardSpacingMinimum Then
				intStandardSpacingMaximum = intStandardSpacingMinimum
			End If

			If sngMassList.Length <> sngIntensityList.Length OrElse sngMassList.Length <= 0 Then
				' List lengths don't match; unable to process the data
				udtSpectrumQualityScore.SpectrumQualityScore = 0
			Else
				intDataCount = sngMassList.Length
				ReDim intPointerArray(intDataCount - 1)
				ReDim sngIntensityListScaled(intDataCount - 1)

				If mFilterMode1Options.UseLogIntensity Then
					' Convert to Log Intensity values
					For intIndex = 0 To intDataCount - 1
						sngIntensityListScaled(intIndex) = CSng(Math.Log(sngIntensityList(intIndex)))
					Next intIndex
				Else
					Array.Copy(sngIntensityList, sngIntensityListScaled, intDataCount)
				End If

				ComputeBPIAndTIC(sngIntensityListScaled, sngBPI, dblTIC)


				If mDebugMode Then
					srSpectrumFile = OpenDebugFile(strSpectrumID, False)

					srSpectrumFile.WriteLine("SegmentNumber" & chTab & _
											"SegmentIndexStart" & chTab & _
											"SegmentIndexMind" & chTab & _
											"SegmentIndexEnd" & chTab & _
											"MassStart" & chTab & _
											"MassMid" & chTab & _
											"MassEnd" & chTab & _
											"NoiseLevel" & chTab & _
											"NoiseStDev" & chTab & _
											"NoiseThresholdModeUsed")
				End If

				If mFilterMode1Options.EnableSegmentedSignalToNoise Then
					' Compute S/N for separate segments across the spectrum
					blnSuccess = ComputeSpectrumNoiseLevelMultiSegment(sngMassList, sngIntensityListScaled, sngIntensityListScaled.Length, mFilterMode1Options.SegmentedSignalToNoiseMZWidth, udtBaselineNoiseStatSegments)
				Else
					blnSuccess = False
				End If

				If Not blnSuccess OrElse udtBaselineNoiseStatSegments Is Nothing Then
					' Compute a single S/N value for the entire spectrum
					ReDim udtBaselineNoiseStatSegments(0)
					With udtBaselineNoiseStatSegments(0)
						.BaselineNoiseStats = ComputeSpectrumNoiseLevel(sngIntensityListScaled, sngIntensityListScaled.Length)
						.SegmentIndexStart = 0
						.SegmentIndexEnd = sngIntensityListScaled.Length - 1
						.ComputeMidPoint()
					End With
					blnSuccess = True
				End If

				If mDebugMode Then
					' Write out the segmented noise level details
					For intSegmentIndex = 0 To udtBaselineNoiseStatSegments.Length - 1
						With udtBaselineNoiseStatSegments(intSegmentIndex)
							If CInt(.SegmentMidpointValue) >= .SegmentIndexStart AndAlso CInt(.SegmentMidpointValue) <= .SegmentIndexEnd Then
								intIndexMid = CInt(.SegmentMidpointValue)
							Else
								intIndexMid = .SegmentIndexStart
							End If

							srSpectrumFile.WriteLine(intSegmentIndex & chTab & _
											.SegmentIndexStart & chTab & _
											.SegmentMidpointValue & chTab & _
											.SegmentIndexEnd & chTab & _
											sngMassList(.SegmentIndexStart).ToString("0") & chTab & _
											sngMassList(intIndexMid).ToString("0") & chTab & _
											sngMassList(.SegmentIndexEnd).ToString("0") & chTab & _
											.BaselineNoiseStats.NoiseLevel.ToString("0.0") & chTab & _
											.BaselineNoiseStats.NoiseStDev.ToString("0.0") & chTab & _
											.BaselineNoiseStats.NoiseThresholdModeUsed.ToString)
						End With
					Next intSegmentIndex

					srSpectrumFile.WriteLine()

				End If

				' Compute the S/N value for every data point
				ReDim sngSignalToNoiseList(sngMassList.Length - 1)
				For intIndex = 0 To sngMassList.Length - 1
					sngCurrentPointNoiseLevel = clsBaselineNoiseEstimator.ComputeSignalToNoiseUsingMultiSegmentData(intIndex, udtBaselineNoiseStatSegments)

					If sngCurrentPointNoiseLevel > 0 Then
						sngSignalToNoiseList(intIndex) = sngIntensityListScaled(intIndex) / sngCurrentPointNoiseLevel
					Else
						sngSignalToNoiseList(intIndex) = -1
					End If
				Next intIndex

				' Populate intPointerArray() using only those data points in sngMassList() that have an intensity >= .NoiseLevelIntensityThreshold and S/N >= .SignalToNoiseThreshold

				sngNoiseLevelIntensityThresholdScaled = mFilterMode1Options.NoiseLevelIntensityThreshold
				sngSNThresholdScaled = mFilterMode1Options.SignalToNoiseThreshold
				If mFilterMode1Options.UseLogIntensity Then
					sngNoiseLevelIntensityThresholdScaled = CSng(Math.Log(sngNoiseLevelIntensityThresholdScaled))
					sngSNThresholdScaled = CSng(Math.Log(sngSNThresholdScaled))
				End If

				intDataCount = 0
				For intIndex = 0 To sngMassList.Length - 1
					If sngIntensityListScaled(intIndex) >= sngNoiseLevelIntensityThresholdScaled Then
						If sngSignalToNoiseList(intIndex) = -1 OrElse sngSignalToNoiseList(intIndex) >= sngSNThresholdScaled Then
							intPointerArray(intDataCount) = intIndex
							intDataCount += 1
						End If
					End If
				Next intIndex

				' Possibly shrink intPointerArray()
				If intDataCount > 0 And intDataCount <> sngMassList.Length Then
					ReDim Preserve intPointerArray(intDataCount - 1)
				End If

				If intDataCount > 0 And intDataCount > intStandardSpacingMinimum Then
					If mFilterMode1Options.DataPointCountToConsider > 1 And mFilterMode1Options.DataPointCountToConsider < intDataCount Then
						' Sort the data by decreasing S/N
						' First populate intPointerArrayToSort and sngSortedSignalToNoiseList
						ReDim intPointerArrayToSort(intDataCount - 1)
						ReDim sngSortedSignalToNoiseList(intDataCount - 1)

						For intIndex = 0 To intDataCount - 1
							intPointerArrayToSort(intIndex) = intPointerArray(intIndex)
							sngSortedSignalToNoiseList(intIndex) = sngSignalToNoiseList(intPointerArray(intIndex))
						Next intIndex

						' Sort intPointerArrayToSort() parallel with sngSortedSignalToNoiseList
						Array.Sort(sngSortedSignalToNoiseList, intPointerArrayToSort)

						' Reverse the order of the items in intPointerArrayToSort
						Array.Reverse(intPointerArrayToSort)

						If intDataCount > mFilterMode1Options.DataPointCountToConsider Then
							intDataCount = mFilterMode1Options.DataPointCountToConsider
						End If

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

					If mDebugMode Then
						' Write out the points and S/N values in this mass spectrum
						srSpectrumFile.WriteLine("Mass" & chTab & "Intensity" & chTab & "NoiseLevel" & chTab & "S/N" & chTab & "PointPassesThresholds")

						For intIndex = 0 To sngMassList.Length - 1
							If Array.BinarySearch(intPointerArray, intIndex) >= 0 Then
								blnPointUsed = True
							Else
								blnPointUsed = False
							End If

							If sngSignalToNoiseList(intIndex) > 0 Then
								sngCurrentPointNoiseLevel = sngIntensityListScaled(intIndex) / sngSignalToNoiseList(intIndex)
							Else
								sngCurrentPointNoiseLevel = 0
							End If

							If blnPointUsed Then
								' Only writing out points that are in intPointerArray
								srSpectrumFile.WriteLine(sngMassList(intIndex) & chTab & sngIntensityListScaled(intIndex) & chTab & sngCurrentPointNoiseLevel & chTab & sngSignalToNoiseList(intIndex) & chTab & blnPointUsed.ToString)
							End If
						Next intIndex

					End If

					' Populate sngWorkingMasses and sngWorkingMassSignalToNoise
					' Reserve space for one extra point in sngWorkingMasses() so we can add the parent ion
					ReDim sngWorkingMasses(intDataCount)
					ReDim sngWorkingMassSignalToNoise(intDataCount)
					sngWorkingSNMaximum = 0

					' Populate the sngWorking arrays
					' At the same time, compute the sum of the squares of the S/N values

					dblProductSumWorkingMasses = 0
					For intIndex = 0 To intDataCount - 1
						sngWorkingMasses(intIndex) = sngMassList(intPointerArray(intIndex))
						sngWorkingMassSignalToNoise(intIndex) = sngSignalToNoiseList(intPointerArray(intIndex))

						dblProductSumWorkingMasses += (sngWorkingMassSignalToNoise(intIndex) * sngWorkingMassSignalToNoise(intIndex))

						If sngWorkingMassSignalToNoise(intIndex) > sngWorkingSNMaximum Then
							sngWorkingSNMaximum = sngWorkingMassSignalToNoise(intIndex)
						End If
					Next intIndex

					' Add the parent ion
					If sngParentIonMH > 0 Then
						sngWorkingMasses(intDataCount) = sngParentIonMH
						sngWorkingMassSignalToNoise(intDataCount) = sngWorkingSNMaximum
						intDataCount += 1

						dblProductSumWorkingMasses += sngWorkingSNMaximum * sngWorkingSNMaximum

					Else
						ReDim Preserve sngWorkingMasses(intDataCount - 1)
					End If

					If mDebugMode Then
						srSpectrumFile.WriteLine()
						srSpectrumFile.WriteLine("MatchIndex" & chTab & _
												 "AminoAcid" & chTab & _
												 "MassLight" & chTab & _
												 "MassHeavy" & chTab & _
												 "MassDifference" & chTab & _
												 "MassError" & chTab & _
												 "SignalToNoiseLight" & chTab & _
												 "SignalToNoiseHeavy" & chTab & _
												 "Charge" & chTab & _
												 "AdjacentSpacingIndexPointer" & chTab & _
												 "SequenceTagLength" & chTab & _
												 "SequenceTagScore" & chTab & _
												 "SequenceTag")
					End If

					If Not objRangeSearch.FillWithData(sngWorkingMasses) Then
						mErrorMessage = "Error calling objRangeSearch.FillWithData in EvaluateMsMsSpectrumMode1 (Filter Mode 1)"

						Try
							Throw New Exception(mErrorMessage)
						Catch ex As Exception
							HandleException(mErrorMessage, ex)
						End Try

					Else
						' Look for points in sngWorkingMasses() that are spaced apart by the standard amino acid masses
						' Note that sngWorkingMasses is sorted ascending

						' The algorithm used is:
						' a) Step through sngWorkingMasses() 
						' b) For each point, look for matching matches in the reverse direction
						' c) If multiple matches are found, keep the match that matches the point with the longest reverse distance

						' Initially reserve space for 20 AASpacing matches; we'll reserve more later if needed
						intAASpacingMatchCount = 0
						ReDim udtAASpacingMatches(19)

						' Reserve space for up to mAminoAcidMassList.Count-1 candidate matching points for each data point
						ReDim udtAASpacingMatchCandidates(mAminoAcidMassList.Count - 1)

						For intIndex = 1 To intDataCount - 1
							' Reset the Match Candidate counter
							intAASpacingMatchCandidateCount = 0

							Dim IEnum As IDictionaryEnumerator = mAminoAcidMassList.GetEnumerator
							Do While IEnum.MoveNext
								sngMassDiffTheoretical = CSng(IEnum.Value)

								If sngWorkingMasses(intIndex) - sngMassDiffTheoretical >= sngWorkingMasses(0) Then
									' Look for peaks that are sngMassDiffTheoretical m/z units below this peak (which assumes each is a 1+ ion)
									blnMatchFound = objRangeSearch.FindValueRange(sngWorkingMasses(intIndex) - sngMassDiffTheoretical, mFilterMode1Options.IonPairMassToleranceHalfWidthDa, intIndexMatch)

									If blnMatchFound Then
										sngMatchingMassDiffTheoretical = sngMassDiffTheoretical
										blnMatchIs2Plus = False
									End If
								Else
									blnMatchFound = False
								End If

								If Not blnMatchFound AndAlso intCharge >= 3 Then
									sngMatchingMassDiffTheoretical = sngMassDiffTheoretical / 2

									If sngWorkingMasses(intIndex) - sngMatchingMassDiffTheoretical >= sngWorkingMasses(0) Then
										' Look for peaks that are sngMassDiffTheoretical/2 m/z units below this peak (which assumes each is a 2+ ion)
										' Note, we do not use the ConvoluteMass()) function to convert sngMassDiffTheoretical from an M+H value to an M+2H value because it is a mass difference, not an absolute mass value
										blnMatchFound = objRangeSearch.FindValueRange(sngWorkingMasses(intIndex) - sngMatchingMassDiffTheoretical, mFilterMode1Options.IonPairMassToleranceHalfWidthDa, intIndexMatch)

										If blnMatchFound Then
											blnMatchIs2Plus = True
										End If
									End If
								End If

								If blnMatchFound Then
									With udtAASpacingMatchCandidates(intAASpacingMatchCandidateCount)
										.AminoAcidSymbol = CChar(IEnum.Key)
										.MassDifference = sngWorkingMasses(intIndex) - sngWorkingMasses(intIndexMatch)
										.MassDifferenceTheoretical = sngMatchingMassDiffTheoretical
										If blnMatchIs2Plus Then
											.ChargeState = 2
										Else
											.ChargeState = 1
										End If

										' Note that intIndex will always be greater than intIndexMatch
										.DataPointIndexHeavy = intIndex
										.DataPointIndexLight = intIndexMatch

										.AdjacentSpacingIndexPointer = -1
										.SequenceTagScore = sngWorkingMassSignalToNoise(intIndex) * sngWorkingMassSignalToNoise(intIndexMatch)
										.SequenceTagLength = 1
									End With
									intAASpacingMatchCandidateCount += 1

									' See if udtAASpacingMatches() contains an entry where the light member of this pair is the heavy member of the comparison pair
									' i.e., look for .DataPointIndexHeavy = intIndexMatch
									For intAASpacingMatchIndex = intAASpacingMatchCount - 1 To 0 Step -1
										If udtAASpacingMatches(intAASpacingMatchIndex).DataPointIndexHeavy = intIndexMatch Then
											' Match found; link the most recent entry in udtAASpacingMatchCandidates() to the one at intAASpacingMatchIndex
											' In addition, bump up the .SequenceTagLength
											With udtAASpacingMatchCandidates(intAASpacingMatchCandidateCount - 1)
												.AdjacentSpacingIndexPointer = intAASpacingMatchIndex
												.SequenceTagLength += udtAASpacingMatches(intAASpacingMatchIndex).SequenceTagLength
												.SequenceTagScore += udtAASpacingMatches(intAASpacingMatchIndex).SequenceTagScore
											End With
											Exit For
										ElseIf udtAASpacingMatches(intAASpacingMatchIndex).DataPointIndexHeavy < intIndexMatch Then
											Exit For
										End If
									Next intAASpacingMatchIndex

									If intAASpacingMatchCandidateCount = udtAASpacingMatchCandidates.Length Then
										' Maximum match count reached; this should never happen because the space in
										'  udtAASpacingMatchCandidates is reserved using mAminoAcidMassList.Count
										Exit Do
									End If
								End If

							Loop

							If intAASpacingMatchCandidateCount > 0 Then
								intBestCandidateIndex = 0
								If intAASpacingMatchCandidateCount > 1 Then
									intBestSequenceTagLength = 0
									dblBestSequenceTagScore = 0

									' Find the best match
									For intAASpacingCandidateIndex = 0 To intAASpacingMatchCandidateCount - 1
										If udtAASpacingMatchCandidates(intAASpacingCandidateIndex).SequenceTagScore > dblBestSequenceTagScore Then
											dblBestSequenceTagScore = udtAASpacingMatchCandidates(intAASpacingCandidateIndex).SequenceTagScore
											intBestSequenceTagLength = udtAASpacingMatchCandidates(intAASpacingCandidateIndex).SequenceTagLength
											intBestCandidateIndex = intAASpacingCandidateIndex
										End If
									Next intAASpacingCandidateIndex

								End If

								' Append the best match to udtAASpacingMatches()
								If intAASpacingMatchCount >= udtAASpacingMatches.Length Then
									ReDim Preserve udtAASpacingMatches(udtAASpacingMatches.Length * 2 - 1)
								End If

								udtAASpacingMatches(intAASpacingMatchCount) = udtAASpacingMatchCandidates(intBestCandidateIndex)
								intAASpacingMatchCount += 1

								If mDebugMode Then
									With udtAASpacingMatches(intAASpacingMatchCount - 1)
										srSpectrumFile.WriteLine(intAASpacingMatchCount - 1 & chTab & _
																 .AminoAcidSymbol & chTab & _
																 sngWorkingMasses(.DataPointIndexLight) & chTab & _
																 sngWorkingMasses(.DataPointIndexHeavy) & chTab & _
																 .MassDifferenceTheoretical & chTab & _
																 (.MassDifference - .MassDifferenceTheoretical) & chTab & _
																 sngWorkingMassSignalToNoise(.DataPointIndexLight) & chTab & _
																 sngWorkingMassSignalToNoise(.DataPointIndexHeavy) & chTab & _
																 .ChargeState & chTab & _
																 .AdjacentSpacingIndexPointer & chTab & _
																 .SequenceTagLength & chTab & _
																 .SequenceTagScore.ToString("0.0") & chTab & _
																 ConstructSequenceTag(udtAASpacingMatches, intAASpacingMatchCount - 1))
									End With
								End If

							End If

						Next intIndex

						' Now process the matches in udtAASpacingMatches
						udtSpectrumQualityScore.IonPairCount = intAASpacingMatchCount
						udtSpectrumQualityScore.SequenceTagLongest = String.Empty

						If intAASpacingMatchCount > 0 Then
							' Compute the percent of the spectrum that is covered by data in udtAASpacingMatches()
							' Since some of the entries in udtAASpacingMatches() will overlap in m/z space, we will use a bit array 
							'  to track, to the nearest 0.5 m/z, which m/z values have an entry in udtAASpacingMatches()

							PopulateBinnedDataUsingAASpacingMatches(udtAASpacingMatches, intAASpacingMatchCount, sngMassList(0), sngMassList(sngMassList.Length - 1), BINNING_PRECISION, sngWorkingMasses, bytBinnedData)

							' Count the number of bins with data in bytBinnedData()
							intBinCountUsed = 0
							For intBinIndex = 0 To bytBinnedData.Length - 1
								If bytBinnedData(intBinIndex) > 0 Then
									intBinCountUsed += 1
								End If
							Next intBinIndex

							' Compute the percentage covered
							udtSpectrumQualityScore.PercentMassSpaceMatched = CSng(intBinCountUsed / bytBinnedData.Length)

							' Find the maximum sequence tag length
							' Additionally, compute the sum of the product of the S/N values for the light and heavy members of each ion pair
							udtSpectrumQualityScore.SequenceTagLengthMax = 0
							intIndexWithLongestSequenceTag = -1

							dblProductSumAAMatches = 0
							For intAASpacingMatchIndex = 0 To intAASpacingMatchCount - 1
								With udtAASpacingMatches(intAASpacingMatchIndex)
									If .SequenceTagLength > udtSpectrumQualityScore.SequenceTagLengthMax Then
										udtSpectrumQualityScore.SequenceTagLengthMax = .SequenceTagLength
										intIndexWithLongestSequenceTag = intAASpacingMatchIndex
									End If

									dblProductSumAAMatches += (sngWorkingMassSignalToNoise(.DataPointIndexLight) * sngWorkingMassSignalToNoise(.DataPointIndexHeavy))
								End With
							Next intAASpacingMatchIndex

							udtSpectrumQualityScore.PercentAbundantPeaksWithMassDiffMatches = CSng(dblProductSumAAMatches / dblProductSumWorkingMasses)
							udtSpectrumQualityScore.AbundantPeaksSumSquares = dblProductSumWorkingMasses

							If intIndexWithLongestSequenceTag >= 0 Then
								udtSpectrumQualityScore.SequenceTagLongest = ConstructSequenceTag(udtAASpacingMatches, intIndexWithLongestSequenceTag)
							End If
						End If

					End If

					' Compute the IonPairCountScore using udtSpectrumQualityScore.IonPairCount
					If udtSpectrumQualityScore.IonPairCount >= intStandardSpacingMaximum Then
						udtSpectrumQualityScore.IonPairCountScore = 1
					ElseIf udtSpectrumQualityScore.IonPairCount <= intStandardSpacingMinimum Then
						udtSpectrumQualityScore.IonPairCountScore = 0
					Else
						' udtSpectrumQualityScore.IonPairCount is between intStandardSpacingMinimum and intStandardSpacingMaximum
						udtSpectrumQualityScore.IonPairCountScore = CSng((udtSpectrumQualityScore.IonPairCount - intStandardSpacingMinimum) / (intStandardSpacingMaximum - intStandardSpacingMinimum))
					End If

					If mDebugMode Then
						srSpectrumFile.WriteLine()
						srSpectrumFile.WriteLine("FilterMode1 Score Summary")

						With udtSpectrumQualityScore
							srSpectrumFile.WriteLine("IonPair_StandardSpacingMinimum" & chTab & intStandardSpacingMinimum)
							srSpectrumFile.WriteLine("IonPair_StandardSpacingMaximum" & chTab & intStandardSpacingMaximum)
							srSpectrumFile.WriteLine("IonPair_Count" & chTab & .IonPairCount)
							srSpectrumFile.WriteLine("IonPair_Score" & chTab & .IonPairCountScore)
							srSpectrumFile.WriteLine("MassSpaceBinCount" & chTab & bytBinnedData.Length)
							srSpectrumFile.WriteLine("PercentMassSpaceMatched" & chTab & .PercentMassSpaceMatched)
							srSpectrumFile.WriteLine("PercentAbundantPeaksWithMassDiffMatches" & chTab & .PercentAbundantPeaksWithMassDiffMatches)
							srSpectrumFile.WriteLine("SequenceTagLengthMax" & chTab & .SequenceTagLengthMax)
							srSpectrumFile.WriteLine("AbundantPeaksSumSquares" & chTab & .AbundantPeaksSumSquares)
							srSpectrumFile.WriteLine("SequenceTag" & chTab & .SequenceTagLongest)

						End With

						srSpectrumFile.WriteLine()

						srSpectrumFile.Close()
					End If

				End If
			End If

		Catch ex As Exception
			If mErrorMessage Is Nothing OrElse mErrorMessage.Length = 0 Then
				mErrorMessage = "Error in EvaluateMsMsSpectrumMode1: " & ex.Message
			End If
			HandleException(mErrorMessage, ex)
			udtSpectrumQualityScore.SpectrumQualityScore = 0
		End Try

		objRangeSearch = Nothing

		With udtSpectrumQualityScore
			' Copy the value for PercentAbundantPeaksWithMassDiffMatches to .SpectrumQualityScore
			.SpectrumQualityScore = .PercentAbundantPeaksWithMassDiffMatches
		End With

		Return udtSpectrumQualityScore

	End Function

	' Filter Mode 2 is based on an algorithm developed by Sam Purvine - eSpectrumFilterMode.mode2
	' It filters out spectra that do not contain a reasonable number of peaks above a S/N threshold
	Public Function EvaluateMsMsSpectrumMode2(ByVal sngMassList() As Single, ByVal sngIntensityList() As Single, ByVal dblParentMZ As Double, ByVal intCharge As Integer, ByRef sngBPI As Single) As Single

		Dim udtScoreDetails As udtMode2ScoreDetailsType
		Dim dblTIC As Double

		If sngIntensityList Is Nothing OrElse sngIntensityList.Length <= 0 Then
			udtScoreDetails.QualityScore = 0
		Else
			' Compute the BPI and TIC

			ComputeBPIAndTIC(sngIntensityList, sngBPI, dblTIC)

			udtScoreDetails = EvaluateMsMsSpectrumMode2(sngMassList, sngIntensityList, dblParentMZ, intCharge, sngBPI, dblTIC)
		End If

		Return udtScoreDetails.QualityScore

	End Function

	Protected Function EvaluateMsMsSpectrumMode2(ByVal sngMassList() As Single, ByVal sngIntensityList() As Single, ByVal dblParentMZ As Double, ByVal intCharge As Integer, ByVal sngBPI As Single, ByVal dblTIC As Double) As udtMode2ScoreDetailsType
		' This function assumes that sngBPI and dblTIC have already been populated
		' It also assumes that the data is sorted by ascending mass

		Dim intIndex As Integer
		Dim intCount As Integer

		Dim udtScoreDetails As udtMode2ScoreDetailsType

		Dim sngSortedIntensityList() As Single

		Const TIC_SCORE_INTERVAL As Single = 0.25
		Const SN_SCORE_INTERVAL As Single = 0.25

		Dim sngMZThreshold As Single
		Dim dblSum As Double

		Dim intSNIteration As Integer
		Dim sngSNThreshold As Single
		Dim intPeakCountThreshold As Integer

		Dim blnPassThreshold As Boolean
		Dim sngSNScore As Single
		Dim sngSNScorePrevious As Single

		Try

			' Compute an intensity threshold for looking for signficant peaks that weigh more than the parent ion m/z value
			' By default, this threshold is 5% of the base peak intensity
			udtScoreDetails.IntensityThresholdForSignificantPeaks = CSng(sngBPI * mFilterMode2Options.SignificantIntensityFractionBasePeak)

			' Step 1
			' Count the number of peaks with mass > dblParentMZ + 2 and intensity > udtScoreDetails.IntensityThreshold
			udtScoreDetails.SignificantPeakNumberCount = 0
			sngMZThreshold = CSng(dblParentMZ + 2)

			For intIndex = sngIntensityList.Length - 1 To 0 Step -1
				If sngMassList(intIndex) > sngMZThreshold Then
					If sngIntensityList(intIndex) >= udtScoreDetails.IntensityThresholdForSignificantPeaks Then
						udtScoreDetails.SignificantPeakNumberCount += 1
					End If
				Else
					Exit For
				End If
			Next

			udtScoreDetails.QualityScore = 0.0
			udtScoreDetails.SignificantPeakCountIsValidForChargeState = False

			' Proceed only if udtScoreDetails.SignificantPeakNumberCount passes the threshold rules
			If intCharge > 1 Then
				' Charge 2+, 3+, etc.
				' Require that udtScoreDetails.SignificantPeakNumberCount be larger than a threshold
				If udtScoreDetails.SignificantPeakNumberCount > mFilterMode2Options.Charge2SignificantPeakNumberThreshold Then
					udtScoreDetails.SignificantPeakCountIsValidForChargeState = True
				End If
			Else
				' Charge 1+
				' Require that udtScoreDetails.SignificantPeakNumberCount be less than a threshold
				If udtScoreDetails.SignificantPeakNumberCount < mFilterMode2Options.Charge1SignificantPeakNumberThreshold Then
					udtScoreDetails.SignificantPeakCountIsValidForChargeState = True
				End If
			End If

			If udtScoreDetails.SignificantPeakCountIsValidForChargeState Then
				' Step 2
				' Set udtScoreDetails.QualityScore to TIC_SCORE_INTERVAL if the TIC of this spectrum is > .TICScoreThreshold
				' Otherwise, set to 0
				If dblTIC >= (mFilterMode2Options.TICScoreThreshold) Then			' Originally 2E6; changed to 2E5 in February 2008
					udtScoreDetails.TICScore = TIC_SCORE_INTERVAL
				ElseIf mFilterMode2Options.ComputeFractionalScores AndAlso mFilterMode2Options.TICScoreThreshold > 0 Then
					' Assign a fractional TIC score
					udtScoreDetails.TICScore = CSng(TIC_SCORE_INTERVAL * dblTIC / mFilterMode2Options.TICScoreThreshold)
				Else
					udtScoreDetails.TICScore = 0
				End If

				udtScoreDetails.QualityScore += udtScoreDetails.TICScore

				' Duplicate sngIntensityList and then sort ascending
				ReDim sngSortedIntensityList(sngIntensityList.Length - 1)
				Array.Copy(sngIntensityList, sngSortedIntensityList, sngIntensityList.Length)

				Array.Sort(sngSortedIntensityList)

				' Step3
				' Compute the average intensity value of the low intensity data in this spectrum
				' .NoiseThresholdFraction defines the fraction of the low intensity data to use (defaults to 50%)

				intCount = 0
				udtScoreDetails.NoiseLevel = 0.0
				dblSum = 0
				For intIndex = 0 To CInt(sngSortedIntensityList.Length * mFilterMode2Options.NoiseThresholdFraction)
					dblSum += sngSortedIntensityList(intIndex)
					intCount += 1
				Next

				If intCount = 0 Then
					udtScoreDetails.NoiseLevel = 1
				Else
					udtScoreDetails.NoiseLevel = CSng(dblSum / intCount)
					If udtScoreDetails.NoiseLevel < 1 Then
						udtScoreDetails.NoiseLevel = 1
					End If
				End If

				' Step 4
				' See if this spectrum passes the each of the three S/N filters (high, moderate, low)
				' Bump udtScoreDetails.QualityScore up by the appropriate amount depending on the results from CountPointsPassingSN
				With mFilterMode2Options

					sngSNScorePrevious = 0
					For intSNIteration = 3 To 1 Step -1
						If intSNIteration = 3 Then
							sngSNThreshold = .HighQualitySNThreshold
							intPeakCountThreshold = .HighQualitySNPeakCount
						ElseIf intSNIteration = 2 Then
							sngSNThreshold = .ModerateQualitySNThreshold
							intPeakCountThreshold = .ModerateQualitySNPeakCount
						Else
							sngSNThreshold = .LowQualitySNThreshold
							intPeakCountThreshold = .LowQualitySNPeakCount
						End If

						blnPassThreshold = CountPointsPassingSN(sngSortedIntensityList, CSng(udtScoreDetails.NoiseLevel), sngSNThreshold, intPeakCountThreshold, SN_SCORE_INTERVAL, sngSNScore)

						If intSNIteration = 3 Then
							udtScoreDetails.HighQualitySNThresholdScore = sngSNScore
						ElseIf intSNIteration = 2 Then
							udtScoreDetails.ModerateQualitySNThresholdScore = sngSNScore
						Else
							udtScoreDetails.LowQualitySNThresholdScore = sngSNScore
						End If

						If blnPassThreshold Then
							udtScoreDetails.QualityScore += SN_SCORE_INTERVAL * intSNIteration
							If .ComputeFractionalScores Then
								udtScoreDetails.QualityScore += sngSNScorePrevious
							End If
							Exit For
						Else
							If intSNIteration = 1 Then
								If .ComputeFractionalScores AndAlso sngSNScore > 0 Then
									udtScoreDetails.QualityScore += sngSNScore
								End If
								Exit For
							Else
								sngSNScorePrevious = sngSNScore
							End If
						End If

					Next intSNIteration
				End With

			End If


		Catch ex As Exception
			If mErrorMessage Is Nothing OrElse mErrorMessage.Length = 0 Then
				mErrorMessage = "Error in EvaluateMsMsSpectrumMode2: " & ex.Message
			End If
			HandleException(mErrorMessage, ex)
		End Try

		Return udtScoreDetails

	End Function

	' Filter Mode 3 is based on an algorithm developed by Eric Strittmatter at PNNL - eSpectrumFilterMode.mode3
	' The aim of the filter is to only select spectra that are likely to be from phosphorylated peptides
	' Returns True if the spectrum passes the filter, otherwise returns false
	' Returns the quality score as parameter sngSpectrumQualityScore
	Public Function EvaluateMsMsSpectrumMode3(ByVal sngMassList() As Single, ByVal sngIntensityList() As Single, _
											  ByVal dblParentMZ As Double, _
											  ByVal intAssumedChargeState As Integer, _
											  ByRef udtIonMatchStats As udtIonMatchStatsType, _
											  ByRef sngSpectrumQualityScore As Single) As Boolean

		Dim intIndex As Integer

		Dim dblTIC As Double
		Dim dblNLAbundanceThreshold As Double

		Dim blnPassesFilter As Boolean

		' The neutral loss object is static to avoid reserving memory for every spectrum
		' However, it will be re-populated on each call to this function
		Static objNeutralLossMasses As System.Collections.Generic.List(Of SearchMassSpecsClass)

		Try
			' Initially set sngSpectrumQualityScore such that its score value is less than the minimum
			If mMinimumQualityScore > 0 Then
				sngSpectrumQualityScore = 0
			Else
				sngSpectrumQualityScore = mMinimumQualityScore - 1
			End If

			If objNeutralLossMasses Is Nothing Then
				objNeutralLossMasses = New System.Collections.Generic.List(Of SearchMassSpecsClass)
			End If

			' Clear udtIonMatchStats
			udtIonMatchStats.Clear()

			' Determine the base peak intensity (maximum intensity in the spectrum)
			ComputeBPIAndTIC(sngIntensityList, udtIonMatchStats.BPI, dblTIC)

			If udtIonMatchStats.BPI < mFilterMode3Options.BasePeakIntensityMinimum Then
				Return False
			End If

			dblNLAbundanceThreshold = udtIonMatchStats.BPI * mFilterMode3Options.NLAbundanceThresholdFractionMax

			If mFilterMode3Options.SpecificMZLosses Is Nothing Then
				mFilterMode3Options.SpecificMZLosses = String.Empty
			End If

			objNeutralLossMasses.Clear()
			If mFilterMode3Options.SpecificMZLosses.Length > 0 Then
				' Searching for specific mass losses
				Dim dblValues As New System.Collections.Generic.List(Of Double)
				If ParseDelimitedIntegerList(mFilterMode3Options.SpecificMZLosses, dblValues) Then
					For Each dblValue As Double In dblValues
						AppendToSearchMZList(objNeutralLossMasses, dblValue, SearchMassSpecsClass.eSearchMassCodeConstants.CustomMass)
					Next
				End If

			Else
				' Always check for loss of 98, regardless of charge state of the parent ion
				' For 1+ spectra, 98 is the only possibly loss (phosphorylation)
				' For 2+ and 3+ spectra, could lose 98 if multiple phosphorylation sites are present
				AppendToSearchMZList(objNeutralLossMasses, MASS_PHOSPHORYLATION, SearchMassSpecsClass.eSearchMassCodeConstants.NL98)

				If Not mFilterMode3Options.LimitToChargeSpecificIons OrElse intAssumedChargeState = 0 OrElse intAssumedChargeState = 1 Then
					' 1+ spectrum
					If mFilterMode3Options.ConsiderWaterLoss Then
						' Check for loss of 116 (which is 98+18)
						AppendToSearchMZList(objNeutralLossMasses, MASS_PHOSPHORYLATION + MASS_WATER, SearchMassSpecsClass.eSearchMassCodeConstants.NL116)
					End If
				End If

				If Not mFilterMode3Options.LimitToChargeSpecificIons OrElse intAssumedChargeState = 0 OrElse intAssumedChargeState = 2 Then
					' 2+ spectrum
					' Check for loss of 49 (which is 98/2)
					AppendToSearchMZList(objNeutralLossMasses, MASS_PHOSPHORYLATION / 2.0#, SearchMassSpecsClass.eSearchMassCodeConstants.NL49)

					If mFilterMode3Options.ConsiderWaterLoss Then
						' Check for loss of 58 (which is (98+18)/2)
						AppendToSearchMZList(objNeutralLossMasses, (MASS_PHOSPHORYLATION + MASS_WATER) / 2.0#, SearchMassSpecsClass.eSearchMassCodeConstants.NL58)

						' Check for loss of 107 (which is (98*2+18)/2)
						AppendToSearchMZList(objNeutralLossMasses, (MASS_PHOSPHORYLATION * 2.0# + MASS_WATER) / 2.0#, SearchMassSpecsClass.eSearchMassCodeConstants.NL107)
					End If
				End If

				If Not mFilterMode3Options.LimitToChargeSpecificIons OrElse intAssumedChargeState = 0 OrElse intAssumedChargeState >= 3 Then
					' 3+ spectrum
					' Check for loss of 32.66 (which is 98/3)
					AppendToSearchMZList(objNeutralLossMasses, MASS_PHOSPHORYLATION / 3.0#, SearchMassSpecsClass.eSearchMassCodeConstants.NL33)

					' Check for loss of loss of 65.32 (which is (98*2)/3)
					AppendToSearchMZList(objNeutralLossMasses, (MASS_PHOSPHORYLATION * 2.0#) / 3.0#, SearchMassSpecsClass.eSearchMassCodeConstants.NL65)

					If mFilterMode3Options.ConsiderWaterLoss Then
						' Check for loss of 38.66 (which is (98+18)/3)
						AppendToSearchMZList(objNeutralLossMasses, (MASS_PHOSPHORYLATION + MASS_WATER) / 3.0#, SearchMassSpecsClass.eSearchMassCodeConstants.NL39)

						' Check for loss of 71.32 (which is (98*2+18)/3)
						AppendToSearchMZList(objNeutralLossMasses, (MASS_PHOSPHORYLATION * 2.0# + MASS_WATER) / 3.0#, SearchMassSpecsClass.eSearchMassCodeConstants.NL71)
					End If
				End If

			End If

			' Set up search bounds for the items in objNeutralLossMasses
			' These bounds are dependent on dblParentMZ so they need to be intialized for every spectrum processed
			For intIndex = 0 To objNeutralLossMasses.Count - 1
				With objNeutralLossMasses(intIndex)
					.LowerBoundMZ = dblParentMZ - .SearchMass - mFilterMode3Options.MassToleranceHalfWidthMZ
					.UpperBoundMZ = dblParentMZ - .SearchMass + mFilterMode3Options.MassToleranceHalfWidthMZ
				End With
			Next

		Catch ex As Exception
			If mErrorMessage Is Nothing OrElse mErrorMessage.Length = 0 Then
				mErrorMessage = "Error in EvaluateMsMsSpectrumMode3 (loc 1): " & ex.Message
			End If
			HandleException(mErrorMessage, ex)
		End Try

		Try
			blnPassesFilter = SearchSpectrumForIons(sngMassList, sngIntensityList, _
													objNeutralLossMasses, dblNLAbundanceThreshold, _
													mIncludeNLStatsOnFilterReport, udtIonMatchStats)

		Catch ex As Exception
			If mErrorMessage Is Nothing OrElse mErrorMessage.Length = 0 Then
				mErrorMessage = "Error in EvaluateMsMsSpectrumMode3 (loc 2): " & ex.Message
			End If
			HandleException(mErrorMessage, ex)
		End Try

		If blnPassesFilter Then
			' Spectrum passed filter; guarantee that its score value is greater than the minimum
			' We'll set the score to 1 if mMinimumQualityScore is <= 1 or set it to mMinimumQualityScore+1 if mMinimumQualityScore is > 1
			If mMinimumQualityScore <= 1 Then
				sngSpectrumQualityScore = 1
			Else
				sngSpectrumQualityScore = mMinimumQualityScore + 1
			End If
		End If

		Return blnPassesFilter

	End Function

	''' <summary>
	''' Looks for the ions specified by mFilterMode4Options.ReporterIonMZs
	''' Updates the parent ion information in udtSpectrumHeaderInfo using mFilterMode4Options.PrecursorIonMassLossDa
	''' </summary>
	''' <param name="sngMassList"></param>
	''' <param name="sngIntensityList"></param>
	''' <param name="udtSpectrumHeaderInfo"></param>
	''' <param name="udtIonMatchStats"></param>
	''' <param name="sngSpectrumQualityScore"></param>
	''' <returns></returns>
	''' <remarks></remarks>
	Protected Function EvaluateMsMsSpectrumMode4(ByVal sngMassList() As Single, ByVal sngIntensityList() As Single, _
												 ByRef udtSpectrumHeaderInfo As MsMsDataFileReader.clsMsMsDataFileReaderBaseClass.udtSpectrumHeaderInfoType, _
												 ByRef udtIonMatchStats As udtIonMatchStatsType, _
												 ByRef sngSpectrumQualityScore As Single) As Boolean

		Dim intMassIndex As Integer

		Dim dblTIC As Double

		Dim blnPassesFilter As Boolean
		Dim intReporterIonMatchCount As Integer
		Dim dblReporterIonMaxIntensity As Double


		' The reporter ion mass list is static to avoid reserving memory for every spectrum
		' However, it will be re-populated on each call to this function
		Static objReporterIonMasses As System.Collections.Generic.List(Of SearchMassSpecsClass)

		Try
			' Initially set sngSpectrumQualityScore such that its score value is less than the minimum
			If mMinimumQualityScore > 0 Then
				sngSpectrumQualityScore = 0
			Else
				sngSpectrumQualityScore = mMinimumQualityScore - 1
			End If

			If objReporterIonMasses Is Nothing Then
				objReporterIonMasses = New System.Collections.Generic.List(Of SearchMassSpecsClass)
			End If

			' Clear udtIonMatchStats
			udtIonMatchStats.Clear()

			' Determine the base peak intensity (maximum intensity in the spectrum)
			ComputeBPIAndTIC(sngIntensityList, udtIonMatchStats.BPI, dblTIC)

			If mFilterMode4Options.ReporterIonMZs Is Nothing Then
				mFilterMode4Options.ReporterIonMZs = String.Empty
			End If


			Dim dblReporterIonMZs As New System.Collections.Generic.List(Of Double)

			objReporterIonMasses.Clear()
			If GetFilterMode4ReporterIons(dblReporterIonMZs) Then
				For Each dblValue As Double In dblReporterIonMZs
					AppendToSearchMZList(objReporterIonMasses, dblValue, mFilterMode4Options.ReporterIonToleranceHalfWidthMZ, SearchMassSpecsClass.eSearchMassCodeConstants.CustomMass)
				Next
			End If

		Catch ex As Exception
			If mErrorMessage Is Nothing OrElse mErrorMessage.Length = 0 Then
				mErrorMessage = "Error in EvaluateMsMsSpectrumMode4 (loc 1): " & ex.Message
			End If
			HandleException(mErrorMessage, ex)
		End Try

		Try
			Dim blnMatchFound As Boolean
			blnPassesFilter = False

			' Note: need to set blnPopulateIonIntensitiesNormalized to true when calling SearchSpectrumForIons since we need to find all of the matching ions
			blnMatchFound = SearchSpectrumForIons(sngMassList, sngIntensityList, _
												  objReporterIonMasses, mFilterMode4Options.ReporterIonMinimumIntensity, _
												  True, udtIonMatchStats)

			If blnMatchFound Then
				' Count the number of matches
				intReporterIonMatchCount = 0
				dblReporterIonMaxIntensity = 0

				For intMassIndex = 0 To objReporterIonMasses.Count - 1
					If objReporterIonMasses(intMassIndex).Matched Then
						intReporterIonMatchCount += 1
						If objReporterIonMasses(intMassIndex).IonIntensity > dblReporterIonMaxIntensity Then
							dblReporterIonMaxIntensity = objReporterIonMasses(intMassIndex).IonIntensity
						End If
					End If
				Next

				If intReporterIonMatchCount >= mFilterMode4Options.ReporterIonMatchCountMinimum AndAlso _
				   dblReporterIonMaxIntensity >= mFilterMode4Options.ReporterIonMaxIntensityThreshold Then
					blnPassesFilter = True
				End If

			End If

		Catch ex As Exception
			If mErrorMessage Is Nothing OrElse mErrorMessage.Length = 0 Then
				mErrorMessage = "Error in EvaluateMsMsSpectrumMode4 (loc 2): " & ex.Message
			End If
			HandleException(mErrorMessage, ex)
		End Try

		If blnPassesFilter Then
			If mFilterMode4Options.PrecursorIonMinimumCharge > 0 Then
				' Make sure the precursor charge state is high enough
				blnPassesFilter = False
				If udtSpectrumHeaderInfo.ParentIonChargeCount >= 1 Then

					For intChargeIndex As Integer = 0 To udtSpectrumHeaderInfo.ParentIonChargeCount - 1
						If udtSpectrumHeaderInfo.ParentIonCharges(intChargeIndex) >= mFilterMode4Options.PrecursorIonMinimumCharge Then
							blnPassesFilter = True
							Exit For
						End If
					Next

				End If
			End If
		End If

		If blnPassesFilter Then

			If mFilterMode4Options.PrecursorIonMassLossDa <> 0 Then
				' Update the parent ion mass

				' Need to update these values:
				'   udtSpectrumHeaderInfo.ParentIonLineText = 
				'   udtSpectrumHeaderInfo.ParentIonMH =
				'   udtSpectrumHeaderInfo.ParentIonMZ =

				udtSpectrumHeaderInfo.ParentIonMH = udtSpectrumHeaderInfo.ParentIonMH - mFilterMode4Options.PrecursorIonMassLossDa

				If udtSpectrumHeaderInfo.ParentIonMH < 10 Then
					' Do not allow the parent ion MH to fall below 10 Da (which is, obviously, already tiny)
					udtSpectrumHeaderInfo.ParentIonMH = 10
				End If

				If udtSpectrumHeaderInfo.ParentIonChargeCount >= 1 Then
					udtSpectrumHeaderInfo.ParentIonMZ = CSng(ConvoluteMass(udtSpectrumHeaderInfo.ParentIonMH, 1, udtSpectrumHeaderInfo.ParentIonCharges(0)))
					udtSpectrumHeaderInfo.ParentIonLineText = udtSpectrumHeaderInfo.ParentIonMH.ToString() & " " & udtSpectrumHeaderInfo.ParentIonCharges(0).ToString()
				Else
					' No charges defined; will have to assume 1+
					udtSpectrumHeaderInfo.ParentIonMZ = udtSpectrumHeaderInfo.ParentIonMH
					udtSpectrumHeaderInfo.ParentIonLineText = udtSpectrumHeaderInfo.ParentIonMH.ToString() & " 1"
				End If

			End If


			' Spectrum passed filter; guarantee that its score value is greater than the minimum
			' We'll set the score to 1 if mMinimumQualityScore is <= 1 or set it to mMinimumQualityScore+1 if mMinimumQualityScore is > 1
			If mMinimumQualityScore <= 1 Then
				sngSpectrumQualityScore = 1
			Else
				sngSpectrumQualityScore = mMinimumQualityScore + 1
			End If
		End If

		Return blnPassesFilter

	End Function

	Private Function SearchSpectrumForIons(ByVal sngMassList() As Single, ByVal sngIntensityList() As Single, _
										   ByRef objSearchMassSpecs As System.Collections.Generic.List(Of SearchMassSpecsClass), _
										   ByVal dblAbundanceThreshold As Double, _
										   ByVal blnPopulateIonIntensitiesNormalized As Boolean, _
										   ByRef udtIonMatchStats As udtIonMatchStatsType) As Boolean

		Dim intIonIndex As Integer
		Dim intMassIndex As Integer

		Dim sngNewPercentage As Single

		Dim blnMatchFound As Boolean

		If udtIonMatchStats.IonIntensitiesNormalized Is Nothing Then
			ReDim udtIonMatchStats.IonIntensitiesNormalized(objSearchMassSpecs.Count - 1)
		ElseIf udtIonMatchStats.IonIntensitiesNormalized.Length < objSearchMassSpecs.Count Then
			ReDim Preserve udtIonMatchStats.IonIntensitiesNormalized(objSearchMassSpecs.Count - 1)
		End If

		blnMatchFound = False
		For intIonIndex = 0 To sngMassList.Length - 1

			' Check whether any of the masses in sngMassList matches the values in objSearchMassSpecs
			For intMassIndex = 0 To objSearchMassSpecs.Count - 1

				If sngMassList(intIonIndex) >= objSearchMassSpecs(intMassIndex).LowerBoundMZ AndAlso _
				   sngMassList(intIonIndex) <= objSearchMassSpecs(intMassIndex).UpperBoundMZ Then

					If sngIntensityList(intIonIndex) >= dblAbundanceThreshold Then
						' Match found

						objSearchMassSpecs(intMassIndex).Matched = True
						If sngIntensityList(intIonIndex) > objSearchMassSpecs(intMassIndex).IonIntensity Then
							objSearchMassSpecs(intMassIndex).IonIntensity = sngIntensityList(intIonIndex)
						End If

						blnMatchFound = True

						If blnPopulateIonIntensitiesNormalized Then
							If udtIonMatchStats.BPI > 0 Then
								' Compute the normalized intensity for this match
								' Update the value in udtIonMatchStats.IonIntensitiesNormalized() if larger than the stored value
								sngNewPercentage = sngIntensityList(intIonIndex) / udtIonMatchStats.BPI * 100
								If sngNewPercentage > udtIonMatchStats.IonIntensitiesNormalized(intMassIndex) Then
									udtIonMatchStats.IonIntensitiesNormalized(intMassIndex) = sngNewPercentage
								End If
							End If
						Else
							' Not populating the IonIntensitiesNormalized values in udtIonMatchStats
							' We can exit the loop now to speed up processing time
							Exit For
						End If
					End If
				End If
			Next intMassIndex

			If blnMatchFound AndAlso Not blnPopulateIonIntensitiesNormalized Then
				Exit For
			End If
		Next intIonIndex

		Return blnMatchFound
	End Function


	''' <summary>
	''' Looks for ions that are within certain m/z ranges
	''' Will remove the ions if blnRemoveIons = True; otherwise, changes their intensity to a value between 0 and 1
	''' </summary>
	''' <param name="intDataCount">Number of data points in sngMassList</param>
	''' <param name="sngMassList">Array of m/z values</param>
	''' <param name="sngIntensityList">Array of intensity values</param>
	''' <param name="udtSpectrumHeaderInfo">Spectrum Header Info</param>
	''' <param name="blnRemoveIons">True to remove ions; False to change their intensity to between 0 and 1</param>
	''' <returns>True if the data is updated.</returns>
	''' <remarks></remarks>
	Protected Function FilterIonsByMZ(ByRef intDataCount As Integer, _
									  ByRef sngMassList() As Single, _
									  ByRef sngIntensityList() As Single, _
									  ByRef udtSpectrumHeaderInfo As MsMsDataFileReader.clsMsMsDataFileReaderBaseClass.udtSpectrumHeaderInfoType, _
									  ByVal blnRemoveIons As Boolean, _
									  ByRef intPositiveDataCountBeforeFilter As Integer, _
									  ByRef intPositiveDataCountAfterFilter As Integer) As Boolean

		Dim intIndex As Integer
		Dim intTargetIndex As Integer

		Dim intParentCharge As Integer
		Dim intChargeIndex As Integer

		Dim blnKeepIon As Boolean
		Dim blnSpectralDataUpdated As Boolean

		Dim dblParentIonMZ As Double
		Dim dblParentIonMonoMass As Double

		Dim dblnCurrentParentExclusionMZ As Double

		Dim objCharges As New Generic.List(Of Integer)

		Dim blnFilterReporterIons As Boolean
		Dim dblReporterIonMZs As New System.Collections.Generic.List(Of Double)
		Dim dblParentIonMZNextLowerCharge As Double

		blnSpectralDataUpdated = False
		intPositiveDataCountBeforeFilter = 0
		intPositiveDataCountAfterFilter = 0

		Try
			' Count the number of positive intensity values
			intPositiveDataCountBeforeFilter = 0
			For intIndex = 0 To intDataCount - 1
				If sngIntensityList(intIndex) > 0 Then
					intPositiveDataCountBeforeFilter += 1
				End If
			Next intIndex

			If Not CheckIonFilteringEnabled() Then
				' Nothing to filter
				blnSpectralDataUpdated = False
				intPositiveDataCountAfterFilter = intPositiveDataCountBeforeFilter
			Else
				' The following is equivalent to: ConvoluteMass(udtSpectrumHeaderInfo.ParentIonMH, 1, 0)
				dblParentIonMonoMass = udtSpectrumHeaderInfo.ParentIonMH - MASS_PROTON
				dblParentIonMZ = udtSpectrumHeaderInfo.ParentIonMZ

				For intChargeIndex = 0 To udtSpectrumHeaderInfo.ParentIonChargeCount - 1
					objCharges.Add(udtSpectrumHeaderInfo.ParentIonCharges(intChargeIndex))
				Next

				If udtSpectrumHeaderInfo.ChargeIs2And3Plus Then
					If Not objCharges.Contains(2) Then
						objCharges.Add(2)
					End If

					If Not objCharges.Contains(3) Then
						objCharges.Add(3)
					End If
				End If

				If objCharges.Count = 0 Then
					' This is unexpected
					' Assume the charge state is 2 or 3
					objCharges.Add(2)
					objCharges.Add(3)
				End If

				If mSpectrumFilterMode = eSpectrumFilterMode.mode4 Then
					If mFilterMode4Options.RemoveReporterIons Then
						blnFilterReporterIons = GetFilterMode4ReporterIons(dblReporterIonMZs)
					Else
						blnFilterReporterIons = False
					End If

				End If

				' Step through the data
				' Check whether any points should be removed
				' If we keep them, then copy from intIndex to intTargetIndex and increment intTargetIndex
				' Otherwise, don't increment intTargetIndex (meaning the data point will get replaced on a subsequent copy)

				For Each intParentCharge In objCharges


					If mSpectrumFilterMode = eSpectrumFilterMode.mode4 Then
						' If the charge is > 1, then convolute the charge down one and remove the resultant peak
						If intParentCharge > 1 Then
							dblParentIonMZNextLowerCharge = ConvoluteMass(dblParentIonMonoMass, 0, intParentCharge - 1)
						End If
					End If

					intTargetIndex = 0
					For intIndex = 0 To intDataCount - 1

						blnKeepIon = True

						If mIonFilterOptions.RemovePrecursor OrElse _
						   (mSpectrumFilterMode = eSpectrumFilterMode.mode4 AndAlso mFilterMode4Options.RemoveMassCorrectedPrecursorIons) Then

							If Math.Abs(dblParentIonMZ - sngMassList(intIndex)) <= mIonFilterOptions.PrecursorCleaningToleranceMZ Then
								blnKeepIon = False
							End If
						End If

						If blnKeepIon AndAlso (mIonFilterOptions.RemoveChargeReducedPrecursors OrElse mIonFilterOptions.RemoveNeutralLossesFromChargeReducedPrecursors) Then

							' Note: in the Coon lab's code, they use intParentCharge-1, which means you won't filter out neutral losses from the parent
							' I have updated the code to filter out neutral losses from the parent m/z and from the charge reduced m/z values
							For intChargeIndex = 1 To intParentCharge
								dblnCurrentParentExclusionMZ = ConvoluteMass(dblParentIonMonoMass, 0, intChargeIndex)

								If mIonFilterOptions.RemoveChargeReducedPrecursors Then
									If Math.Abs(dblnCurrentParentExclusionMZ - sngMassList(intIndex)) <= mIonFilterOptions.PrecursorCleaningToleranceMZ Then
										blnKeepIon = False
									End If
								End If

								If blnKeepIon AndAlso mIonFilterOptions.RemoveNeutralLossesFromChargeReducedPrecursors Then
									If sngMassList(intIndex) >= ConvoluteMass(dblParentIonMonoMass - mIonFilterOptions.NeutralLossCleaningWindowDa, 0, intChargeIndex) AndAlso _
									   sngMassList(intIndex) <= dblnCurrentParentExclusionMZ + mIonFilterOptions.PrecursorCleaningToleranceMZ Then
										blnKeepIon = False
									End If
								End If
							Next
						End If

						If mSpectrumFilterMode = eSpectrumFilterMode.mode4 Then

							If blnKeepIon AndAlso blnFilterReporterIons Then
								' Remove the reporter ion masses
								For Each dblValue As Double In dblReporterIonMZs
									If Math.Abs(dblValue - sngMassList(intIndex)) <= mFilterMode4Options.ReporterIonToleranceHalfWidthMZ Then
										blnKeepIon = False
										Exit For
									End If
								Next

							End If

							If blnKeepIon AndAlso intParentCharge > 1 AndAlso mFilterMode4Options.RemoveMassCorrectedPrecursorIons Then
								If Math.Abs(dblParentIonMZNextLowerCharge - sngMassList(intIndex)) <= mFilterMode4Options.ReporterIonToleranceHalfWidthMZ Then
									blnKeepIon = False
								End If
							End If

						End If

						If blnKeepIon Then
							If intTargetIndex <> intIndex Then
								sngMassList(intTargetIndex) = sngMassList(intIndex)
								sngIntensityList(intTargetIndex) = sngIntensityList(intIndex)
							End If
							intTargetIndex += 1
						Else
							blnSpectralDataUpdated = True

							If Not blnRemoveIons Then
								' Instead of removing the ion, change its intensity to 0

								' The following could be used to change the intensity to a small number but not 0
								''If sngIntensityList(intTargetIndex) > 0 Then
								''    ' First divide the intensity by 10000
								''    sngIntensityList(intTargetIndex) /= 10000

								''    ' If the intensity is still 1 or larger, then divide by 10 until it drops below one
								''    Do While sngIntensityList(intTargetIndex) >= 1
								''        sngIntensityList(intTargetIndex) /= 10
								''    Loop
								''End If

								sngIntensityList(intTargetIndex) = 0
								intTargetIndex += 1
							End If
						End If

					Next intIndex

					' Update intDataCount
					If intDataCount > intTargetIndex Then
						intDataCount = intTargetIndex
					End If
				Next

				' Count the number of positive intensity values now that the filter has been applied
				intPositiveDataCountAfterFilter = 0
				For intIndex = 0 To intDataCount - 1
					If sngIntensityList(intIndex) > 0 Then
						intPositiveDataCountAfterFilter += 1
					End If
				Next intIndex

			End If

		Catch ex As Exception
			Throw New Exception("Error in FilterIonsByMZ: " & ex.Message, ex)
		End Try

		Return blnSpectralDataUpdated

	End Function

	Protected Function FilterModeOptionBoolToSng(ByVal blnOption As Boolean) As Single
		If blnOption Then
			Return -1
		Else
			Return 0
		End If
	End Function

	Protected Function FilterModeOptionSngToBool(ByVal sngValue As Single) As Boolean
		If sngValue <> 0 Then
			Return True
		Else
			Return False
		End If
	End Function

	Protected Function FlattenArray(ByVal strArray() As String, ByVal chDelimiter As Char) As String
		Try
			Return FlattenArray(strArray, 0, strArray.Length - 1, chDelimiter)
		Catch ex As Exception
			Return String.Empty
		End Try
	End Function

	Protected Function FlattenArray(ByVal strArray() As String, ByVal intStartIndex As Integer, ByVal intEndIndex As Integer, ByVal strDelimiter As String) As String
		Dim strList As String
		Dim intIndex As Integer

		If strArray Is Nothing OrElse strArray.Length = 0 Then
			strList = String.Empty
		Else
			If intStartIndex < 0 Then intStartIndex = 0

			If intStartIndex >= strArray.Length Then
				strList = String.Empty
			Else
				If intEndIndex >= strArray.Length Then
					intEndIndex = strArray.Length - 1
				End If

				strList = strArray(intStartIndex)
				For intIndex = intStartIndex + 1 To intEndIndex
					If strArray(intIndex) Is Nothing Then
						strList &= strDelimiter
					Else
						strList &= strDelimiter & strArray(intIndex)
					End If
				Next intIndex
			End If
		End If

		Return strList
	End Function

	Public Function GenerateFinniganScanStatsFiles(ByVal strFinniganRawFilePath As String) As Boolean
		Dim ioFile As System.IO.FileInfo
		Dim blnsuccess As Boolean

		Try
			ioFile = New System.IO.FileInfo(strFinniganRawFilePath)

			blnsuccess = GenerateFinniganScanStatsFiles(strFinniganRawFilePath, ioFile.DirectoryName)

		Catch ex As Exception
			mErrorMessage = "Error obtaining FileInfo on file " & strFinniganRawFilePath & "; " & ex.Message
			ShowErrorMessage(mErrorMessage)
			TraceLog("Exception occurred: " & mErrorMessage)
			blnsuccess = False
		End Try

		Return blnsuccess

	End Function

	Public Function GenerateFinniganScanStatsFiles(ByVal strFinniganRawFilePath As String, ByVal strOutputFolderPath As String) As Boolean
		Dim ioFile As System.IO.FileInfo

		Dim blnSuccess As Boolean
		Dim strExeFilePath As String
		Dim strArgs As String = String.Empty

		' Call Finnigan_Datafile_Info_Scanner.exe to parse strFinniganRawFilePath

		Try
			strExeFilePath = System.IO.Path.Combine(GetAppFolderPath, FINNIGAN_DATAFILE_INFO_SCANNER)

			If Not System.IO.File.Exists(strExeFilePath) Then
				mErrorMessage = FINNIGAN_DATAFILE_INFO_SCANNER & " application not found in the program folder (" & GetAppFolderPath() & "); unable to generate the ScanStats.txt file"
				ShowErrorMessage(mErrorMessage)
				TraceLog(mErrorMessage)

				blnSuccess = False
			Else
				ioFile = New System.IO.FileInfo(strFinniganRawFilePath)

				' Define the input file path
				strArgs = "/I:" & PossiblyQuoteName(ioFile.FullName)

				' Define the output folder path
				strArgs &= " /O:" & PossiblyQuoteName(strOutputFolderPath)

				LogMessage("Generating Finnigan Scan Stats file using " & strExeFilePath)
				LogMessage("CmdLine: " & strArgs)

				TraceLog("Call RunProgram with " & strExeFilePath & " " & strArgs)

				Dim blnCreateWindow As Boolean = False
				blnSuccess = Me.RunProgram(strExeFilePath, String.Empty, strArgs, False, blnCreateWindow, ProcessWindowStyle.Minimized)

				TraceLog("RunProgram complete; blnSuccess = " & blnSuccess.ToString)

			End If
		Catch ex As Exception
			mErrorMessage = "Error calling " & FINNIGAN_DATAFILE_INFO_SCANNER & " with arguments '" & strArgs & "'; " & ex.Message
			ShowErrorMessage(mErrorMessage)
			TraceLog("Exception occurred: " & mErrorMessage)
			blnSuccess = False
		End Try


		Return blnSuccess

	End Function

	Public Shared Function GetBaseDatasetNameFromFileName(ByVal strFilePath As String) As String
		' Examines strFilePath
		' Looks for and removes _dta.txt or _fht.txt from the end
		' If the file ends in .dta, then returns the text up to the first period
		' Otherwise, simply removes the extension (as would required for .mgf files)

		Dim strDatasetName As String
		Dim intPeriodLoc As Integer

		If strFilePath Is Nothing OrElse strFilePath.Length = 0 Then
			Return String.Empty
		End If

		strDatasetName = System.IO.Path.GetFileName(strFilePath)

		intPeriodLoc = strDatasetName.IndexOf("."c)
		If intPeriodLoc > 0 Then
			strDatasetName = strDatasetName.Substring(0, intPeriodLoc)
		End If

		If strDatasetName.ToUpper.EndsWith("_DTA") OrElse _
			strDatasetName.ToUpper.EndsWith("_FHT") Then
			strDatasetName = strDatasetName.Substring(0, strDatasetName.Length - 4)
		End If

		Return strDatasetName

	End Function

	Public Shared Function GetAllowedFilterMatchTypes() As String
		Return TEXT_MATCH_TYPE_CONTAINS & ", " & _
			   TEXT_MATCH_TYPE_EXACT & ", " & _
			   TEXT_MATCH_TYPE_REGEX
	End Function

	Public Shared Function GetAllowedMSCollisionModeMatchTypes() As String
		Return GetAllowedFilterMatchTypes()
	End Function

	Public Overrides Function GetDefaultExtensionsToParse() As String()
		Dim strExtensionsToParse(3) As String

		strExtensionsToParse(0) = System.IO.Path.GetExtension(DTA_TXT_EXTENSION)
		strExtensionsToParse(1) = System.IO.Path.GetExtension(FHT_TXT_EXTENSION)
		strExtensionsToParse(2) = DTA_EXTENSION
		strExtensionsToParse(3) = MGF_EXTENSION

		Return strExtensionsToParse

	End Function

	Public Overrides Function GetErrorMessage() As String
		' Returns "" if no error

		Dim strErrorMessage As String = String.Empty

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
			If strErrorMessage.Length > 0 Then strErrorMessage &= "; "
			strErrorMessage &= mErrorMessage
		End If

		Return strErrorMessage

	End Function

	Private Function GetFilterMode4ReporterIons(ByRef dblReporterIonMZs As System.Collections.Generic.List(Of Double)) As Boolean
		Return ParseDelimitedIntegerList(mFilterMode4Options.ReporterIonMZs, dblReporterIonMZs)
	End Function

	Private Function GetReportFileName(ByVal strInputFilePath As String) As String
		Return GetReportFileName(strInputFilePath, String.Empty)
	End Function

	Private Function GetReportFileName(ByVal strInputFilePath As String, ByVal strOutputFolder As String) As String
		Dim strReportFileName As String
		strReportFileName = GetBaseDatasetNameFromFileName(System.IO.Path.GetFileName(strInputFilePath)) & "_SpectraQuality.txt"

		If Not strOutputFolder Is Nothing AndAlso strOutputFolder.Length > 0 Then
			strReportFileName = System.IO.Path.Combine(strOutputFolder, strReportFileName)
		End If

		Return strReportFileName

	End Function

	Private Sub HandleEvaluationResults(ByVal strReportFilePath As String, _
				ByRef udtSpectrumHeaderInfo As MsMsDataFileReader.clsMsMsDataFileReaderBaseClass.udtSpectrumHeaderInfoType, _
				ByVal udtSpectrumQualityScore As udtSpectrumQualityScoreType, ByVal sngBPI As Single, _
				ByVal blnIncludeNLStats As Boolean, _
				ByVal blnIncludeReporterIonStats As Boolean, _
				ByRef udtIonMatchStats As udtIonMatchStatsType, _
				ByVal intMSLevelFilter As Integer, _
				ByVal strScanTypeFilter As String, ByRef reScanTypeFilter As System.Text.RegularExpressions.Regex, _
				ByVal strCollisionModeFilter As String, ByRef reCollisionModeFilter As System.Text.RegularExpressions.Regex, _
				ByRef blnKeepSpectrum As Boolean, _
				ByVal blnIonFilteringEnabled As Boolean, ByVal intPositiveDataCountBeforeFilter As Integer, ByVal intPositiveDataCountAfterFilter As Integer)

		Dim blnPassesFilter As Boolean

		Dim intMSLevel As Integer
		Dim strScanTypeName As String
		Dim strCollisionMode As String

		Dim objMatch As Object

		If mEvaluateSpectrumQualityOnly Or mGenerateFilterReport Then
			' Add a new row to the report file
			AppendReportLine(strReportFilePath, _
							udtSpectrumHeaderInfo.ScanNumberStart, udtSpectrumHeaderInfo.ScanNumberEnd, _
							udtSpectrumHeaderInfo.ParentIonCharges(0), udtSpectrumQualityScore, _
							udtSpectrumHeaderInfo.ParentIonMZ, sngBPI, _
							blnIncludeNLStats, blnIncludeReporterIonStats, udtIonMatchStats, _
							blnIonFilteringEnabled, intPositiveDataCountBeforeFilter, intPositiveDataCountAfterFilter)
		End If

		If Not mEvaluateSpectrumQualityOnly Then
			' Determine whether this spectrum should be kept, based on the quality score, the MSLevel filter, mDiscardValidSpectra, and mMinimumIonCount

			If udtSpectrumQualityScore.SpectrumQualityScore >= mMinimumQualityScore Then
				blnPassesFilter = True
			Else
				blnPassesFilter = False
			End If

			If blnPassesFilter And mMinimumIonCount > 0 Then
				If intPositiveDataCountAfterFilter < mMinimumIonCount Then blnPassesFilter = False
			End If

			If blnPassesFilter AndAlso intMSLevelFilter <> 0 Then
				' Make sure this spectrum matches intMSLevelFilter
				Try
					objMatch = mScanStatsPointer(udtSpectrumHeaderInfo.ScanNumberStart)

					If objMatch Is Nothing Then
						' Match not found in mScanStatsPointer; include the spectrum in the file anyway for safety
					Else
						intMSLevel = mScanStatsInfo(CInt(objMatch)).MSLevel
						If intMSLevel <> intMSLevelFilter Then
							blnPassesFilter = False
						End If
					End If

				Catch ex As Exception
					' mScanStatsInfo doesn't contain scan .ScanNumberStart
					' We'll include it in the output file anyway for safety
				End Try
			End If


			If blnPassesFilter AndAlso strScanTypeFilter.Length > 0 Then
				' Make sure this spectrum matches strScanTypeFilter
				Try
					objMatch = mScanStatsPointer(udtSpectrumHeaderInfo.ScanNumberStart)

					If objMatch Is Nothing Then
						' Match not found in mScanStatsPointer; include the spectrum in the file anyway for safety
					Else
						strScanTypeName = mScanStatsInfo(CInt(objMatch)).ScanTypeName
						Select Case mScanTypeMatchType
							Case eTextMatchTypeConstants.Exact
								If strScanTypeName.ToLower <> strScanTypeFilter.ToLower Then
									blnPassesFilter = False
								End If

							Case eTextMatchTypeConstants.RegEx
								If Not reScanTypeFilter.Match(strScanTypeName).Success Then
									blnPassesFilter = False
								End If

							Case Else
								' Includes eTextMatchTypeConstants.Contains

								If strScanTypeName.ToLower.IndexOf(strScanTypeFilter.ToLower) < 0 Then
									blnPassesFilter = False
								End If

						End Select
					End If

				Catch ex As Exception
					' mScanStatsInfo doesn't contain scan .ScanNumberStart
					' We'll include it in the output file anyway for safety
				End Try
			End If


			' Note that strCollisionModeFilter gets populated using mMSCollisionModeFilter
			'  if this program is able to generate the scan stats file
			If blnPassesFilter AndAlso strCollisionModeFilter.Length > 0 Then
				' Make sure this spectrum matches strCollisionModeFilter
				Try
					objMatch = mExtendedStatsPointer(udtSpectrumHeaderInfo.ScanNumberStart)

					If objMatch Is Nothing Then
						' Match not found in mExtendedStatsPointer; include the spectrum in the file anyway for safety
					Else
						strCollisionMode = mExtendedStatsInfo(CInt(objMatch)).CollisionMode
						Select Case mMSCollisionModeMatchType
							Case eTextMatchTypeConstants.Exact
								If strCollisionMode.ToLower <> strCollisionModeFilter.ToLower Then
									blnPassesFilter = False
								End If

							Case eTextMatchTypeConstants.RegEx
								If Not reCollisionModeFilter.Match(strCollisionMode).Success Then
									blnPassesFilter = False
								End If

							Case Else
								' Includes eTextMatchTypeConstants.Contains

								If strCollisionMode.ToLower.IndexOf(strCollisionModeFilter.ToLower) < 0 Then
									blnPassesFilter = False
								End If

						End Select
					End If

				Catch ex As Exception
					' mExtendedStatsInfo doesn't contain scan .ScanNumberStart
					' We'll include it in the output file anyway for safety
				End Try
			End If


			If (blnPassesFilter And Not mDiscardValidSpectra) Or _
			   (Not blnPassesFilter And mDiscardValidSpectra) Then

				' Either the spectrum passes the filters and we're not discarding valid spectra OR
				'  the spectrum doesn't pass the filters and we are discarding valid spectra

				' Therefore, keep this spectrum
				blnKeepSpectrum = True

			Else
				blnKeepSpectrum = False
			End If
		End If
	End Sub

    Private Sub InitializeAminoAcidMassList(ByRef htMassList As Hashtable)
        ' Note: The amino acid masses are monoisotopic masses, and are the standard AA mass minus H2O
        ' Using the ! symbol to force them to be stored as single precision numbers

        If htMassList Is Nothing Then htMassList = New Hashtable

        htMassList.Clear()
        htMassList.Add("G"c, 57.02146!)
        htMassList.Add("A"c, 71.03711!)
        htMassList.Add("S"c, 87.03203!)      ' Note that the Sequest params file lists this mass as 87.02303, which is incorrect
        htMassList.Add("P"c, 97.05276!)
        htMassList.Add("V"c, 99.06841!)
        htMassList.Add("T"c, 101.047676!)
        htMassList.Add("C"c, 103.009186!)
        htMassList.Add("L"c, 113.084061!)
        htMassList.Add("N"c, 114.042923!)
        htMassList.Add("D"c, 115.026939!)
        htMassList.Add("Q"c, 128.058578!)
        htMassList.Add("K"c, 128.094955!)
        htMassList.Add("E"c, 129.042587!)
        htMassList.Add("M"c, 131.040482!)
        htMassList.Add("H"c, 137.058914!)
        htMassList.Add("F"c, 147.0684!)
        htMassList.Add("R"c, 156.1011!)
        htMassList.Add("Y"c, 163.063324!)
        htMassList.Add("W"c, 186.079315!)

    End Sub

    Private Sub InitializeVariables()

        'Filter options
        mSpectrumFilterMode = eSpectrumFilterMode.mode1
        mMinimumQualityScore = 0
        mGenerateFilterReport = True
        mIncludeNLStatsOnFilterReport = True            ' Only used if mSpectrumFilterMode = mode3

        mOverwriteExistingFiles = True
        mOverwriteReportFile = True
        mAutoCloseReportFile = True

        mMaximumProgressUpdateIntervalSeconds = 1       ' Default to 1 second; use a larger number to decrease the number of entries to the log file

        mDiscardValidSpectra = False
        mDeleteBadDTAFiles = False
        mEvaluateSpectrumQualityOnly = False

        mMSLevelFilter = 0
        mScanTypeFilter = String.Empty
        mMSCollisionModeFilter = String.Empty
		mMinimumIonCount = 0

        With mIonFilterOptions
            .RemovePrecursor = False
            .RemoveChargeReducedPrecursors = False
            .RemoveNeutralLossesFromChargeReducedPrecursors = False
            .PrecursorCleaningToleranceMZ = DEFAULT_IONFILTER_PRECURSOR_TOLERANCE_MZ
            .NeutralLossCleaningWindowDa = DEFAULT_IONFILTER_PRECURSOR_NL_CLEANING_WINDOW_DA
            .CondenseData = False
			.CondenseDataMaxIonCount = 0
		End With

        ' Filter mode 1
        ' Initialize the values for mFilterMode1Options.StandardMassSpacingCounts()
        mFilterMode1Options.Initialize()

        ' Initialize the remaining settings for FilterMode1
        With mFilterMode1Options
            .IonPairMassToleranceHalfWidthDa = 0.2
            .NoiseLevelIntensityThreshold = 10
            .DataPointCountToConsider = 50
            .SignalToNoiseThreshold = 5
            .EnableSegmentedSignalToNoise = True
            .SegmentedSignalToNoiseMZWidth = 50
            .UseLogIntensity = False
        End With

        ' Filter mode 2
        With mFilterMode2Options
            .SignificantIntensityFractionBasePeak = 0.05
            .NoiseThresholdFraction = 0.5
            .Charge1SignificantPeakNumberThreshold = 2
            .Charge2SignificantPeakNumberThreshold = 2
            .TICScoreThreshold = 200000.0                   ' Originally 2E6; changed to 2E5 in February 2008
            .HighQualitySNThreshold = 20
            .HighQualitySNPeakCount = 10
            .ModerateQualitySNThreshold = 15
            .ModerateQualitySNPeakCount = 6
            .LowQualitySNThreshold = 10
            .LowQualitySNPeakCount = 5
            .ComputeFractionalScores = True
            .SequestParamFilePath = String.Empty
        End With

        ' Filter mode 3
        With mFilterMode3Options
            .BasePeakIntensityMinimum = 1000
            .MassToleranceHalfWidthMZ = 0.7
            .NLAbundanceThresholdFractionMax = 0.5
            .LimitToChargeSpecificIons = True
            .ConsiderWaterLoss = True
            .SpecificMZLosses = String.Empty
        End With

        ' Filter mode 4
        With mFilterMode4Options
            .ReporterIonMZs = DEFAULT_MODE4_REPORTER_ION_MZs
            .ReporterIonMinimumIntensity = 50
            .ReporterIonToleranceHalfWidthMZ = 0.7
            .RemoveReporterIons = True
            .PrecursorIonMassLossDa = DEFAULT_MODE4_PARENT_ION_MASS_LOSS_DA
            .ReporterIonMatchCountMinimum = 2
            .ReporterIonMaxIntensityThreshold = 500
            .RemoveMassCorrectedPrecursorIons = True
            .PrecursorIonMinimumCharge = 2
        End With

        ' Populate mAminoAcidMassList with the amino acids
        InitializeAminoAcidMassList(mAminoAcidMassList)

        mLocalErrorCode = eFilterMsMsSpectraErrorCodes.NoError

        mReportFilePath = String.Empty

    End Sub

    Public Shared Function IsNumber(ByVal strValue As String) As Boolean
        Dim dblResult As Double
        Try
            Return Double.TryParse(strValue, dblResult)
        Catch ex As Exception
            Return False
        End Try
    End Function

    Public Function LoadParameterFileSettings(ByVal strParameterFilePath As String) As Boolean
        ' Returns True if no error; otherwise, returns False
        ' If strParameterFilePath is blank, then returns True since this isn't an error

        Const FILTER_OPTIONS_SECTION As String = "FilterOptions"
        Const ION_FILTER_OPTIONS_SECTION As String = "IonFilterOptions"
        Const FILTER_MODE1 As String = "FilterMode1"
        Const FILTER_MODE2 As String = "FilterMode2"
        Const FILTER_MODE3 As String = "FilterMode3"
        Const FILTER_MODE4 As String = "FilterMode4"

        Dim objSettingsFile As New XmlSettingsFileAccessor

        Dim intStandardMassSpacingCount As Integer
        Dim blnValueNotPresent As Boolean

        Dim intChargeIndex As Integer
        Dim strKeyName As String

        If mSettingsLoadedViaCode Then Return True

        Try

            If strParameterFilePath Is Nothing OrElse strParameterFilePath.Length = 0 Then
                ' No parameter file specified; nothing to load
                Return True
            End If

            If Not System.IO.File.Exists(strParameterFilePath) Then
                ' See if strParameterFilePath points to a file in the same directory as the application
				strParameterFilePath = System.IO.Path.Combine(GetAppFolderPath(), System.IO.Path.GetFileName(strParameterFilePath))
                If Not System.IO.File.Exists(strParameterFilePath) Then
                    MyBase.SetBaseClassErrorCode(clsProcessFilesBaseClass.eProcessFilesErrorCodes.ParameterFileNotFound)
                    mErrorMessage = MyBase.GetBaseClassErrorMessage()
                    Return False
                End If
            End If

            If objSettingsFile.LoadSettings(strParameterFilePath) Then
                If Not objSettingsFile.SectionPresent(FILTER_OPTIONS_SECTION) Then
                    mErrorMessage = "The node '<section name=""" & FILTER_OPTIONS_SECTION & """> was not found in the parameter file: " & strParameterFilePath
                    ShowErrorMessage(mErrorMessage)
                    MyBase.SetBaseClassErrorCode(clsProcessFilesBaseClass.eProcessFilesErrorCodes.InvalidParameterFile)
                    Return False
                Else
                    Me.SpectrumFilterMode = CType(objSettingsFile.GetParam(FILTER_OPTIONS_SECTION, "FilterMode", CInt(Me.SpectrumFilterMode)), eSpectrumFilterMode)
                    Me.MinimumQualityScore = objSettingsFile.GetParam(FILTER_OPTIONS_SECTION, "MinimumQualityScore", Me.MinimumQualityScore)
                    Me.GenerateFilterReport = objSettingsFile.GetParam(FILTER_OPTIONS_SECTION, "GenerateFilterReport", Me.GenerateFilterReport)
                    Me.IncludeNLStatsOnFilterReport = objSettingsFile.GetParam(FILTER_OPTIONS_SECTION, "IncludeNLStatsOnFilterReport", Me.IncludeNLStatsOnFilterReport)
                    Me.OverwriteExistingFiles = objSettingsFile.GetParam(FILTER_OPTIONS_SECTION, "OverwriteExistingFiles", Me.OverwriteExistingFiles)
                    Me.DiscardValidSpectra = objSettingsFile.GetParam(FILTER_OPTIONS_SECTION, "DiscardValidSpectra", Me.DiscardValidSpectra)
                    Me.EvaluateSpectrumQualityOnly = objSettingsFile.GetParam(FILTER_OPTIONS_SECTION, "EvaluateSpectrumQualityOnly", Me.EvaluateSpectrumQualityOnly)
                    Me.MSLevelFilter = objSettingsFile.GetParam(FILTER_OPTIONS_SECTION, "MSLevelFilter", Me.MSLevelFilter)

                    Me.ScanTypeFilter = objSettingsFile.GetParam(FILTER_OPTIONS_SECTION, "ScanTypeFilter", Me.ScanTypeFilter)
                    Me.ScanTypeMatchType = objSettingsFile.GetParam(FILTER_OPTIONS_SECTION, "ScanTypeMatchType", Me.ScanTypeMatchType)

                    Me.MSCollisionModeFilter = objSettingsFile.GetParam(FILTER_OPTIONS_SECTION, "MSCollisionModeFilter", Me.MSCollisionModeFilter)
                    Me.MSCollisionModeMatchType = objSettingsFile.GetParam(FILTER_OPTIONS_SECTION, "MSCollisionModeMatchType", Me.MSCollisionModeMatchType)

					Me.MinimumIonCount = objSettingsFile.GetParam(FILTER_OPTIONS_SECTION, "MinimumIonCount", Me.MinimumIonCount)
				End If

                If Not objSettingsFile.SectionPresent(ION_FILTER_OPTIONS_SECTION) Then
                    ' It's OK if this section is missing
                Else
                    Me.IonFilter_RemovePrecursor = objSettingsFile.GetParam(ION_FILTER_OPTIONS_SECTION, "RemovePrecursor", Me.IonFilter_RemovePrecursor)
                    Me.IonFilter_RemoveChargeReducedPrecursors = objSettingsFile.GetParam(ION_FILTER_OPTIONS_SECTION, "RemoveChargeReducedPrecursors", Me.IonFilter_RemoveChargeReducedPrecursors)
                    Me.IonFilter_RemoveNeutralLossesFromChargeReducedPrecursors = objSettingsFile.GetParam(ION_FILTER_OPTIONS_SECTION, "RemoveNeutralLossesFromChargeReducedPrecursors", Me.IonFilter_RemoveNeutralLossesFromChargeReducedPrecursors)
                    Me.IonFilter_PrecursorCleaningToleranceMZ = objSettingsFile.GetParam(ION_FILTER_OPTIONS_SECTION, "PrecursorCleaningToleranceMZ", Me.IonFilter_PrecursorCleaningToleranceMZ)
                    Me.IonFilter_NeutralLossCleaningWindowDa = objSettingsFile.GetParam(ION_FILTER_OPTIONS_SECTION, "NeutralLossCleaningWindowDa", Me.IonFilter_NeutralLossCleaningWindowDa)
                    Me.IonFilter_CondenseData = objSettingsFile.GetParam(ION_FILTER_OPTIONS_SECTION, "CondenseData", Me.IonFilter_CondenseData)
					Me.IonFilter_CondenseDataMaxIonCount = objSettingsFile.GetParam(ION_FILTER_OPTIONS_SECTION, "CondenseDataMaxIonCount", Me.IonFilter_CondenseDataMaxIonCount)
				End If

                If Not objSettingsFile.SectionPresent(FILTER_MODE1) Then
                    If Me.SpectrumFilterMode = eSpectrumFilterMode.mode1 Then
                        mErrorMessage = "The Spectrum Filter Mode is 1, but node '<section name=""" & FILTER_MODE1 & """> was not found in the parameter file: " & strParameterFilePath
                        ShowErrorMessage(mErrorMessage)
                        MyBase.SetBaseClassErrorCode(clsProcessFilesBaseClass.eProcessFilesErrorCodes.InvalidParameterFile)
                        Return False
                    End If
                Else
                    With mFilterMode1Options

                        intStandardMassSpacingCount = 0
                        For intChargeIndex = 0 To .StandardMassSpacingCounts.Length - 1
                            strKeyName = "Charge" & (intChargeIndex + 1).ToString & "StandardMassSpacingMinimum"
                            .StandardMassSpacingCounts(intChargeIndex).Minimum = objSettingsFile.GetParam(FILTER_MODE1, strKeyName, .StandardMassSpacingCounts(intChargeIndex).Minimum, blnValueNotPresent)
                            If Not blnValueNotPresent Then
                                intStandardMassSpacingCount += 1
                            End If

                            strKeyName = "Charge" & (intChargeIndex + 1).ToString & "StandardMassSpacingMaximum"
                            .StandardMassSpacingCounts(intChargeIndex).Maximum = objSettingsFile.GetParam(FILTER_MODE1, strKeyName, .StandardMassSpacingCounts(intChargeIndex).Maximum, blnValueNotPresent)
                            If Not blnValueNotPresent Then
                                intStandardMassSpacingCount += 1
                            End If
                        Next intChargeIndex

                        .IonPairMassToleranceHalfWidthDa = objSettingsFile.GetParam(FILTER_MODE1, "IonPairMassToleranceHalfWidthDa", .IonPairMassToleranceHalfWidthDa)
                        .NoiseLevelIntensityThreshold = objSettingsFile.GetParam(FILTER_MODE1, "NoiseLevelIntensityThreshold", .NoiseLevelIntensityThreshold)
                        .DataPointCountToConsider = objSettingsFile.GetParam(FILTER_MODE1, "DataPointCountToConsider", .DataPointCountToConsider)
                        .SignalToNoiseThreshold = objSettingsFile.GetParam(FILTER_MODE1, "SignalToNoiseThreshold", .SignalToNoiseThreshold)
                        .EnableSegmentedSignalToNoise = objSettingsFile.GetParam(FILTER_MODE1, "EnableSegmentedSignalToNoise", .EnableSegmentedSignalToNoise)
                        .SegmentedSignalToNoiseMZWidth = objSettingsFile.GetParam(FILTER_MODE1, "SegmentedSignalToNoiseMZWidth", .SegmentedSignalToNoiseMZWidth)
                        .UseLogIntensity = objSettingsFile.GetParam(FILTER_MODE1, "UseLogIntensity", .UseLogIntensity)
                    End With
                End If

                If Not objSettingsFile.SectionPresent(FILTER_MODE2) Then
                    If Me.SpectrumFilterMode = eSpectrumFilterMode.mode2 Then
                        mErrorMessage = "The Spectrum Filter Mode is 2, but node '<section name=""" & FILTER_MODE2 & """> was not found in the parameter file: " & strParameterFilePath
                        ShowErrorMessage(mErrorMessage)
                        MyBase.SetBaseClassErrorCode(clsProcessFilesBaseClass.eProcessFilesErrorCodes.InvalidParameterFile)
                        Return False
                    End If
                Else
                    With mFilterMode2Options
                        .SignificantIntensityFractionBasePeak = objSettingsFile.GetParam(FILTER_MODE2, "SignificantIntensityFractionBasePeak", .SignificantIntensityFractionBasePeak)
                        .NoiseThresholdFraction = objSettingsFile.GetParam(FILTER_MODE2, "NoiseThresholdFraction", .NoiseThresholdFraction)
                        .Charge1SignificantPeakNumberThreshold = objSettingsFile.GetParam(FILTER_MODE2, "Charge1SignificantPeakNumberThreshold", .Charge1SignificantPeakNumberThreshold)
                        .Charge2SignificantPeakNumberThreshold = objSettingsFile.GetParam(FILTER_MODE2, "Charge2SignificantPeakNumberThreshold", .Charge2SignificantPeakNumberThreshold)
                        .TICScoreThreshold = objSettingsFile.GetParam(FILTER_MODE2, "TICThreshold", .TICScoreThreshold)

                        .HighQualitySNThreshold = objSettingsFile.GetParam(FILTER_MODE2, "HighQualitySNThreshold", .HighQualitySNThreshold)
                        .HighQualitySNPeakCount = objSettingsFile.GetParam(FILTER_MODE2, "HighQualitySNPeakCount", .HighQualitySNPeakCount)
                        .ModerateQualitySNThreshold = objSettingsFile.GetParam(FILTER_MODE2, "ModerateQualitySNThreshold", .ModerateQualitySNThreshold)
                        .ModerateQualitySNPeakCount = objSettingsFile.GetParam(FILTER_MODE2, "ModerateQualitySNPeakCount", .ModerateQualitySNPeakCount)
                        .LowQualitySNThreshold = objSettingsFile.GetParam(FILTER_MODE2, "LowQualitySNThreshold", .LowQualitySNThreshold)
                        .LowQualitySNPeakCount = objSettingsFile.GetParam(FILTER_MODE2, "LowQualitySNPeakCount", .LowQualitySNPeakCount)
                        .ComputeFractionalScores = objSettingsFile.GetParam(FILTER_MODE2, "ComputeFractionalScores", .ComputeFractionalScores)
                    End With
                End If

                If Not objSettingsFile.SectionPresent(FILTER_MODE3) Then
                    If Me.SpectrumFilterMode = eSpectrumFilterMode.mode3 Then
                        mErrorMessage = "The Spectrum Filter Mode is 3, but node '<section name=""" & FILTER_MODE3 & """> was not found in the parameter file: " & strParameterFilePath
                        ShowErrorMessage(mErrorMessage)
                        MyBase.SetBaseClassErrorCode(clsProcessFilesBaseClass.eProcessFilesErrorCodes.InvalidParameterFile)
                        Return False
                    End If
                Else
                    With mFilterMode3Options
                        .BasePeakIntensityMinimum = objSettingsFile.GetParam(FILTER_MODE3, "BasePeakIntensityMinimum", .BasePeakIntensityMinimum)
                        .MassToleranceHalfWidthMZ = objSettingsFile.GetParam(FILTER_MODE3, "MassToleranceHalfWidthMZ", .MassToleranceHalfWidthMZ)
                        .NLAbundanceThresholdFractionMax = objSettingsFile.GetParam(FILTER_MODE3, "NLAbundanceThresholdFractionMax", .NLAbundanceThresholdFractionMax)
                        .LimitToChargeSpecificIons = objSettingsFile.GetParam(FILTER_MODE3, "LimitToChargeSpecificIons", .LimitToChargeSpecificIons)
                        .ConsiderWaterLoss = objSettingsFile.GetParam(FILTER_MODE3, "ConsiderWaterLoss", .ConsiderWaterLoss)
                        .SpecificMZLosses = objSettingsFile.GetParam(FILTER_MODE3, "SpecificMZLosses", .SpecificMZLosses)
                    End With
                End If

                If Not objSettingsFile.SectionPresent(FILTER_MODE4) Then
                    If Me.SpectrumFilterMode = eSpectrumFilterMode.mode4 Then
                        mErrorMessage = "The Spectrum Filter Mode is 4, but node '<section name=""" & FILTER_MODE3 & """> was not found in the parameter file: " & strParameterFilePath
                        ShowErrorMessage(mErrorMessage)
                        MyBase.SetBaseClassErrorCode(clsProcessFilesBaseClass.eProcessFilesErrorCodes.InvalidParameterFile)
                        Return False
                    End If
                Else
                    With mFilterMode4Options
                        .ReporterIonMZs = objSettingsFile.GetParam(FILTER_MODE4, "ReporterIonMZs", .ReporterIonMZs)
                        .ReporterIonMinimumIntensity = objSettingsFile.GetParam(FILTER_MODE4, "ReporterIonMinimumIntensity", .ReporterIonMinimumIntensity)
                        .ReporterIonToleranceHalfWidthMZ = objSettingsFile.GetParam(FILTER_MODE4, "ReporterIonToleranceHalfWidthMZ", .ReporterIonToleranceHalfWidthMZ)
                        .RemoveReporterIons = objSettingsFile.GetParam(FILTER_MODE4, "RemoveReporterIons", .RemoveReporterIons)
                        .PrecursorIonMassLossDa = objSettingsFile.GetParam(FILTER_MODE4, "PrecursorIonMassLossDa", .PrecursorIonMassLossDa)
                        .ReporterIonMatchCountMinimum = objSettingsFile.GetParam(FILTER_MODE4, "ReporterIonMatchCountMinimum", .ReporterIonMatchCountMinimum)
                        .ReporterIonMaxIntensityThreshold = objSettingsFile.GetParam(FILTER_MODE4, "ReporterIonMaxIntensityThreshold", .ReporterIonMaxIntensityThreshold)
                        .RemoveMassCorrectedPrecursorIons = objSettingsFile.GetParam(FILTER_MODE4, "RemoveMassCorrectedPrecursorIons", .RemoveMassCorrectedPrecursorIons)
                        .PrecursorIonMinimumCharge = objSettingsFile.GetParam(FILTER_MODE4, "PrecursorIonMinimumCharge", .PrecursorIonMinimumCharge)
                    End With
                End If
            End If

        Catch ex As Exception
            mErrorMessage = "Error in LoadParameterFileSettings: " & ex.Message
            HandleException(mErrorMessage, ex)
            Return False
        End Try

        Return True

    End Function

    Private Function LoadScanStatsFile(ByVal strScanStatsFilePath As String) As Boolean
        Dim blnSuccess As Boolean

		Dim strLineIn As String
        Dim strSplitLine() As String

        Dim blnHeadersDefined As Boolean
        Dim intColumnMap() As Integer

        Dim intIndex As Integer
        Dim intScanStatsInfoCount As Integer
        Dim intScanNumber As Integer

        Dim strValue As String
        Dim intValue As Integer

        Dim strMessage As String

        Try

            ' Read the _ScanStats file

            ReDim intColumnMap(SCANSTATS_COL_COUNT - 1)
            For intIndex = 0 To intColumnMap.Length - 1
                intColumnMap(intIndex) = -1
            Next

            ' Initially reserve space for 1000 scans
            intScanStatsInfoCount = 0
            ReDim mScanStatsInfo(999)

            If mScanStatsPointer Is Nothing Then
                mScanStatsPointer = New Hashtable
            Else
                mScanStatsPointer.Clear()
            End If


			If System.IO.File.Exists(strScanStatsFilePath) Then

				Using srInFile As System.IO.StreamReader = New System.IO.StreamReader(New System.IO.FileStream(strScanStatsFilePath, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.Read))

					Do While srInFile.Peek() >= 0
						strLineIn = srInFile.ReadLine
						If Not strLineIn Is Nothing AndAlso strLineIn.Trim.Length > 0 Then
							strLineIn = strLineIn.Trim

							Try
								strSplitLine = strLineIn.Split(ControlChars.Tab)

								If Not blnHeadersDefined Then
									' Parse the header line to define the column mapping
									For intIndex = 0 To strSplitLine.Length - 1

										Select Case strSplitLine(intIndex).ToLower
											Case SCANSTATS_COL_SCAN_NUM.ToLower
												intColumnMap(eScanStatsColumns.ScanNumber) = intIndex

											Case SCANSTATS_COL_SCAN_TYPE.ToLower
												intColumnMap(eScanStatsColumns.ScanType) = intIndex

											Case SCANSTATS_COL_SCAN_TYPE_NAME.ToLower
												intColumnMap(eScanStatsColumns.ScanTypeName) = intIndex

											Case Else
												' Ignore this column
										End Select
									Next intIndex

									If intColumnMap(eScanStatsColumns.ScanNumber) < 0 Then
										' Scan Number column was not found; this is a fatal error
										strMessage = "'" & SCANSTATS_COL_SCAN_NUM & "' column not found in " & strScanStatsFilePath & "; unable to continue"
										TraceLog(strMessage)
										ShowErrorMessage(strMessage)
										blnSuccess = False
										Exit Try
									End If

									If mScanTypeFilter.Length > 0 AndAlso intColumnMap(eScanStatsColumns.ScanTypeName) < 0 Then
										' Scan Type Name column was not found; this is a fatal error
										strMessage = "'" & SCANSTATS_COL_SCAN_TYPE_NAME & "' column not found in " & strScanStatsFilePath & "; unable to continue"
										TraceLog(strMessage)
										ShowErrorMessage(strMessage)
										blnSuccess = False
										Exit Try
									End If
									blnHeadersDefined = True
								Else

									If intScanStatsInfoCount >= mScanStatsInfo.Length Then
										' Reserve more space in mScanStatsInfo
										ReDim Preserve mScanStatsInfo(mScanStatsInfo.Length * 2 - 1)
									End If

									strValue = LookupSplitLineValue(strSplitLine, intColumnMap, eScanStatsColumns.ScanNumber)
									If Integer.TryParse(strValue, intValue) Then
										intScanNumber = intValue

										If mScanStatsPointer.Contains(intScanNumber) Then
											' The same scan is present multiple times in the ScanStatsEx file; this is unexpected
											' We will skip this duplicate entry
										Else
											' Make a new entry in mScanStatsInfo
											With mScanStatsInfo(intScanStatsInfoCount)

												.ScanNumber = intScanNumber

												strValue = LookupSplitLineValue(strSplitLine, intColumnMap, eScanStatsColumns.ScanType)
												If Not Int32.TryParse(strValue, .MSLevel) Then
													ShowErrorMessage("Error: ScanType column is not an integer in line " & strLineIn)
													.MSLevel = 0
												End If

												.ScanTypeName = LookupSplitLineValue(strSplitLine, intColumnMap, eScanStatsColumns.ScanTypeName)
											End With

											' Store a mapping between intScanNumber and intScanStatsInfoCount
											mScanStatsPointer.Add(intScanNumber, intScanStatsInfoCount)

											intScanStatsInfoCount += 1
										End If
									End If

								End If

							Catch ex As Exception
								ShowErrorMessage("Error parsing line " & strLineIn & "; " & ex.Message)
							End Try
						End If

					Loop

				End Using

				' Shrink mScanStatsInfo
				ReDim Preserve mScanStatsInfo(intScanStatsInfoCount - 1)

				blnSuccess = True
			Else
				blnSuccess = False
			End If

        Catch ex As Exception
            ShowErrorMessage("Error reading the ScanStats file (" & strScanStatsFilePath & "); " & ex.Message)
            blnSuccess = False      
        End Try

        Return blnSuccess

    End Function

    Private Function LoadScanStatsExFile(ByVal strScanStatsExFilePath As String) As Boolean
        Dim blnSuccess As Boolean

		Dim strLineIn As String
        Dim strSplitLine() As String

        Dim blnHeadersDefined As Boolean
        Dim intColumnMap() As Integer

        Dim intIndex As Integer
        Dim intExtendedStatsInfoCount As Integer
        Dim intScanNumber As Integer

        Dim strValue As String
        Dim intValue As Integer

        Dim strMessage As String

        Try

            ' Read the _ScanStatsEx file

            ReDim intColumnMap(SCANSTATS_EX_COL_COUNT - 1)
            For intIndex = 0 To intColumnMap.Length - 1
                intColumnMap(intIndex) = -1
            Next

            ' Initially reserve space for 1000 scans
            intExtendedStatsInfoCount = 0
            ReDim mExtendedStatsInfo(999)

            If mExtendedStatsPointer Is Nothing Then
                mExtendedStatsPointer = New Hashtable
            Else
                mExtendedStatsPointer.Clear()
            End If


            If System.IO.File.Exists(strScanStatsExFilePath) Then
				Using srInFile As System.IO.StreamReader = New System.IO.StreamReader(New System.IO.FileStream(strScanStatsExFilePath, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.Read))

					Do While srInFile.Peek() >= 0
						strLineIn = srInFile.ReadLine
						If Not strLineIn Is Nothing AndAlso strLineIn.Trim.Length > 0 Then
							strLineIn = strLineIn.Trim

							Try
								strSplitLine = strLineIn.Split(ControlChars.Tab)

								If Not blnHeadersDefined Then
									' Parse the header line to define the column mapping
									For intIndex = 0 To strSplitLine.Length - 1

										Select Case strSplitLine(intIndex).ToLower
											Case SCANSTATS_COL_SCAN_NUM.ToLower
												intColumnMap(eScanStatsExColumns.ScanNumber) = intIndex

											Case SCANSTATS_COL_ION_INJECTION_TIME.ToLower
												intColumnMap(eScanStatsExColumns.IonInjectionTime) = intIndex

											Case SCANSTATS_COL_SCAN_SEGMENT.ToLower
												intColumnMap(eScanStatsExColumns.ScanSegment) = intIndex

											Case SCANSTATS_COL_SCAN_EVENT.ToLower
												intColumnMap(eScanStatsExColumns.ScanEvent) = intIndex

											Case SCANSTATS_COL_CHARGE_STATE.ToLower
												intColumnMap(eScanStatsExColumns.ChargeState) = intIndex

											Case SCANSTATS_COL_MONOISOTOPIC_MZ.ToLower
												intColumnMap(eScanStatsExColumns.MonoisotopicMZ) = intIndex

											Case SCANSTATS_COL_COLLISION_MODE.ToLower
												intColumnMap(eScanStatsExColumns.CollisionMode) = intIndex

											Case SCANSTATS_COL_SCAN_FILTER_TEXT.ToLower
												intColumnMap(eScanStatsExColumns.ScanFilterText) = intIndex

											Case Else
												' Ignore this column

										End Select
									Next intIndex

									If intColumnMap(eScanStatsExColumns.ScanNumber) < 0 Then
										' Scan Number column was not found; this is a fatal error
										strMessage = "'" & SCANSTATS_COL_SCAN_NUM & "' column not found in " & strScanStatsExFilePath & "; unable to continue"
										TraceLog(strMessage)
										ShowErrorMessage(strMessage)
										blnSuccess = False
										Exit Try
									End If

									If mMSCollisionModeFilter.Length > 0 AndAlso intColumnMap(eScanStatsExColumns.CollisionMode) < 0 Then
										' Collision mode column was not found; this is a fatal error
										strMessage = "'" & SCANSTATS_COL_COLLISION_MODE & "' column not found in " & strScanStatsExFilePath & "; unable to continue"
										TraceLog(strMessage)
										ShowErrorMessage(strMessage)
										blnSuccess = False
										Exit Try
									End If

									blnHeadersDefined = True
								Else

									If intExtendedStatsInfoCount >= mExtendedStatsInfo.Length Then
										' Reserve more space in mExtendedStatsInfo
										ReDim Preserve mExtendedStatsInfo(mExtendedStatsInfo.Length * 2 - 1)
									End If

									strValue = LookupSplitLineValue(strSplitLine, intColumnMap, eScanStatsExColumns.ScanNumber)
									If Integer.TryParse(strValue, intValue) Then
										intScanNumber = intValue

										If mExtendedStatsPointer.Contains(intScanNumber) Then
											' The same scan is present multiple times in the ScanStatsEx file; this is unexpected
											' We will skip this duplicate entry
										Else
											' Make a new entry in mExtendedStatsInfo
											With mExtendedStatsInfo(intExtendedStatsInfoCount)

												.ScanNumber = intScanNumber
												.IonInjectionTime = LookupSplitLineValue(strSplitLine, intColumnMap, eScanStatsExColumns.IonInjectionTime)
												.ScanSegment = LookupSplitLineValue(strSplitLine, intColumnMap, eScanStatsExColumns.ScanSegment)
												.ScanEvent = LookupSplitLineValue(strSplitLine, intColumnMap, eScanStatsExColumns.ScanEvent)
												.ChargeState = LookupSplitLineValue(strSplitLine, intColumnMap, eScanStatsExColumns.ChargeState)
												.MonoisotopicMZ = LookupSplitLineValue(strSplitLine, intColumnMap, eScanStatsExColumns.MonoisotopicMZ)
												.CollisionMode = LookupSplitLineValue(strSplitLine, intColumnMap, eScanStatsExColumns.CollisionMode)
												.ScanFilterText = LookupSplitLineValue(strSplitLine, intColumnMap, eScanStatsExColumns.ScanFilterText)

											End With

											' Store a mapping between intScanNumber and intExtendedStatsInfoCount
											mExtendedStatsPointer.Add(intScanNumber, intExtendedStatsInfoCount)

											intExtendedStatsInfoCount += 1
										End If
									End If

								End If

							Catch ex As Exception
								ShowErrorMessage("Error parsing line " & strLineIn & "; " & ex.Message)
							End Try
						End If

					Loop

				End Using

				' Shrink mExtendedStatsInfo
				ReDim Preserve mExtendedStatsInfo(intExtendedStatsInfoCount - 1)

				blnSuccess = True
			Else
				blnSuccess = False
			End If

        Catch ex As Exception
            ShowErrorMessage("Error reading the ScanStatsEx file (" & strScanStatsExFilePath & "); " & ex.Message)
            blnSuccess = False     
        End Try

        Return blnSuccess

    End Function

    ''' <summary>
    ''' This sub will populate a m/z and intensity array with fake data from 50 to 2500 m/z, at an interval of 0.1 m/z units
    ''' It next calls FilterIonsByMZ for the given parent mono mass and charge
    ''' Finally, it will examine the data
    ''' </summary>
    ''' <param name="dblParentMonoMass"></param>
    ''' <param name="intCharge"></param>
    ''' <remarks></remarks>
    Private Sub LogMatchingIonFilterRange(ByVal dblParentMonoMass As Single, ByVal intCharge As Integer)

        Dim dblMZ As Double
        Dim dblMZStart As Double
        Dim dblMZEnd As Double
        Dim dblMZInterval As Double

        Dim intIndex As Integer
        Dim intDataCount As Integer
        Dim sngMZList() As Single
        Dim sngIntensityList() As Single

		Dim udtSpectrumHeaderInfo As MsMsDataFileReader.clsMsMsDataFileReaderBaseClass.udtSpectrumHeaderInfoType = New MsMsDataFileReader.clsMsMsDataFileReaderBaseClass.udtSpectrumHeaderInfoType
        Dim blnSpectralDataUpdated As Boolean

        Dim sngFilterRangeStart As Single
        Dim sngFilterRangeEnd As Single
        Dim blnInsideFilterRange As Boolean

        Dim objFilteredRanges As Generic.Dictionary(Of Single, Single)
        Dim objEnum As Generic.Dictionary(Of Single, Single).Enumerator

        Dim strParentDesc As String

        Try

            dblMZStart = 50
            dblMZEnd = 2500
            dblMZInterval = 0.1

            ' Compute the expected number of data points
            intDataCount = CInt(Math.Ceiling(dblMZEnd - dblMZStart) / dblMZInterval) + 1

            ReDim sngMZList(intDataCount)
            ReDim sngIntensityList(intDataCount)

            ' Reset the data count and initialize dblMZ
            intDataCount = 0
            dblMZ = dblMZStart

            Do While dblMZ <= dblMZEnd AndAlso intDataCount < sngMZList.Length

                sngMZList(intDataCount) = CSng(dblMZ)
                sngIntensityList(intDataCount) = 10

                dblMZ += dblMZInterval
                intDataCount += 1
            Loop

            With udtSpectrumHeaderInfo
                .ChargeIs2And3Plus = False
                .ParentIonChargeCount = 1
                ReDim .ParentIonCharges(0)
                .ParentIonCharges(0) = intCharge
                .ParentIonMH = CSng(ConvoluteMass(dblParentMonoMass, 0, 1))
                .ParentIonMZ = CSng(ConvoluteMass(dblParentMonoMass, 0, intCharge))
            End With

            ' Filter the data
            blnSpectralDataUpdated = FilterIonsByMZ(intDataCount, sngMZList, sngIntensityList, udtSpectrumHeaderInfo, False, 0, 0)

            ' Now determine which ranges of data have an intensity of 0
            sngFilterRangeStart = 0
            sngFilterRangeEnd = 0
            blnInsideFilterRange = False

            objFilteredRanges = New Generic.Dictionary(Of Single, Single)

            For intIndex = 0 To intDataCount - 1
                If sngIntensityList(intIndex) <= 0 Then
                    If blnInsideFilterRange Then
                        sngFilterRangeEnd = sngMZList(intIndex)
                    Else
                        ' New filter range
                        blnInsideFilterRange = True
                        sngFilterRangeStart = sngMZList(intIndex)
                        sngFilterRangeEnd = sngMZList(intIndex)
                    End If
                Else
                    ' Non-zero intensity
                    If blnInsideFilterRange Then
                        blnInsideFilterRange = False

                        objFilteredRanges.Add(sngFilterRangeStart, sngFilterRangeEnd)
                    End If
                End If
            Next

            strParentDesc = dblParentMonoMass.ToString("0.0") & " Da parent ion observed as " & intCharge.ToString("0") & "+ at " & udtSpectrumHeaderInfo.ParentIonMZ.ToString("0.00") & " m/z"
            If objFilteredRanges.Count = 0 Then
                LogMessage("Current ion filtering options would not remove any ions for " & strParentDesc)
            Else
                LogMessage("Ranges of m/z values that would be removed for " & strParentDesc)
            End If

            objEnum = objFilteredRanges.GetEnumerator
            Do While objEnum.MoveNext
                LogMessage(" " & objEnum.Current.Key.ToString("0.0") & " to " & objEnum.Current.Value.ToString("0.0"))
            Loop

        Catch ex As Exception
            HandleException("Error in LogMatchingIonFilterRange", ex)
        End Try

    End Sub

    Private Function LookupSplitLineValue(ByRef strSplitLine() As String, ByRef intColumnMap() As Integer, ByVal intColIndex As Integer) As String

        If intColumnMap(intColIndex) >= 0 AndAlso intColumnMap(intColIndex) < strSplitLine.Length Then
            Return strSplitLine(intColumnMap(intColIndex))
        Else
            Return String.Empty
        End If

    End Function

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
            HandleException(mErrorMessage, ex)
        End Try

    End Sub

    Protected Function OpenDebugFile(ByVal strSpectrumID As String, ByVal blnAppend As Boolean) As System.IO.StreamWriter
        Dim strDebugFileName As String
        Dim srSpectrumFile As System.IO.StreamWriter

        ' Create a file with debug information
        If strSpectrumID Is Nothing OrElse strSpectrumID.Length = 0 Then
            strDebugFileName = "FilterDebug_CurrentSpectrum.txt"
        Else
            strDebugFileName = "FilterDebug_" & strSpectrumID & ".txt"
        End If
        srSpectrumFile = New System.IO.StreamWriter(strDebugFileName, blnAppend)

        Return srSpectrumFile

    End Function

    ''' <summary>
    ''' Returns true if 1 or more reporter ions are defined for mode 4
    ''' </summary>
    ''' <param name="strList">List of values to parse.  Can be separated by comma, semicolon, or space</param>
    ''' <param name="dblValues">List of doubles extracted from strList</param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Private Function ParseDelimitedIntegerList(ByRef strList As String, _
                                               ByRef dblValues As System.Collections.Generic.List(Of Double)) As Boolean

        Static strSplitLineSaved As String
        Static dblValuesSaved As System.Collections.Generic.List(Of Double)
        Static intSaveCopyUseCount As Integer

        Dim strSplitLine() As String
        Dim dblValue As Double

        Dim intIndex As Integer

        If dblValues Is Nothing Then
            dblValues = New System.Collections.Generic.List(Of Double)
        Else
            dblValues.Clear()
        End If

        If String.IsNullOrEmpty(strList) Then
            Return False
        Else
            If Not String.IsNullOrEmpty(strSplitLineSaved) Then
                ' Return the cached info
                If strSplitLineSaved = strList AndAlso Not dblValuesSaved Is Nothing Then
                    dblValues = dblValuesSaved
                    intSaveCopyUseCount += 1
                    Return True
                End If
            End If

            ' Split strList on commas, spaces, or semicolons
            strSplitLine = strList.Split(New Char() {","c, ";"c, " "c})

            If strSplitLine.Length = 0 Then
                Return False
            Else
                For intIndex = 0 To strSplitLine.Length - 1
                    If Not String.IsNullOrEmpty(strSplitLine(intIndex)) Then
                        If Double.TryParse(strSplitLine(intIndex), dblValue) Then
                            dblValues.Add(dblValue)
                        End If
                    End If
                Next

                strSplitLineSaved = String.Copy(strList)
                dblValuesSaved = dblValues

                If dblValues.Count > 0 Then
                    Return True
                Else
                    Return False
                End If
            End If
        End If
    End Function

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
                ShowErrorMessage(mErrorMessage)
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
            HandleException(mErrorMessage, ex)
        End Try

        Return blnSuccess

    End Function

    ''Protected Function PointPassesSignalToNoise(ByVal intDataPointIndex As Integer, ByVal sngY As Single, ByRef udtBaselineNoiseStatSegments() As clsBaselineNoiseEstimator.udtBaselineNoiseStatSegmentsType, ByVal sngSignalToNoiseThreshold As Single, byref sngSignalToNoise as single) As Boolean
    ''    Dim sngSpectrumNoiseLevel As Single

    ''    sngSpectrumNoiseLevel = clsBaselineNoiseEstimator.ComputeSignalToNoiseUsingMultiSegmentData(intDataPointIndex, sngY, udtBaselineNoiseStatSegments)

    ''    If sngSpectrumNoiseLevel > 0 Then
    ''        sngSignalToNoise = sngY / sngSpectrumNoiseLevel
    ''    Else
    ''        sngSignalToNoise = 0
    ''    End If

    ''    If sngSpectrumNoiseLevel <= 0 OrElse sngSignalToNoise >= sngSignalToNoiseThreshold Then
    ''        Return True
    ''    Else
    ''        Return False
    ''    End If

    ''End Function

    Private Sub PopulateBinnedDataUsingAASpacingMatches(ByRef udtAASpacingMatches() As udtAminoAcidSpacingStatsType, ByVal intAASpacingMatchCount As Integer, ByVal sngMassMinimum As Single, ByVal sngMassMaximum As Single, ByVal sngBinningPrecision As Single, ByRef sngWorkingMasses() As Single, ByRef bytBinnedData() As Byte)

        Dim intBinnedDataCount As Integer

        Dim intBinMinimum As Integer
        Dim intBinMaximum As Integer

        Dim intAASpacingMatchIndex As Integer

        Dim intTargetIndex As Integer
        Dim intTargetIndexStart As Integer
        Dim intTargetIndexEnd As Integer

        intBinMinimum = CInt(Math.Floor(sngMassMinimum))
        intBinMaximum = CInt(Math.Ceiling(sngMassMaximum))

        intBinnedDataCount = CInt(Math.Ceiling(intBinMaximum * (1 / sngBinningPrecision) - intBinMinimum * (1 / sngBinningPrecision) + 1))
        ReDim bytBinnedData(intBinnedDataCount - 1)

        For intAASpacingMatchIndex = 1 To intAASpacingMatchCount - 1
            With udtAASpacingMatches(intAASpacingMatchIndex)
                ' Round sngWorkingMasses() to the nearest 0.5 m/z and determine the index in bytBinnedData to start updating at
                intTargetIndexStart = CalculateTargetBinIndex(sngWorkingMasses(.DataPointIndexLight), intBinMinimum, intBinnedDataCount - 1, sngBinningPrecision)

                ' Round sngWorkingMasses() to the nearest 0.5 m/z and determine the index in bytBinnedData to update through
                intTargetIndexEnd = CalculateTargetBinIndex(sngWorkingMasses(.DataPointIndexHeavy), intBinMinimum, intBinnedDataCount - 1, sngBinningPrecision)

                For intTargetIndex = intTargetIndexStart To intTargetIndexEnd
                    bytBinnedData(intTargetIndex) = 1
                Next intTargetIndex

            End With
        Next intAASpacingMatchIndex

    End Sub

    Private Function PossiblyQuoteName(ByVal strText As String) As String
        Const DOUBLE_QUOTE As Char = """"c

        If strText Is Nothing Then strText = String.Empty

        If strText.IndexOf(" "c) >= 0 Then
            Return DOUBLE_QUOTE & strText & DOUBLE_QUOTE
        Else
            Return strText
        End If

    End Function

    Private Function ProcessDtaFile(ByVal strInputFilePath As String, ByVal strOutputFolderPath As String) As Boolean
        ' Processes a single Dta file
        ' Returns True if success, False if failure
        '
        ' If strOutputFolderPath is empty or is the same folder as strInputFilePath's folder, then if the
        '   spectrum fails the filter, then the file is renamed to .dta.old
        ' Otherwise if strOutputFolderPath points to another folder, then if the spectrum passes the filter,
        '   then it is copied to the output folder

        Dim objDtaTextFileReader As New MsMsDataFileReader.clsDtaTextFileReader

		Dim strMSMSDataList() As String = Nothing
        Dim intMsMsDataCount As Integer
		Dim udtSpectrumHeaderInfo As MsMsDataFileReader.clsMsMsDataFileReaderBaseClass.udtSpectrumHeaderInfoType = New MsMsDataFileReader.clsMsMsDataFileReaderBaseClass.udtSpectrumHeaderInfoType

        Dim intDataCount As Integer
		Dim sngMassList() As Single = Nothing
		Dim sngIntensityList() As Single = Nothing

        Dim udtIonMatchStats As udtIonMatchStatsType

        Dim sngBPI As Single

        Dim intPositiveDataCountBeforeFilter As Integer
        Dim intPositiveDataCountAfterFilter As Integer

        Dim blnSuccess As Boolean
        Dim blnValidOutputFolder As Boolean

        Dim blnIncludeNLStats As Boolean
        Dim blnIncludeReporterIonStats As Boolean

        Dim strNewFilePath As String
		Dim udtSpectrumQualityScore As udtSpectrumQualityScoreType = New udtSpectrumQualityScoreType
        Dim blnKeepSpectrum As Boolean

        Dim blnIonFilteringEnabled As Boolean
        Dim blnSpectralDataUpdated As Boolean
        Dim blnDataCondensed As Boolean

        Dim strInputFileBaseName As String

        Static strInputFileBaseNameSaved As String = String.Empty
        Static intFilesProcessed As Integer = 0
        Static intFileCount As Integer = 0

        Static intMSLevelFilter As Integer = 0

        Static strScanTypeFilter As String = String.Empty
        Static strCollisionModeFilter As String = String.Empty

        Static reScanTypeFilter As System.Text.RegularExpressions.Regex
        Static reCollisionModeFilter As System.Text.RegularExpressions.Regex

		blnIonFilteringEnabled = CheckIonFilteringEnabled()

        blnSuccess = True

        Try
            ReDim udtIonMatchStats.IonIntensitiesNormalized(SearchMassSpecsClass.SEARCH_MASS_CODE_COUNT - 1)

            blnValidOutputFolder = ValidateOutputFolder(strInputFilePath, strOutputFolderPath)

            ' Extract the text up to the first "." in strInputFilePath, then store in strInputFileBaseName
            strInputFileBaseName = System.IO.Path.GetFileNameWithoutExtension(strInputFilePath)
            If strInputFileBaseName.IndexOf("."c) > 0 Then
                strInputFileBaseName = strInputFileBaseName.Substring(0, strInputFileBaseName.IndexOf("."c))
            End If

            ' Only lookup the number of .Dta files on the first call to this function, or if the base name of the DTA file changes
            If intFileCount = 0 OrElse strInputFileBaseNameSaved Is Nothing OrElse strInputFileBaseNameSaved <> strInputFileBaseName Then
                strInputFileBaseNameSaved = String.Copy(strInputFileBaseName)
                intFileCount = System.IO.Directory.GetFiles(System.IO.Path.GetDirectoryName(strInputFilePath), "*.dta").GetLength(0)
                intFilesProcessed = 0

                ' Examine mMSLevelFilter, mScanTypeFilter, and mMSCollisionModeFilter
                ' If mMSLevelFilter is > 0, or is not blank, or mMSCollisionModeFilter is not blank, then look for or generate the scan stats files
                ' If the scan stats ex file is found (or generated), then loads it and populates strCollisionModeFilter using mMSCollisionModeFilter
                ProcessWorkCheckForAndGenerateScanStatsFile(strInputFilePath, intMSLevelFilter, strScanTypeFilter, strCollisionModeFilter)

                If mScanTypeMatchType = eTextMatchTypeConstants.RegEx Then
                    reScanTypeFilter = New System.Text.RegularExpressions.Regex(strScanTypeFilter, Text.RegularExpressions.RegexOptions.Compiled Or Text.RegularExpressions.RegexOptions.IgnoreCase)
                End If

                If mMSCollisionModeMatchType = eTextMatchTypeConstants.RegEx Then
                    reCollisionModeFilter = New System.Text.RegularExpressions.Regex(strCollisionModeFilter, Text.RegularExpressions.RegexOptions.Compiled Or Text.RegularExpressions.RegexOptions.IgnoreCase)
                End If

                WriteSettingsToLog(strInputFilePath, "", intMSLevelFilter, strScanTypeFilter, strCollisionModeFilter, mEvaluateSpectrumQualityOnly)
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
                intPositiveDataCountBeforeFilter = 0
                intPositiveDataCountAfterFilter = 0

                If intDataCount > 0 Then
                    udtSpectrumQualityScore = EvaluateMsMsSpectrumStart(sngMassList, sngIntensityList, _
                                                                        udtSpectrumHeaderInfo, _
                                                                        sngBPI, _
                                                                        blnIncludeNLStats, _
                                                                        blnIncludeReporterIonStats, _
                                                                        udtIonMatchStats)

                    If blnIonFilteringEnabled Then
                        Const blnRemoveIons As Boolean = False
                        blnSpectralDataUpdated = FilterIonsByMZ(intDataCount, sngMassList, sngIntensityList, udtSpectrumHeaderInfo, blnRemoveIons, intPositiveDataCountBeforeFilter, intPositiveDataCountAfterFilter)

                        If mIonFilterOptions.CondenseData Then
                            blnDataCondensed = CondenseData(intDataCount, sngMassList, sngIntensityList, intPositiveDataCountAfterFilter)
                            If blnDataCondensed Then
                                blnSpectralDataUpdated = True
                            End If
                        End If
					Else
						blnSpectralDataUpdated = False
					End If

					If blnSpectralDataUpdated Or mSpectrumFilterMode = eSpectrumFilterMode.mode4 Then
						' Need to re-create the .DTA file
						WriteDTAFile(strInputFilePath, udtSpectrumHeaderInfo.ParentIonLineText, intDataCount, sngMassList, sngIntensityList)
					End If
				Else
					udtSpectrumQualityScore.Initialize()
					udtSpectrumQualityScore.SpectrumQualityScore = -1
					blnSpectralDataUpdated = False
				End If

				HandleEvaluationResults(GetReportFileName(strInputFilePath, strOutputFolderPath), _
					   udtSpectrumHeaderInfo, udtSpectrumQualityScore, sngBPI, _
					   blnIncludeNLStats, blnIncludeReporterIonStats, udtIonMatchStats, _
					   intMSLevelFilter, _
					   strScanTypeFilter, reScanTypeFilter, _
					   strCollisionModeFilter, reCollisionModeFilter, _
					   blnKeepSpectrum, _
					   blnIonFilteringEnabled, intPositiveDataCountBeforeFilter, intPositiveDataCountAfterFilter)


				If blnKeepSpectrum Then

					If blnValidOutputFolder Then
						'if the user provided a valid output folder then we move .dta files to the output folder:
						strNewFilePath = System.IO.Path.Combine(strOutputFolderPath, System.IO.Path.GetFileName(strInputFilePath))

						If Not CheckExistingFile(strNewFilePath, True) Then
							Exit Try
						Else
							Try
								System.IO.File.Copy(strInputFilePath, strNewFilePath)
							Catch ex As Exception
								mErrorMessage = "Error copying " & strInputFilePath & " to " & strNewFilePath
								SetLocalErrorCode(eFilterMsMsSpectraErrorCodes.FileCopyError)
								HandleException(mErrorMessage, ex)
							End Try
						End If
					Else
						'file passed filter and since no output folder was provided we keep it in its present folder
					End If

				Else

					If blnValidOutputFolder Then
						'we do nothing
						'we do not want to move the unwanted spectra to the output folder 
					Else
						If mDeleteBadDTAFiles Then
							'no output folder was provided and mDeleteBadDTAFiles = True, so delete the .dta file
							Try
								System.IO.File.Delete(strInputFilePath)
							Catch ex As Exception
								mErrorMessage = "Error deleting " & strInputFilePath
								SetLocalErrorCode(eFilterMsMsSpectraErrorCodes.FileDeleteError)
								HandleException(mErrorMessage, ex)
							End Try
						Else
							'no output folder was provided so we rename the unwanted spectra to .bad since mDeleteBadDTAFiles = False
							strNewFilePath = System.IO.Path.ChangeExtension(strInputFilePath, ".bad")

							If Not CheckExistingFile(strNewFilePath, True) Then
								Exit Try
							Else
								Try
									System.IO.File.Copy(strInputFilePath, strNewFilePath)

									Try
										System.IO.File.Delete(strInputFilePath)
									Catch ex As Exception
										mErrorMessage = "Error deleting " & strInputFilePath
										SetLocalErrorCode(eFilterMsMsSpectraErrorCodes.FileDeleteError)
										HandleException(mErrorMessage, ex)
									End Try

								Catch ex As Exception
									mErrorMessage = "Error copying " & strInputFilePath & " to " & strNewFilePath
									SetLocalErrorCode(eFilterMsMsSpectraErrorCodes.FileCopyError)
									HandleException(mErrorMessage, ex)
								End Try
							End If

						End If
					End If

				End If

			End If


		Catch ex As Exception
			mErrorMessage = "Error in ProcessDtaFile: " & ex.Message
			SetLocalErrorCode(eFilterMsMsSpectraErrorCodes.InputFileAccessError)

			HandleException(mErrorMessage, ex)
			blnSuccess = False
		End Try

        Return blnSuccess

    End Function

    Private Function ProcessDtaTxtFile(ByVal strInputFilePath As String, ByVal strOutputFolderPath As String) As Boolean

        Dim blnCombineIdenticalSpectra As Boolean
        blnCombineIdenticalSpectra = False

        Dim objDtaTextFileReader As MsMsDataFileReader.clsMsMsDataFileReaderBaseClass
        objDtaTextFileReader = New MsMsDataFileReader.clsDtaTextFileReader(blnCombineIdenticalSpectra)

        Return ProcessDTATextOrMGF(objDtaTextFileReader, eInputFileModeConstants.ConcatenatedDTA, strInputFilePath, strOutputFolderPath)

    End Function

    Private Function ProcessMascotGenericFile(ByVal strInputFilePath As String, ByVal strOutputFolderPath As String) As Boolean

        Dim objMGFReader As MsMsDataFileReader.clsMsMsDataFileReaderBaseClass
        objMGFReader = New MsMsDataFileReader.clsMGFReader

        Return ProcessDTATextOrMGF(objMGFReader, eInputFileModeConstants.MGF, strInputFilePath, strOutputFolderPath)

    End Function

    Private Function ProcessDTATextOrMGF(ByRef objFileReader As MsMsDataFileReader.clsMsMsDataFileReaderBaseClass, _
                                         ByVal eInputFileMode As eInputFileModeConstants, _
                                         ByVal strInputFilePath As String, _
                                         ByVal strOutputFolderPath As String) As Boolean

		Dim srOutFile As System.IO.StreamWriter = Nothing

		Dim strMSMSDataList() As String = Nothing
		Dim udtSpectrumHeaderInfo As MsMsDataFileReader.clsMsMsDataFileReaderBaseClass.udtSpectrumHeaderInfoType = New MsMsDataFileReader.clsMsMsDataFileReaderBaseClass.udtSpectrumHeaderInfoType

        Dim intDataCount, intMsMsDataCount As Integer
        Dim intProcessCount As Integer
        Dim intMSSpectraCountIonFiltered As Integer
        Dim intMSSpectraCountPassingFilter As Integer

        Dim intPositiveDataCountBeforeFilter As Integer
        Dim intPositiveDataCountAfterFilter As Integer

		Dim sngMassList() As Single = Nothing
		Dim sngIntensityList() As Single = Nothing
        Dim udtIonMatchStats As udtIonMatchStatsType

        Dim sngBPI As Single

        Dim strOutputFilePath As String
        Dim strReportFilePath As String
        Dim strInputFilePathOriginal As String
        Dim strMostRecentSpectrumText As String = String.Empty

        Dim blnValidOutputFolder As Boolean

        Dim blnProceed As Boolean
        Dim blnSuccess As Boolean
        Dim blnIncludeNLStats As Boolean
        Dim blnIncludeReporterIonStats As Boolean

        Dim blnSpectrumFound As Boolean
		Dim udtSpectrumQualityScore As udtSpectrumQualityScoreType = New udtSpectrumQualityScoreType
        Dim blnKeepSpectrum As Boolean

        Dim blnIonFilteringEnabled As Boolean
        Dim blnSpectralDataUpdated As Boolean
        Dim blnDataCondensed As Boolean

        Dim dtLastProgressUpdate As DateTime
        Dim intSpectraRead As Integer
        Dim intProgressPercentComplete As Integer

        Dim intMSLevelFilter As Integer = 0

        Dim strScanTypeFilter As String = String.Empty
        Dim strCollisionModeFilter As String = String.Empty

		Dim reScanTypeFilter As System.Text.RegularExpressions.Regex = Nothing
		Dim reCollisionModeFilter As System.Text.RegularExpressions.Regex = Nothing

        Try
            ReDim udtIonMatchStats.IonIntensitiesNormalized(SearchMassSpecsClass.SEARCH_MASS_CODE_COUNT - 1)

            blnValidOutputFolder = ValidateOutputFolder(strInputFilePath, strOutputFolderPath)
            strInputFilePathOriginal = String.Copy(strInputFilePath)

            If blnValidOutputFolder Then
                ' Note that we do not create backups of files to be overwritten if an Output Folder is defined
                strOutputFilePath = System.IO.Path.Combine(strOutputFolderPath, System.IO.Path.GetFileName(strInputFilePath))

                blnProceed = CheckExistingFile(strOutputFilePath)
                If Not blnProceed Then Exit Try

                blnProceed = True
            Else
                ' Create a backup copy of the input file, unless mEvaluateSpectrumQualityOnly = True
                If mEvaluateSpectrumQualityOnly Then
                    strOutputFilePath = String.Empty
                    blnProceed = True
                Else
                    blnProceed = BackupFileWithRevisioning(strInputFilePath, False)

                    ' Switch around the filenames as needed
                    strOutputFilePath = strInputFilePath
                    strInputFilePath = strInputFilePath & ".bak"
                End If
            End If

            If Not blnProceed Then Exit Try

            ' Examine mMSLevelFilter, mScanTypeFilter, and mMSCollisionModeFilter
            ' If mMSLevelFilter is > 0, or is not blank, or mMSCollisionModeFilter is not blank, then look for or generate the scan stats files
            ' If the scan stats ex file is found (or generated), then loads it and populates strCollisionModeFilter using mMSCollisionModeFilter
            ProcessWorkCheckForAndGenerateScanStatsFile(strInputFilePathOriginal, intMSLevelFilter, strScanTypeFilter, strCollisionModeFilter)

            If mScanTypeMatchType = eTextMatchTypeConstants.RegEx Then
                reScanTypeFilter = New System.Text.RegularExpressions.Regex(strScanTypeFilter, Text.RegularExpressions.RegexOptions.Compiled Or Text.RegularExpressions.RegexOptions.IgnoreCase)
            End If

            If mMSCollisionModeMatchType = eTextMatchTypeConstants.RegEx Then
                reCollisionModeFilter = New System.Text.RegularExpressions.Regex(strCollisionModeFilter, Text.RegularExpressions.RegexOptions.Compiled Or Text.RegularExpressions.RegexOptions.IgnoreCase)
            End If

            If Not mEvaluateSpectrumQualityOnly Then
                ' Create the output file
                srOutFile = New IO.StreamWriter(strOutputFilePath)

                ' Write a blank line to the start of the output file
                srOutFile.WriteLine()
            End If

            ' Open the input file and parse it
            If Not objFileReader.OpenFile(strInputFilePath) Then
                LogMessage("Error opening " & strInputFilePath)
                SetLocalErrorCode(eFilterMsMsSpectraErrorCodes.InputFileAccessError)
                blnSuccess = False
                Exit Try
            End If

            MyBase.ShowMessage("Filtering MsMs Spectra: ", False, True)

            WriteSettingsToLog(strInputFilePath, strOutputFilePath, intMSLevelFilter, strScanTypeFilter, strCollisionModeFilter, mEvaluateSpectrumQualityOnly)

            intProcessCount = 0
            intMSSpectraCountIonFiltered = 0
            intMSSpectraCountPassingFilter = 0

            blnIonFilteringEnabled = CheckIonFilteringEnabled()

            If mMaximumProgressUpdateIntervalSeconds < 0.2 Then mMaximumProgressUpdateIntervalSeconds = 0.2

            Console.Write("  ")

            If blnValidOutputFolder Then
                strReportFilePath = GetReportFileName(strOutputFilePath, strOutputFolderPath)
            Else
                strReportFilePath = GetReportFileName(strInputFilePathOriginal, System.IO.Path.GetDirectoryName(strInputFilePathOriginal))
            End If

            Do
                Try

                    ' Read the next spectrum
                    blnSpectrumFound = objFileReader.ReadNextSpectrum(strMSMSDataList, intMsMsDataCount, udtSpectrumHeaderInfo)
                    intSpectraRead += 1

                    If intSpectraRead Mod 1000 = 1 OrElse System.DateTime.UtcNow.Subtract(dtLastProgressUpdate).TotalSeconds >= mMaximumProgressUpdateIntervalSeconds Then
                        dtLastProgressUpdate = System.DateTime.UtcNow

                        ' Update the label with the progress
                        intProgressPercentComplete = CInt(Math.Round(objFileReader.ProgressPercentComplete(), 0))
                        UpdateProgress("Filtering Concatenated File: " & intProcessCount.ToString & " spectra processed", intProgressPercentComplete)
                    End If

                    If blnSpectrumFound Then

                        If mSpectrumFilterMode = eSpectrumFilterMode.NoFilter AndAlso Not blnIonFilteringEnabled Then
                            ' We do not need to populate sngMassList or sngIntensityList
                            ' Simply call EvaluateMsMsSpectrumStart so that udtSpectrumQualityScore gets updated
                            udtSpectrumQualityScore = EvaluateMsMsSpectrumStart(Nothing, Nothing, _
                                                                                udtSpectrumHeaderInfo, _
                                                                                sngBPI, _
                                                                                blnIncludeNLStats, _
                                                                                blnIncludeReporterIonStats, _
                                                                                udtIonMatchStats)

							intDataCount = intMsMsDataCount
							intPositiveDataCountAfterFilter = intDataCount

                            strMostRecentSpectrumText = objFileReader.GetMostRecentSpectrumFileText

                        Else
                            ' Populate sngMassList and sngIntensityList
                            intDataCount = objFileReader.ParseMsMsDataList(strMSMSDataList, intMsMsDataCount, sngMassList, sngIntensityList)
                            intPositiveDataCountBeforeFilter = 0
                            intPositiveDataCountAfterFilter = 0

                            If intDataCount > 0 Then
                                ' Call EvaluateMsMsSpectrum()
                                udtSpectrumQualityScore = EvaluateMsMsSpectrumStart(sngMassList, sngIntensityList, _
                                                                                    udtSpectrumHeaderInfo, _
                                                                                    sngBPI, _
                                                                                    blnIncludeNLStats, _
                                                                                    blnIncludeReporterIonStats, _
                                                                                    udtIonMatchStats)

                                If blnIonFilteringEnabled Then
                                    Const blnRemoveIons As Boolean = False
                                    blnSpectralDataUpdated = FilterIonsByMZ(intDataCount, sngMassList, sngIntensityList, udtSpectrumHeaderInfo, blnRemoveIons, intPositiveDataCountBeforeFilter, intPositiveDataCountAfterFilter)
                                Else
                                    blnSpectralDataUpdated = False
                                End If

                                If mIonFilterOptions.CondenseData Then
                                    blnDataCondensed = CondenseData(intDataCount, sngMassList, sngIntensityList, intPositiveDataCountAfterFilter)
                                    If blnDataCondensed Then
                                        blnSpectralDataUpdated = True
                                    End If
                                End If

                                If blnSpectralDataUpdated Or mSpectrumFilterMode = eSpectrumFilterMode.mode4 Then
                                    ' Note: we always re-create the .DTA file when using filter mode 4 since the parent ion mass may have been updated

                                    intMSSpectraCountIonFiltered += 1

                                    ' Need to re-create the .DTA file
                                    Select Case eInputFileMode
                                        Case eInputFileModeConstants.ConcatenatedDTA
                                            strMostRecentSpectrumText = ControlChars.NewLine & _
                                                                        udtSpectrumHeaderInfo.SpectrumTitleWithCommentChars & ControlChars.NewLine

                                            strMostRecentSpectrumText &= WriteDTAFileToString(udtSpectrumHeaderInfo.ParentIonLineText, intDataCount, sngMassList, sngIntensityList)

                                        Case eInputFileModeConstants.MGF
                                            strMostRecentSpectrumText = ControlChars.NewLine & _
                                                                        WriteMGFEntryToString(udtSpectrumHeaderInfo, intDataCount, sngMassList, sngIntensityList)

                                        Case Else
                                            ' Unknown mode
                                            strMostRecentSpectrumText = objFileReader.GetMostRecentSpectrumFileText
                                    End Select
                                Else
                                    strMostRecentSpectrumText = objFileReader.GetMostRecentSpectrumFileText
                                End If
                            End If
                        End If


                        If intDataCount > 0 Then

                            HandleEvaluationResults(strReportFilePath, _
                                   udtSpectrumHeaderInfo, udtSpectrumQualityScore, sngBPI, _
                                   blnIncludeNLStats, blnIncludeReporterIonStats, udtIonMatchStats, _
                                   intMSLevelFilter, _
                                   strScanTypeFilter, reScanTypeFilter, _
                                   strCollisionModeFilter, reCollisionModeFilter, _
                                   blnKeepSpectrum, _
                                   blnIonFilteringEnabled, intPositiveDataCountBeforeFilter, intPositiveDataCountAfterFilter)

                            If blnKeepSpectrum Then
                                intMSSpectraCountPassingFilter += 1

                                If Not srOutFile Is Nothing Then

                                    If strMostRecentSpectrumText.StartsWith(ControlChars.NewLine) AndAlso strMostRecentSpectrumText.Length > 2 Then
                                        ' Skip the first two characters since we don't want to blank lines between DTA entries; just one blank line
                                        srOutFile.Write(strMostRecentSpectrumText.Substring(2))
                                    Else
                                        srOutFile.Write(strMostRecentSpectrumText)
                                    End If

                                    If Not strMostRecentSpectrumText.EndsWith(ControlChars.NewLine & ControlChars.NewLine) Then
                                        srOutFile.WriteLine()
                                    End If
                                End If
                            End If
                        Else
                            ' Spectrum doesn't have any data; skip it
                        End If

                        intProcessCount += 1
                        If intProcessCount Mod 100 = 0 Then
                            Console.Write(".")
                        End If
                    End If
                Catch ex As Exception
                    ' Error reading or parsing this spectrum; go on to the next one
                    ShowErrorMessage("Error reading spectrum (intSpectraRead = " & intSpectraRead.ToString & ": " & ex.Message)
                End Try

                If mAbortProcessing Then
                    ShowMessage("Processing aborted", True, True)
                    Exit Do
                End If
            Loop While blnSpectrumFound

            If intProcessCount >= 100 Then Console.WriteLine()

            objFileReader.CloseFile()
            If Not srOutFile Is Nothing Then
                srOutFile.Close()
            End If

            ShowMessage("Processing complete; processed " & intProcessCount & " spectra")
            ShowMessage("Spectrum count passing filters: " & intMSSpectraCountPassingFilter)
            ShowMessage("Spectrum count with m/z values removed by ion filtering: " & intMSSpectraCountIonFiltered)

            blnSuccess = True

        Catch ex As Exception
            mErrorMessage = "Error in ProcessDTATextOrMGF: " & ex.Message
            SetLocalErrorCode(eFilterMsMsSpectraErrorCodes.InputFileAccessError)
            HandleException(mErrorMessage, ex)
            blnSuccess = False
        End Try

        Return blnSuccess

    End Function

    ' Main processing function (utilizes EvaluateMsMsSpectrum)
    Public Overloads Overrides Function ProcessFile(ByVal strInputFilePath As String, ByVal strOutputFolderPath As String, ByVal strParameterFilePath As String, ByVal blnResetErrorCode As Boolean) As Boolean
        ' Returns True if success, False if failure
        ' This function can process .Dta, .MGF, or _Dta.txt files

        Dim ioFile As System.IO.FileInfo
        Dim strInputFilePathFull As String

        Dim blnSuccess As Boolean

        ' Make sure the required DLLs are present in the working directory
        If Not ValidateRequiredDLLs() Then
            Return False
        End If

        If blnResetErrorCode Then
            SetLocalErrorCode(eFilterMsMsSpectraErrorCodes.NoError)
        End If

        If Not LoadParameterFileSettings(strParameterFilePath) Then
            mErrorMessage = "Parameter file load error: " & strParameterFilePath
            ShowErrorMessage(mErrorMessage)
            If MyBase.ErrorCode = clsProcessFilesBaseClass.eProcessFilesErrorCodes.NoError Then
                MyBase.SetBaseClassErrorCode(clsProcessFilesBaseClass.eProcessFilesErrorCodes.InvalidParameterFile)
            End If
            Return False
        End If

        Try
            If strInputFilePath Is Nothing OrElse strInputFilePath.Length = 0 Then
                ShowErrorMessage("Input file name is empty")
                MyBase.SetBaseClassErrorCode(clsProcessFilesBaseClass.eProcessFilesErrorCodes.InvalidInputFilePath)
            Else

                If Not CleanupFilePaths(strInputFilePath, strOutputFolderPath) Then
                    MyBase.SetBaseClassErrorCode(clsProcessFilesBaseClass.eProcessFilesErrorCodes.FilePathError)
                Else
                    Try

                        If mSpectrumFilterMode = eSpectrumFilterMode.mode2 AndAlso _
                           Not mFilterMode2Options.SequestParamFilePath Is Nothing AndAlso _
                           mFilterMode2Options.SequestParamFilePath.Length > 0 Then
                            blnSuccess = ParseSequestParamFile(mFilterMode2Options.SequestParamFilePath)
                            If Not blnSuccess Then Exit Try
                        End If

                        ' Obtain the full path to the input file
                        ioFile = New System.IO.FileInfo(strInputFilePath)
                        strInputFilePathFull = ioFile.FullName

                        If System.IO.Path.GetExtension(strInputFilePathFull).ToUpper = DTA_EXTENSION Then
                            blnSuccess = ProcessDtaFile(strInputFilePathFull, strOutputFolderPath)

                        ElseIf strInputFilePathFull.ToUpper.EndsWith(DTA_TXT_EXTENSION) OrElse _
                               strInputFilePathFull.ToUpper.EndsWith(FHT_TXT_EXTENSION) Then
                            MyBase.ShowMessage("Parsing " & System.IO.Path.GetFileName(strInputFilePath))
                            blnSuccess = ProcessDtaTxtFile(strInputFilePathFull, strOutputFolderPath)

                        ElseIf strInputFilePathFull.ToUpper.EndsWith(MGF_EXTENSION) Then
                            MyBase.ShowMessage("Parsing " & System.IO.Path.GetFileName(strInputFilePath))
                            blnSuccess = ProcessMascotGenericFile(strInputFilePathFull, strOutputFolderPath)

                        Else
                            ' Unknown file extension
                            mErrorMessage = "Unknown file extension: " & System.IO.Path.GetExtension(strInputFilePathFull)
                            SetLocalErrorCode(eFilterMsMsSpectraErrorCodes.UnknownFileExtension)
                            blnSuccess = False
                        End If

                        If Not blnSuccess Then
                            SetLocalErrorCode(eFilterMsMsSpectraErrorCodes.InputFileAccessError, True)
                        End If

                        If mAutoCloseReportFile Then CloseReportFile()

                    Catch ex As Exception
                        mErrorMessage = "Error calling ProcessDtaFile or ProcessDtaTxtFile: " & ex.Message
                        HandleException(mErrorMessage, ex)
                    End Try
                End If
            End If
        Catch ex As Exception
            mErrorMessage = "Error in ProcessFile: " & ex.Message
            HandleException(mErrorMessage, ex)
        End Try

        Return blnSuccess

    End Function

    Private Sub ProcessWorkCheckForAndGenerateScanStatsFile(ByVal strInputFilePath As String, _
                                                            ByRef intMSLevelFilter As Integer, _
                                                            ByRef strScanTypeFilter As String, _
                                                            ByRef strCollisionModeFilter As String)

        ' Examines mMSLevelFilter and mMSCollisionModeFilter
        ' If mMSLevelFilter is > 0 or if mMSCollisionModeFilter is not blank, then we need to load the scan stats files
        ' First looks for the _ScanStats.txt and _ScanStatsEx.txt files; if found, loads them
        ' Otherwise, if a Finnigan .Raw file exists, then the Finnigan_Datafile_Info_Scanner is called to generate the _ScanStats.xt file and the _ScanStatsEx.txt file

        ' If a scan stats file is found (or successfully created), then loads the file, populating mMSLevelFilter
        ' If a scan stats ex file is found (or successfully created), then loads the file, populating mExtendedStatsInfo

        ' Will update intMSLevelFilter to mMSLevelFilter if the ScanStats file is successfully loaded; otherwise sets intMSLevelFilter to 0
        ' Will update strScanTypeFilter to mScanTypeFilter if the ScanStats file is successfully loaded; otherwise, sets strScanTypeFilter to ""
        ' Will update strCollisionModeFilter to mMSCollisionModeFilter if the ScanStatsEx file is successfully loaded; otherwise sets strCollisionModeFilter to ""

        Dim blnScanStatsFilesExist As Boolean
        Dim ioFile As System.IO.FileInfo

        Dim strBaseDatasetName As String
        Dim strScanStatsFilePath As String
        Dim strScanStatsExFilePath As String

        Dim strFinniganRawFilePath As String

        Dim strMessage As String

        intMSLevelFilter = 0
        strScanTypeFilter = String.Empty
        strCollisionModeFilter = String.Empty

        mScanStatsInfoLoaded = False
        mExtendedStatsInfoLoaded = False

        If mMSLevelFilter > 0 OrElse mScanTypeFilter.Length > 0 OrElse mMSCollisionModeFilter.Length > 0 Then

            Try
                strMessage = "Looking for ScanStats files for " & strInputFilePath
                ShowMessage("  " & strMessage)

                TraceLog("")
                TraceLog(strMessage)

                ioFile = New System.IO.FileInfo(strInputFilePath)

                strBaseDatasetName = GetBaseDatasetNameFromFileName(ioFile.Name)

                ' See if the ScanStats files already exist
                blnScanStatsFilesExist = CheckForExistingScanStatsFiles(ioFile.DirectoryName, strBaseDatasetName)

                strScanStatsFilePath = ConstructScanStatsFilePath(ioFile.DirectoryName, strBaseDatasetName)
                strScanStatsExFilePath = ConstructScanStatsExFilePath(ioFile.DirectoryName, strBaseDatasetName)

                If Not blnScanStatsFilesExist Then
                    ' See if a .Raw file matching strInputFileBaseName exists
                    ' If it does, process the .Raw file using Finnigan_Datafile_Info_Scanner.exe 
                    '  so that we can obtain MSLevel, Scan Type, and collision mode information

                    strFinniganRawFilePath = System.IO.Path.Combine(ioFile.DirectoryName, strBaseDatasetName & ".raw")

                    TraceLog("ScanStats.txt file not found at: " & strScanStatsFilePath)

                    If System.IO.File.Exists(strFinniganRawFilePath) Then
                        strMessage = "Generating _ScanStats.txt file using " & strFinniganRawFilePath
                        ShowMessage("  " & strMessage)
                        TraceLog(strMessage)

                        UpdateProgress("Generating ScanStats file; please wait ", 0)

                        If Not GenerateFinniganScanStatsFiles(strFinniganRawFilePath) Then
                            TraceLog("GenerateFinniganScanStatsFiles returned False")
                            strScanStatsFilePath = String.Empty
                        End If
                    Else
                        strMessage = "Warning, raw file not found at: " & strFinniganRawFilePath
                        ShowMessage(strMessage)
                        TraceLog(strMessage)
                    End If
                End If

                If System.IO.File.Exists(strScanStatsFilePath) Then
                    TraceLog("Loading " & strScanStatsFilePath)
                    If LoadScanStatsFile(strScanStatsFilePath) Then
                        mScanStatsInfoLoaded = True

                        intMSLevelFilter = mMSLevelFilter
                        strScanTypeFilter = String.Copy(mScanTypeFilter)

                        ShowMessage("  MSLevel info and Scan Type info successfully loaded from " & System.IO.Path.GetFileName(strScanStatsFilePath))
                    Else
                        LogMessage("Load failed for: " & strScanStatsFilePath)
                    End If
                Else
                    strMessage = "ScanStats.txt file still not found after calling GenerateFinniganScanStatsFiles"
                    ShowMessage(strMessage)
                    TraceLog(strMessage)
                End If

                If System.IO.File.Exists(strScanStatsExFilePath) Then
                    TraceLog("Loading " & strScanStatsExFilePath)
                    If LoadScanStatsExFile(strScanStatsExFilePath) Then
                        mExtendedStatsInfoLoaded = True
                        strCollisionModeFilter = String.Copy(mMSCollisionModeFilter)

                        ShowMessage("  Collision mode info successfully loaded from " & System.IO.Path.GetFileName(strScanStatsExFilePath))
                    Else
                        LogMessage("Load failed for: " & strScanStatsExFilePath)
                    End If
                Else
                    strMessage = "ScanStatsEx.txt file not found at " & strScanStatsExFilePath
                    ShowMessage(strMessage)
                    TraceLog(strMessage)
                End If

            Catch ex As Exception
                HandleException("Error in ProcessWorkCheckForAndGenerateScanStatsFile", ex)
            End Try

        End If

    End Sub

    Private Function RunProgram(ByVal strFilePath As String, ByVal strArguments As String, ByVal blnThrowExceptions As Boolean) As Boolean
        TraceLog("Call Me.RunProgram, passing strWorkingdirectory:=String.Empty and eWindowStyle:=ProcessWindowStyle.Hidden")
        Return Me.RunProgram(strFilePath, String.Empty, strArguments, blnThrowExceptions, False, ProcessWindowStyle.Hidden)
    End Function

    Private Function RunProgram(ByVal strFilePath As String, ByVal strWorkingDirectory As String, ByVal strArguments As String, ByVal blnThrowExceptions As Boolean) As Boolean
        TraceLog("Call Me.RunProgram, passing strWorkingdirectory:=" & strWorkingDirectory & " and eWindowStyle:=ProcessWindowStyle.Hidden")
        Return Me.RunProgram(strFilePath, strWorkingDirectory, strArguments, blnThrowExceptions, False, ProcessWindowStyle.Hidden)
    End Function

    Private Function RunProgram(ByVal strFilePath As String, ByVal strWorkingDirectory As String, ByVal strArguments As String, ByVal blnThrowExceptions As Boolean, ByVal blnCreateWindow As Boolean, ByVal eWindowStyle As System.Diagnostics.ProcessWindowStyle) As Boolean
        ' Initialize objProgramRunner

        Const MAX_PROGRESS_DOTS As Integer = 10
        Dim ioFileInfo As System.IO.FileInfo
        Dim objProgramRunner As clsProgRunnerThreaded

        Dim blnSuccess As Boolean
        Dim intWaitIteration As Integer
        Dim intSleepTimeMSec As Integer

        Try
            TraceLog("Instantiate objProgRunner")
            objProgramRunner = New clsProgRunnerThreaded

            ioFileInfo = New System.IO.FileInfo(strFilePath)
            If strWorkingDirectory Is Nothing OrElse strWorkingDirectory.Length = 0 Then
                strWorkingDirectory = ioFileInfo.DirectoryName
            End If

            TraceLog("strWorkingDirectory = " & strWorkingDirectory)
            TraceLog("strFilePath = " & strFilePath)
            TraceLog("strArguments = " & strArguments)

        Catch ex As Exception
            ShowErrorMessage("Error instantiating objProgramRunner: " & ex.Message)
            Return False
        End Try

        With objProgramRunner
            .Program = strFilePath
            .WorkDir = strWorkingDirectory
            .Arguments = strArguments
            .CreateNoWindow = Not blnCreateWindow
            .EchoOutputToConsole = True
            .WindowStyle = eWindowStyle

            .MonitoringInterval = 500               ' msec
            .Repeat = False
            .RepeatHoldOffTime = 0

            'Start the program executing
            .StartAndMonitorProgram()

            intSleepTimeMSec = CInt(mMaximumProgressUpdateIntervalSeconds * 1000)
            If intSleepTimeMSec < 500 Then
                intSleepTimeMSec = 500
            ElseIf intSleepTimeMSec > 15000 Then
                intSleepTimeMSec = 15000
            End If

            'loop until program is complete
            intWaitIteration = 0
            While (.State <> 0) And (.State <> 10)

                System.Threading.Thread.Sleep(intSleepTimeMSec)

                intWaitIteration += 1
                If intWaitIteration > MAX_PROGRESS_DOTS AndAlso intWaitIteration Mod MAX_PROGRESS_DOTS = 0 Then
                    mProgressStepDescription = mProgressStepDescription.Substring(0, mProgressStepDescription.Length - MAX_PROGRESS_DOTS)
                End If

                UpdateProgress(mProgressStepDescription & ".")
            End While

            'If .State = 10 Or (blnUseResultCode And ProgRunner.ExitCode <> 0) Then
            If .State = 10 Then
                blnSuccess = False
            Else
                blnSuccess = True
            End If
        End With

        objProgramRunner = Nothing
        Return blnSuccess

    End Function

    ''' <summary>
    ''' Looks at the current settings to determine if the _ScanStats.txt and _ScanStatsEx.txt files are required in order to perform the filtering
    ''' </summary>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public Function ScanStatsFileIsRequired() As Boolean

        If mScanTypeFilter Is Nothing Then mScanTypeFilter = String.Empty
        If mMSCollisionModeFilter Is Nothing Then mMSCollisionModeFilter = String.Empty

        If mMSLevelFilter > 0 OrElse mScanTypeFilter.Length > 0 OrElse mMSCollisionModeFilter.Length > 0 Then
            Return True
        Else
            Return False
        End If
    End Function

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

        Dim intLinesRead As Integer

        Dim intEntryCount As Integer
        Dim udtSpectrumQualityList() As udtSpectrumQualityEntryType

        Dim srInFile As System.IO.StreamReader
        Dim swOutFile As System.IO.StreamWriter

        Dim strHeaderLine As String = String.Empty

        Dim intIndex As Integer
        Dim strLineIn As String
        Dim strSplitLine() As String

        Dim chSepChars() As Char = New Char() {ControlChars.Tab}

        ReDim udtSpectrumQualityList(999)

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
                    ' We're passing "4" to the .Split() function since we need to parse out the first 3 columns, 
                    '  but we want to lump the 4th column (and beyond) into strSplitLine(3)
                    strSplitLine = strLineIn.Split(chSepChars, 4)

                    If intEntryCount >= udtSpectrumQualityList.Length Then
                        ReDim Preserve udtSpectrumQualityList(udtSpectrumQualityList.Length * 2 - 1)
                    End If

                    If strSplitLine.Length >= 4 Then
                        With udtSpectrumQualityList(intEntryCount)
                            .ScanNumberStart = CInt(strSplitLine(0))
                            .ScanNumberEnd = CInt(strSplitLine(1))
                            .Charge = CInt(strSplitLine(2))
                            .Additional = FlattenArray(strSplitLine, 3, strSplitLine.Length - 1, ControlChars.Tab)
                        End With

                        intEntryCount += 1
                    End If
                End If

            End While

            ' Close the file
            srInFile.Close()

            If intEntryCount > 0 Then

                ' Sort the data on scan number start, scan number end, then charge
                Dim iQualityListComparerClass As New SpectrumQualityListComparerClass
                Array.Sort(udtSpectrumQualityList, 0, intEntryCount - 1, iQualityListComparerClass)
                iQualityListComparerClass = Nothing

                ' Overwrite the file with the sorted values
                swOutFile = New IO.StreamWriter(mCurrentReportFileName, False)

                swOutFile.WriteLine(strHeaderLine)
                For intIndex = 0 To intEntryCount - 1
                    swOutFile.WriteLine(udtSpectrumQualityList(intIndex).ScanNumberStart.ToString & ControlChars.Tab & _
                                            udtSpectrumQualityList(intIndex).ScanNumberEnd.ToString & ControlChars.Tab & _
                                            udtSpectrumQualityList(intIndex).Charge.ToString & ControlChars.Tab & _
                                            udtSpectrumQualityList(intIndex).Additional)
                Next intIndex
                swOutFile.Close()
            End If

        Catch ex As Exception
            ShowErrorMessage("Error in SortSpectrumQualityTextFile: " & ex.Message)
        End Try


    End Sub

    Public Shared Function SpectrumFilterModeTypeToString(ByVal eSpectrumFilterMode As eSpectrumFilterMode) As String
        Select Case eSpectrumFilterMode
            Case clsMsMsSpectrumFilter.eSpectrumFilterMode.NoFilter
                Return "Fragmentation pattern filter disabled"
            Case clsMsMsSpectrumFilter.eSpectrumFilterMode.mode1
                Return "Mode 1: Amino Acid Spacing Filter"
            Case clsMsMsSpectrumFilter.eSpectrumFilterMode.mode2
                Return "Mode 2: Intensity Threshold Filter"
            Case clsMsMsSpectrumFilter.eSpectrumFilterMode.mode3
                Return "Mode 3: Phosph Neutral Loss Filter"
            Case clsMsMsSpectrumFilter.eSpectrumFilterMode.mode4
                Return "Mode 4: Reporter Ion m/z Filter"
            Case Else
                Return String.Empty
        End Select

    End Function

    Public Shared Function TextMatchTypeCodeToString(ByVal eTextMatchTypeCode As eTextMatchTypeConstants) As String
        Select Case eTextMatchTypeCode
            Case eTextMatchTypeConstants.Contains
                Return TEXT_MATCH_TYPE_CONTAINS
            Case eTextMatchTypeConstants.Exact
                Return TEXT_MATCH_TYPE_EXACT
            Case eTextMatchTypeConstants.RegEx
                Return TEXT_MATCH_TYPE_REGEX
            Case Else
                Return String.Empty
        End Select
    End Function

    Public Shared Function TextMatchTypeStringToCode(ByVal strMatchType As String) As eTextMatchTypeConstants

        Select Case strMatchType.ToLower
            Case TEXT_MATCH_TYPE_CONTAINS.ToLower
                Return eTextMatchTypeConstants.Contains

            Case TEXT_MATCH_TYPE_EXACT.ToLower
                Return eTextMatchTypeConstants.Exact

            Case TEXT_MATCH_TYPE_REGEX.ToLower
                Return eTextMatchTypeConstants.RegEx

            Case Else
                ' Try for a fuzzy match
                If strMatchType.ToLower.IndexOf(TEXT_MATCH_TYPE_CONTAINS.ToLower) >= 0 Then
                    Return eTextMatchTypeConstants.Contains

                ElseIf strMatchType.ToLower.IndexOf(TEXT_MATCH_TYPE_EXACT.ToLower) >= 0 Then
                    Return (eTextMatchTypeConstants.Exact)

                ElseIf strMatchType.ToLower.IndexOf(TEXT_MATCH_TYPE_REGEX.ToLower) >= 0 Then
                    Return eTextMatchTypeConstants.RegEx

                Else
                    ' Default to mode "contains"
                    Return eTextMatchTypeConstants.Contains
                End If

        End Select
    End Function

    Private Sub TraceLog(ByVal strMessage As String)
		Dim strTraceFilePath As String

        If Not TRACE_LOG_ENABLED Then Exit Sub

        Try
            strTraceFilePath = "MSMSSpectrumFilter_Trace_" & System.DateTime.Now.ToString("yyyy-MM-dd") & ".txt"

			Using swOutFile As System.IO.StreamWriter = New System.IO.StreamWriter(New System.IO.FileStream(strTraceFilePath, IO.FileMode.Append, IO.FileAccess.Write, IO.FileShare.Read))
				swOutFile.WriteLine(System.DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") & ControlChars.Tab & strMessage)
			End Using

        Catch ex As Exception
            ShowErrorMessage("Error in TraceLog: " & ex.Message)
		End Try

    End Sub

    Public Sub UpdateAminoAcidMass(ByVal strAminoAcidSymbolOneLetter As String, ByVal sngNewMass As Single)

        Try
            strAminoAcidSymbolOneLetter = strAminoAcidSymbolOneLetter.ToUpper
            If mAminoAcidMassList.ContainsKey(strAminoAcidSymbolOneLetter) Then
                mAminoAcidMassList(strAminoAcidSymbolOneLetter) = sngNewMass
            End If
        Catch ex As Exception
            ShowErrorMessage("Error in UpdateAminoAcidMass: " & ex.Message)
        End Try

    End Sub

    Private Function ValidateOutputFolder(ByRef strInputFilePath As String, ByRef strOutputFolderPath As String) As Boolean
        ' Returns True if strOutputFolderPath points to a valid folder
        ' A valid folder does not equal strInputFilePath's folder
        '
        ' If an error occurs, then sets the Base Class error code to eProcessFilesErrorCodes.InvalidOutputFolderPath

        Dim blnValidOutputFolder As Boolean

        Try
            ' Check if strOutputFolderPath is defined and is not equal to strInputFilePath's folder
            blnValidOutputFolder = False
            If Not strOutputFolderPath Is Nothing AndAlso strOutputFolderPath.Length > 0 Then
                strOutputFolderPath = System.IO.Path.GetFullPath(strOutputFolderPath)
                If System.IO.Directory.Exists(strOutputFolderPath) Then
                    If strOutputFolderPath.ToLower <> System.IO.Path.GetDirectoryName(strInputFilePath).ToLower Then
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
        Static blnDllsValidated As Boolean

        Dim strRequiredDLLs() As String = New String() {"MsMsDataFileReader.dll"}

        Dim intIndex As Integer
        Dim strFilePath As String
        Dim strCurrentFolderPath As String

        If blnDllsValidated Then Return True

        ' Make sure the required DLLs are present

		For intIndex = 0 To strRequiredDLLs.Length - 1			                       
			strCurrentFolderPath = GetAppFolderPath()
			strFilePath = System.IO.Path.Combine(strCurrentFolderPath, strRequiredDLLs(intIndex))
			If Not System.IO.File.Exists(strFilePath) Then
				mErrorMessage = "DLL not found: " & strFilePath
				SetLocalErrorCode(eFilterMsMsSpectraErrorCodes.MissingRequiredDLL)
				ShowErrorMessage(mErrorMessage)
				Return False
			End If
		Next intIndex

        blnDllsValidated = True
        Return True

    End Function

    Protected Sub WriteDTAFile(ByVal strDTAFilePath As String, _
                               ByVal strParentIonLineText As String, _
                               ByVal intDataCount As Integer, _
                               ByRef sngMassList() As Single, _
                               ByRef sngIntensityList() As Single)

        Dim swOutfile As System.IO.StreamWriter


        Try
            swOutfile = New System.IO.StreamWriter(New System.IO.FileStream(strDTAFilePath, IO.FileMode.Create, IO.FileAccess.Write, IO.FileShare.Read))

            swOutfile.Write(WriteDTAFileToString(strParentIonLineText, intDataCount, sngMassList, sngIntensityList))

            swOutfile.Close()

        Catch ex As Exception

            mErrorMessage = "Error in writing DTA file " & strDTAFilePath & ": " & ex.Message
            HandleException(mErrorMessage, ex)
        End Try

    End Sub

    Protected Function WriteDTAFileToString(ByVal strParentIonLineText As String, _
                                            ByVal intDataCount As Integer, _
                                            ByRef sngMassList() As Single, _
                                            ByRef sngIntensityList() As Single) As String

        Dim sbStringBuilder As New System.Text.StringBuilder
        Dim intIndex As Integer

        ' Define the parent ion line
        sbStringBuilder.AppendLine(strParentIonLineText)

        ' Append in m/z and intensity data
        For intIndex = 0 To intDataCount - 1
            If sngIntensityList(intIndex) > 1 Then
                sbStringBuilder.AppendLine(sngMassList(intIndex).ToString & " " & sngIntensityList(intIndex).ToString("0.00"))
            Else
                sbStringBuilder.AppendLine(sngMassList(intIndex).ToString & " " & sngIntensityList(intIndex).ToString)
            End If
        Next

        Return sbStringBuilder.ToString

    End Function

    Protected Function WriteMGFEntryToString(ByRef udtSpectrumHeaderInfo As MsMsDataFileReader.clsMsMsDataFileReaderBaseClass.udtSpectrumHeaderInfoType, _
                                            ByVal intDataCount As Integer, _
                                            ByRef sngMassList() As Single, _
                                            ByRef sngIntensityList() As Single) As String

        Dim sbStringBuilder As New System.Text.StringBuilder
        Dim strLineOut As String

        Dim intIndex As Integer

        sbStringBuilder.AppendLine("BEGIN IONS")

        If Not udtSpectrumHeaderInfo.SpectrumTitle Is Nothing AndAlso udtSpectrumHeaderInfo.SpectrumTitle.Length > 0 Then
            sbStringBuilder.AppendLine(udtSpectrumHeaderInfo.SpectrumTitle)
        Else
            sbStringBuilder.AppendLine("TITLE=Spectrum")
        End If

        If Not udtSpectrumHeaderInfo.ParentIonLineText Is Nothing AndAlso udtSpectrumHeaderInfo.ParentIonLineText.Length > 0 Then
            sbStringBuilder.AppendLine(udtSpectrumHeaderInfo.ParentIonLineText)
        Else
            sbStringBuilder.AppendLine("PEPMASS=" & udtSpectrumHeaderInfo.ParentIonMZ)
        End If

        If udtSpectrumHeaderInfo.ParentIonChargeCount >= 1 Then
            strLineOut = "CHARGE=" & udtSpectrumHeaderInfo.ParentIonCharges(0) & "+"

            If udtSpectrumHeaderInfo.ParentIonChargeCount > 1 Then
                For intIndex = 1 To udtSpectrumHeaderInfo.ParentIonChargeCount - 1
                    strLineOut &= " and " & udtSpectrumHeaderInfo.ParentIonCharges(intIndex) & "+"
                Next
            End If

            sbStringBuilder.AppendLine(strLineOut)
        Else
            ' Unknown charge; assume 2+ and 3+
            sbStringBuilder.AppendLine("CHARGE=2+ and 3+")
        End If

        ' Append in m/z and intensity data
        For intIndex = 0 To intDataCount - 1
            If sngIntensityList(intIndex) > 1 Then
                sbStringBuilder.AppendLine(sngMassList(intIndex).ToString & " " & sngIntensityList(intIndex).ToString("0.0"))
            Else
                sbStringBuilder.AppendLine(sngMassList(intIndex).ToString & " " & sngIntensityList(intIndex).ToString)
            End If
        Next

        sbStringBuilder.AppendLine("END IONS")

        Return sbStringBuilder.ToString

    End Function

    Private Sub WriteSettingsToLog(ByVal strInputFilePath As String, _
                                   ByVal strOutputFilePath As String, _
                                   ByVal intMSLevelFilter As Integer, _
                                   ByVal strScanTypeFilter As String, _
                                   ByVal strCollisionModeFilter As String, _
                                   ByVal blnEvaluateSpectrumQualityOnly As Boolean)

        Dim blnIonFilteringEnabled As Boolean

        blnIonFilteringEnabled = CheckIonFilteringEnabled()

        LogMessage("Reading " & strInputFilePath)
        If Not strOutputFilePath Is Nothing AndAlso strOutputFilePath.Length > 0 Then
            LogMessage("Writing " & strOutputFilePath)
        End If

        LogMessage("Current settings")
        LogMessage(" EvaluateSpectrumQualityOnly: " & blnEvaluateSpectrumQualityOnly)
        LogMessage(" MSLevelFilter: " & intMSLevelFilter)

        If strScanTypeFilter.Length = 0 Then
            LogMessage(" ScanTypeFilter: ''")
        Else
            LogMessage(" ScanTypeFilter: '" & strScanTypeFilter & "' (match type " & TextMatchTypeCodeToString(mScanTypeMatchType) & ")")
        End If

        If strCollisionModeFilter.Length = 0 Then
            LogMessage(" MSCollisionModeFilter: ''")
        Else
            LogMessage(" MSCollisionModeFilter: '" & strCollisionModeFilter & "' (match type " & TextMatchTypeCodeToString(mMSCollisionModeMatchType) & ")")
        End If

        LogMessage(" Filter mode: " & SpectrumFilterModeTypeToString(mSpectrumFilterMode))
        LogMessage(" Ion filtering enabled: " & blnIonFilteringEnabled)

        If blnIonFilteringEnabled Then
            LogMessage("  RemovePrecursor: " & mIonFilterOptions.RemovePrecursor)
            LogMessage("  RemoveChargeReducedPrecursors: " & mIonFilterOptions.RemoveChargeReducedPrecursors)
            LogMessage("  RemoveNeutralLossesFromChargeReducedPrecursors: " & mIonFilterOptions.RemoveNeutralLossesFromChargeReducedPrecursors)

            ' Write to the log the m/z ranges that would be filtered out for four theoretical mass spectra
            LogMatchingIonFilterRange(1000, 2)
            LogMatchingIonFilterRange(1000, 3)
            LogMatchingIonFilterRange(1000, 4)

            LogMessage("  CondenseData: " & mIonFilterOptions.CondenseData)
            LogMessage("  CondenseDataMaxIonCount: " & mIonFilterOptions.CondenseDataMaxIonCount)

        End If

		If mMinimumIonCount > 0 Then
			LogMessage(" MinimumIonCount: " & mMinimumIonCount)
		End If

    End Sub

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

    Private Class SearchMassSpecsClass

        ' Be sure to update SEARCH_MASS_CODE_COUNT when changing enum SearchMassSpecsClass.eSearchMassCodeConstants
        Public Const SEARCH_MASS_CODE_COUNT As Integer = 10
        Public Enum eSearchMassCodeConstants
            CustomMass = 0
            NL98 = 1            ' loss of 98 from the parent m/z
            NL116 = 2           ' loss of (98+18) = 116
            NL49 = 3            ' loss of 98/2 = 49
            NL58 = 4            ' loss of (98+18)/2 = 58 
            NL107 = 5           ' loss of (98*2+18)/2 = 107
            NL33 = 6            ' loss of 98/3 = 32.66
            NL65 = 7            ' loss of (98*2)/3 = 65.32
            NL39 = 8            ' loss of (98+18)/3 = 38.66
            NL71 = 9            ' loss of (98*2+18)/3 = 71.32
        End Enum

        Public SearchMass As Double
        Public LowerBoundMZ As Double
        Public UpperBoundMZ As Double
        Public SearchMassCode As eSearchMassCodeConstants
        Public Matched As Boolean
        Public IonIntensity As Double
    End Class

End Class
