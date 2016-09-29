Option Strict On

Public Class clsUniqueSeqInfo
    Private mObsCount As Integer

    ''' <summary>
    ''' Observation count
    ''' </summary>
    ''' <returns></returns>
    ''' <remarks>Overridden in clsPSMInfo because it tracks specific PSMObservations</remarks>
    Public Overridable ReadOnly Property ObsCount As Integer
        Get
            Return mObsCount
        End Get
    End Property

    ''' <summary>
    ''' True if the C-terminus of the peptide is Lysine
    ''' </summary>
    Public Property CTermK As Boolean

    ''' <summary>
    ''' True if the C-terminus of the peptide is Arginine
    ''' </summary>
    Public Property CTermR As Boolean

    ''' <summary>
    ''' True if the peptide has an internal K or R that is not followed by P
    ''' </summary>
    ''' <returns></returns>
    Public Property MissedCleavage As Boolean

    ''' <summary>
    ''' True if the peptide is from a keratin protein
    ''' </summary>
    ''' <returns></returns>
    Public Property KeratinPeptide As Boolean

    ''' <summary>
    ''' True if the peptide is from a trypsin protein
    ''' </summary>
    ''' <returns></returns>
    Public Property TrypsinPeptide As Boolean

    ''' <summary>
    ''' True if the peptide is partially or fully tryptic
    ''' </summary>
    ''' <returns></returns>
    Public Property Tryptic As Boolean

    ''' <summary>
    ''' Constructor
    ''' </summary>
    Protected Sub New()
        Clear()
    End Sub

    Public Overridable Sub Clear()
        mObsCount = 0
        CTermK = False
        CTermR = False
        MissedCleavage = False
        KeratinPeptide = False
        TrypsinPeptide = False
        Tryptic = False
    End Sub

    Public Overridable Sub UpdateObservationCount(observationCount As Integer)
        mObsCount = observationCount
    End Sub
End Class
