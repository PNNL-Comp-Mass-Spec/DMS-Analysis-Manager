Option Strict On

Imports System.Linq

Public Class clsPSMInfo

    Public Const UNKNOWN_MSGF_SPECPROB As Double = 10
    Public Const UNKNOWN_EVALUE As Double = Double.MaxValue
    Public Const UNKNOWN_FDR As Integer = -1
    Public Const UNKNOWN_SEQID As Integer = -1

    Private ReadOnly mObservations As List(Of PSMObservation)

    ''' <summary>
    ''' True if the C-terminus of the peptide is Lysine
    ''' </summary>
    Public Property CTermK As Boolean

    ''' <summary>
    ''' True if the C-terminus of the peptide is Arginine
    ''' </summary>
    Public Property CTermR As Boolean

    ''' <summary>
    ''' True if the peptide is from a keratin protein
    ''' </summary>
    ''' <returns></returns>
    Public Property KeratinPeptide As Boolean

    ''' <summary>
    ''' True if the peptide has an internal K or R that is not followed by P
    ''' </summary>
    ''' <returns></returns>
    Public Property MissedCleavage As Boolean

    ''' <summary>
    ''' True if this is a phosphopeptide
    ''' </summary>
    Public Property Phosphopeptide As Boolean

    ''' <summary>
    ''' Protein name (from the _fht.txt or _syn.txt file)
    ''' </summary>
    Public Property Protein As String

    ''' <summary>
    ''' First sequence ID for this normalized peptide
    ''' </summary>
    Public Property SeqIdFirst As Integer

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
    ''' Details for each PSM that maps to this class
    ''' </summary>
    Public ReadOnly Property Observations As List(Of PSMObservation)
        Get
            Return mObservations
        End Get
    End Property

    Public ReadOnly Property BestMSGF As Double
        Get
            If Observations.Count = 0 Then
                Return UNKNOWN_MSGF_SPECPROB
            Else
                Return (From item In Observations Order By item.MSGF Select item.MSGF).First
            End If
        End Get
    End Property

    Public ReadOnly Property BestEValue As Double
        Get
            If Observations.Count = 0 Then
                Return UNKNOWN_EVALUE
            Else
                Return (From item In Observations Order By item.EValue Select item.EValue).First
            End If
        End Get
    End Property

    Public ReadOnly Property BestFDR As Double
        Get
            If Observations.Count = 0 Then
                Return UNKNOWN_FDR
            Else
                Return (From item In Observations Order By item.FDR Select item.FDR).First
            End If
        End Get
    End Property

    ''' <summary>
    ''' Constructor
    ''' </summary>
    Public Sub New()
        mObservations = New List(Of PSMObservation)
        Clear()
    End Sub

    ''' <summary>
    ''' Reset the fields
    ''' </summary>
    Public Sub Clear()
        Protein = String.Empty
        SeqIdFirst = UNKNOWN_SEQID
        mObservations.Clear()
        CTermK = False
        CTermR = False
        MissedCleavage = False
        KeratinPeptide = False
        TrypsinPeptide = False
        Phosphopeptide = False
        Tryptic = False
    End Sub

    ''' <summary>
    ''' Add a PSM Observation
    ''' </summary>
    ''' <param name="observation"></param>
    Public Sub AddObservation(observation As PSMObservation)
        mObservations.Add(observation)
    End Sub

    Public Overrides Function ToString() As String
        If Observations.Count = 0 Then
            Return String.Format("SeqID {0}, {1} (0 observations)",
                                 SeqIdFirst, Protein)
        End If

        If Observations.Count = 1 Then
            Return String.Format("SeqID {0}, {1}, Scan {2} (1 observation)",
                                 SeqIdFirst, Protein, Observations(0).Scan)
        Else
            Return String.Format("SeqID {0}, {1}, Scans {2}-{3} ({4} observations)",
                                 SeqIdFirst, Protein, Observations(0).Scan,
                                 Observations(Observations.Count - 1).Scan, Observations.Count)
        End If

    End Function

    Public Class PSMObservation
        Public Property Scan As Integer

        ''' <summary>
        ''' FDR (aka QValue)
        ''' </summary>
        Public Property FDR As Double

        ''' <summary>
        ''' MSGF SpecProb; will be UNKNOWN_MSGF_SPECPROB (10) if MSGF SpecProb is not available
        ''' </summary>
        ''' <remarks>MSPathFinder results use this field to store SpecEValue</remarks>
        Public Property MSGF As Double

        ''' <summary>
        ''' Only used when MSGF SpecProb is not available
        ''' </summary>
        Public Property EValue As Double

        Public Property PassesFilter As Boolean

        Public Sub New()
            Clear()
        End Sub

        Public Sub Clear()
            Scan = 0
            FDR = UNKNOWN_FDR
            MSGF = UNKNOWN_MSGF_SPECPROB
            EValue = UNKNOWN_EVALUE
            PassesFilter = False
        End Sub

        Public Overrides Function ToString() As String
            Return String.Format("Scan {0}, FDR {1:F4}, MSGF {2:E3}", Scan, FDR, MSGF)
        End Function
    End Class
End Class
