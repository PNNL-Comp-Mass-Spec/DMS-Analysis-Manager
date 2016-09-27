Option Strict On

Imports System.Linq

Public Class clsPSMInfo

    Public Const UNKNOWN_MSGF_SPECPROB As Double = 10
    Public Const UNKNOWN_EVALUE As Double = Double.MaxValue
    Public Const UNKNOWN_FDR As Integer = -1
    Public Const UNKNOWN_SEQID As Integer = -1

    Private ReadOnly mObservations As List(Of PSMObservation)

    ''' <summary>
    ''' Protein name (from the _fht.txt or _syn.txt file)
    ''' </summary>
    Public Property Protein As String

    ''' <summary>
    ''' First sequence ID for this normalized peptide
    ''' </summary>
    Public Property SeqIdFirst As Integer

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
            Return "SeqID " & SeqIdFirst & ", " & Protein
        End If

        If Observations.Count = 1 Then
            Return "SeqID " & SeqIdFirst & ", " & Protein & ", Scan " & Observations(0).Scan
        End If

        Return "SeqID " & SeqIdFirst & ", " & Protein & ", Scans " & Observations(0).Scan.ToString() & "-" &
               Observations(Observations.Count - 1).Scan.ToString()
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
    End Class
End Class
