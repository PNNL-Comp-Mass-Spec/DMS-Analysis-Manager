Option Strict On

Public Class clsPSMInfo

    Public Const UNKNOWN_MSGF_SPECPROB As Double = 10
    Public Const UNKNOWN_EVALUE As Double = Double.MaxValue
    Public Const UNKNOWN_FDR As Integer = -1
    Public Const UNKNOWN_SEQID As Integer = -1

    ''' <summary>
    ''' Protein name (from the _fht.txt or _syn.txt file)
    ''' </summary>
    Public Property Protein As String

    ''' <summary>
    ''' FDR (aka QValue)
    ''' </summary>
    Public Property FDR As Double

    ''' <summary>
    ''' MSGF SpecProb; will be UNKNOWN_MSGF_SPECPROB (10) if MSGF SpecProb is not available
    ''' </summary>
    Public Property MSGF As Double

    ''' <summary>
    ''' Only used when MSGF SpecProb is not available
    ''' </summary>
    Public Property EValue As Double

    ''' <summary>
    ''' First sequence ID for this normalized peptide
    ''' </summary>
    Public Property SeqIdFirst As Integer

    ''' <summary>
    ''' Scan numbers that the peptide was observed in
    ''' </summary>
    Public Property Scans As List(Of Integer)

    Public Sub New()
        Clear()
    End Sub

    Public Sub Clear()
        Protein = String.Empty
        FDR = UNKNOWN_FDR
        MSGF = UNKNOWN_MSGF_SPECPROB
        EValue = UNKNOWN_EVALUE
        SeqIdFirst = UNKNOWN_SEQID
        Scans = New List(Of Integer)
    End Sub

    Public Overrides Function ToString() As String
        If Scans.Count = 0 Then
            Return "clsPSMInfo: empty"
        End If
        Return "Scan " & Scans(0).ToString & ": " & Protein
    End Function
End Class
