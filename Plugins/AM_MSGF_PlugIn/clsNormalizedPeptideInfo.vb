
''' <summary>
''' Tracks the mods for a given normalized peptide
''' </summary>    
Public Class clsNormalizedPeptideInfo

    ''' <summary>
    ''' Peptide clean sequence (no mod symbols)
    ''' </summary>
    ''' <remarks>This field is empty in dictNormalizedPeptides because the keys in the dictionary are the clean sequence</remarks>
    Public ReadOnly Property CleanSequence As String

    ''' <summary>
    ''' List of modified amino acids
    ''' </summary>
    ''' <remarks>Keys are mod names or symbols; values are the 0-based residue index</remarks>
    Public Property ModifiedResidues As List(Of KeyValuePair(Of String, Integer))

    ''' <summary>
    ''' Sequence ID for this normalized peptide
    ''' </summary>
    ''' <returns></returns>
    Public Property SeqID As Integer

    ''' <summary>
    ''' Constructor
    ''' </summary>
    ''' <param name="peptideCleanSequence"></param>
    Public Sub New(peptideCleanSequence As String)
        CleanSequence = peptideCleanSequence
        ModifiedResidues = New List(Of KeyValuePair(Of String, Integer))
        SeqID = -1
    End Sub

    Public Overrides Function ToString() As String
        If ModifiedResidues Is Nothing Then
            Return String.Format("{0}: {1}, ModCount={2}", SeqID, CleanSequence, 0)
        Else
            Return String.Format("{0}: {1}, ModCount={2}", SeqID, CleanSequence, ModifiedResidues.Count)
        End If
    End Function

End Class
