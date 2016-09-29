
''' <summary>
''' Tracks the mods for a given normalized peptide
''' </summary>    
Public Class clsNormalizedPeptideInfo

    ''' <summary>
    ''' Peptide clean sequence (no mod symbols)
    ''' </summary>
    ''' <remarks>This field is empty in dictNormalizedPeptides because the keys in the dictionary are the clean sequence</remarks>
    Public ReadOnly Property CleanSequence As String

    ' ReSharper disable once CollectionNeverUpdated.Global
    ''' <summary>
    ''' List of modified amino acids
    ''' </summary>
    ''' <remarks>Keys are mod names or symbols; values are the 0-based residue index</remarks>
    Public ReadOnly Property Modifications As List(Of KeyValuePair(Of String, Integer))

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
        Modifications = New List(Of KeyValuePair(Of String, Integer))
        SeqID = -1
    End Sub

    Public Sub StoreModifications(newModifications As IEnumerable(Of KeyValuePair(Of String, Integer)))
        Modifications.Clear()
        Modifications.AddRange(newModifications)
    End Sub

    Public Overrides Function ToString() As String
        If Modifications Is Nothing Then
            Return String.Format("{0}: {1}, ModCount={2}", SeqID, CleanSequence, 0)
        Else
            Return String.Format("{0}: {1}, ModCount={2}", SeqID, CleanSequence, Modifications.Count)
        End If
    End Function

End Class
