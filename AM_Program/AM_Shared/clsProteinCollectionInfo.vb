Public Class clsProteinCollectionInfo

    Private mOrgDBDescription As String

    ''' <summary>
    ''' Legacy Fasta file name
    ''' </summary>
    ''' <remarks>Will be "na" when using a protein collection</remarks>
    Public Property LegacyFastaName As String

    Public Property ProteinCollectionOptions As String
    Public Property ProteinCollectionList As String

    Public Property UsingLegacyFasta As Boolean
    Public Property UsingSplitFasta As Boolean
    Public Property ErrorMessage As String
    Public Property IsValid As Boolean

    Public ReadOnly Property OrgDBDescription As String
        Get
            Return mOrgDBDescription
        End Get
    End Property

    Public Sub New(jobParams As IJobParams)

        LegacyFastaName = jobParams.GetParam("LegacyFastaFileName")
        ProteinCollectionOptions = jobParams.GetParam("ProteinOptions")
        ProteinCollectionList = jobParams.GetParam("ProteinCollectionList")
        UsingSplitFasta = jobParams.GetJobParameter("SplitFasta", False)

        ' Update mOrgDBDescription and UsingLegacyFasta
        UpdateDescription()

    End Sub

    Public Sub UpdateDescription()
        If Not String.IsNullOrWhiteSpace(ProteinCollectionList) AndAlso Not ProteinCollectionList.ToLower() = "na" Then
            mOrgDBDescription = "Protein collection: " + ProteinCollectionList + " with options " + ProteinCollectionOptions
            UsingLegacyFasta = False
            IsValid = True
        ElseIf Not String.IsNullOrWhiteSpace(LegacyFastaName) AndAlso Not LegacyFastaName.ToLower() = "na" Then
            mOrgDBDescription = "Legacy DB: " + LegacyFastaName
            UsingLegacyFasta = True
            IsValid = True
        Else
            ErrorMessage = "Both the ProteinCollectionList and LegacyFastaFileName parameters are empty or 'na'"
            IsValid = False
        End If
    End Sub


End Class
