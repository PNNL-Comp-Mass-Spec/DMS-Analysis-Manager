Imports PHRPReader

Public Class clsDataPackageJobInfo

    Private ReadOnly mJob As Integer

    Private ReadOnly mDataset As String

    ' ReSharper disable once ConvertToVbAutoProperty
    Public ReadOnly Property Job As Integer
        Get
            Return mJob
        End Get
    End Property

    ' ReSharper disable once ConvertToVbAutoProperty
    Public ReadOnly Property Dataset As String
        Get
            Return mDataset
        End Get
    End Property

    Public Property DatasetID As Integer

    Public Property Instrument As String

    Public Property InstrumentGroup As String

    Public Property Experiment As String

    Public Property Experiment_Reason As String

    Public Property Experiment_Comment As String

    Public Property Experiment_Organism As String

    ''' <summary>
    ''' NEWT ID for Experiment_Organism; see http://dms2.pnl.gov/ontology/report/NEWT/
    ''' </summary>
    ''' <value></value>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public Property Experiment_NEWT_ID As Integer

    ''' <summary>
    ''' NEWT Name for Experiment_Organism; see http://dms2.pnl.gov/ontology/report/NEWT/
    ''' </summary>
    ''' <value></value>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public Property Experiment_NEWT_Name As String

    Public Property Tool As String

    ''' <summary>
    ''' Number of steps in a split fasta job
    ''' </summary>
    ''' <value></value>
    ''' <returns></returns>
    ''' <remarks>0 if not a split fasta job</remarks>
    Public Property NumberOfClonedSteps As Integer

    Public Property ResultType As String

    Public Property PeptideHitResultType As clsPHRPReader.ePeptideHitResultType

    Public Property SettingsFileName As String

    Public Property ParameterFileName As String

    ''' <summary>
    ''' Generated Fasta File Name or legacy fasta file name
    ''' </summary>
    ''' <value></value>
    ''' <returns></returns>
    ''' <remarks>
    ''' For jobs where ProteinCollectionList = 'na', this is the legacy fasta file name
    ''' Otherwise, this is the generated fasta file name (or "na")
    ''' </remarks>
    Public Property OrganismDBName As String

    Public Property LegacyFastaFileName As String

    Public Property ProteinCollectionList As String

    Public Property ProteinOptions As String

    Public Property ServerStoragePath As String

    Public Property ArchiveStoragePath As String

    Public Property ResultsFolderName As String

    Public Property DatasetFolderName As String

    Public Property SharedResultsFolder As String

    Public Property RawDataType As String

    ''' <summary>
    ''' Constructor
    ''' </summary>
    ''' <remarks></remarks>
    Public Sub New(job As Integer, datasetName As String)
        mJob = job
        mDataset = datasetName

        DatasetID = 0
        Instrument = ""
        InstrumentGroup = ""
        Experiment = ""
        Experiment_Reason = ""
        Experiment_Comment = ""
        Experiment_Organism = ""
        Experiment_NEWT_ID = 0
        Experiment_NEWT_Name = ""
        Tool = ""
        NumberOfClonedSteps = 0
        ResultType = ""
        PeptideHitResultType = clsPHRPReader.ePeptideHitResultType.Unknown
        SettingsFileName = ""
        ParameterFileName = ""
        OrganismDBName = ""
        LegacyFastaFileName = ""
        ProteinCollectionList = ""
        ProteinOptions = ""
        ServerStoragePath = ""
        ArchiveStoragePath = ""
        ResultsFolderName = ""
        DatasetFolderName = ""
        SharedResultsFolder = ""
        RawDataType = ""
    End Sub

End Class
