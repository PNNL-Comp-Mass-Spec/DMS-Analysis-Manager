Public Class clsDataPackageDatasetInfo

    Private ReadOnly mDataset As String
    Private ReadOnly mDatasetID As Integer

    ' ReSharper disable once ConvertToVbAutoProperty
    Public ReadOnly Property Dataset As String
        Get
            Return mDataset
        End Get
    End Property

    ' ReSharper disable once ConvertToVbAutoProperty
    Public ReadOnly Property DatasetID As Integer
        Get
            Return mDatasetID
        End Get
    End Property

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

    Public Property ServerStoragePath As String

    Public Property ArchiveStoragePath As String

    Public Property RawDataType As String

    ''' <summary>
    ''' Constructor
    ''' </summary>
    ''' <remarks></remarks>
    Public Sub New(datasetName As String, datasetId As Integer)
        mDataset = datasetName
        mDatasetID = datasetId

        Instrument = ""
        InstrumentGroup = ""
        Experiment = ""
        Experiment_Reason = ""
        Experiment_Comment = ""
        Experiment_Organism = ""
        Experiment_NEWT_ID = 0
        Experiment_NEWT_Name = ""
        ServerStoragePath = ""
        ArchiveStoragePath = ""
        RawDataType = ""

    End Sub

End Class
