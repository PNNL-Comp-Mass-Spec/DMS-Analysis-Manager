Public Class clsSampleMetadata

    Public Structure udtCvParamInfoType
        Public Accession As String
        Public CvRef As String
        Public Value As String
        Public Name As String
        Public unitCvRef As String
        Public unitName As String
        Public unitAccession As String
        Public Sub Clear()
            Accession = String.Empty
            CvRef = String.Empty
            Value = String.Empty
            Name = String.Empty
            unitCvRef = String.Empty
            unitName = String.Empty
            unitAccession = String.Empty
        End Sub
    End Structure

    Public Property Species As String                                            ' Recommended to use NEWT CVs
    Public Property Tissue As String                                             ' Recommended to use BRENDA CVs (BTO)
    Public Property CellType As String                                           ' Recommended to use CL CVs
    Public Property Disease As String                                            ' Recommended to use DOID CVs
    Public Property Modifications As Dictionary(Of String, udtCvParamInfoType)   ' Recommended to use PSI-MOD, though Unimod is acceptable
    Public Property InstrumentGroup As String                                    ' Recommended to use MS CVs
    Public Property Quantification As String
    Public Property ExperimentalFactor As String

    ''' <summary>
    ''' Constructor
    ''' </summary>
    ''' <remarks></remarks>
    Public Sub New()
        Clear()
    End Sub

    Public Sub Clear()
        Species = String.Empty
        Tissue = String.Empty
        CellType = String.Empty
        Disease = String.Empty
        Modifications = New Dictionary(Of String, udtCvParamInfoType)(StringComparer.CurrentCultureIgnoreCase)
        InstrumentGroup = String.Empty
        Quantification = String.Empty
        ExperimentalFactor = String.Empty
    End Sub

End Class
