Public Class clsResultFileContainer

    ''' <summary>
    ''' Tracks the .mgf or _dta.txt or .mzML file for the analysis job
    ''' </summary>
    ''' <value></value>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public Property MGFFilePath As String

    ''' <summary>
    ''' One or more .mzid.gz files
    ''' </summary>
    ''' <value></value>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public Property MzIDFilePaths As List(Of String)

    ''' <summary>
    ''' Tracks the .pepXML.gz file for the analysis job
    ''' </summary>
    ''' <value></value>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public Property PepXMLFile As String

    Public Property PrideXmlFilePath As String

    ''' <summary>
    ''' Constructor
    ''' </summary>
    ''' <remarks></remarks>
    Public Sub New()
        MzIDFilePaths = New List(Of String)
    End Sub
End Class
