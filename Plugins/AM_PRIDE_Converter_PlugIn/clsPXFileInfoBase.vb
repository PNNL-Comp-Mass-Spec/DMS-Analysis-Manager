Imports AnalysisManagerBase.clsAnalysisResources

Public Class clsPXFileInfoBase

#Region "Module Variables"
    Protected mFileName As String
    Protected mJobInfo As udtDataPackageJobInfoType
#End Region

#Region "Structures and Enums"
    Public Enum ePXFileType
        Undefined = 0
        Result = 1              ' .msgf-pride.xml files
        ResultMzId = 2          ' .mzid files from MSGF+  (listed as "result" files in the .px file)
        Raw = 3                 ' Instrument data files (typically .raw files)
        Search = 4              ' Search engine output files, such as Mascot DAT or other output files (from analysis pipelines, such as pep.xml or prot.xml).
        Peak = 5                ' _dta.txt or .mgf files
    End Enum
#End Region

#Region "Auto-properties"
    Public Property FileID As Integer
    Public Property Length As Int64
    Public Property MD5Hash As String
#End Region

#Region "Properties"
    Public ReadOnly Property Filename() As String
        Get
            Return mFileName
        End Get
    End Property

    Public ReadOnly Property JobInfo() As udtDataPackageJobInfoType
        Get
            Return mJobInfo
        End Get
    End Property
#End Region

    Public Sub New(FileName As String, udtJobInfo As udtDataPackageJobInfoType)
        mFileName = FileName
        mJobInfo = udtJobInfo
    End Sub

    Public Sub Update(oSource As clsPXFileInfoBase)
        Me.mFileName = oSource.mFileName
        Me.mJobInfo = oSource.mJobInfo
        Me.FileID = oSource.FileID
        Me.Length = oSource.Length
        Me.MD5Hash = oSource.MD5Hash
    End Sub
End Class
