Imports AnalysisManagerBase.clsAnalysisResources

Public Class clsPXFileInfo
    Inherits clsPXFileInfoBase

#Region "Module Variables"
    Protected mFileMappings As List(Of Integer)
#End Region

#Region "Auto-properties"
    Public Property PXFileType As ePXFileType
#End Region

#Region "Properties"
    Public ReadOnly Property FileMappings() As List(Of Integer)
        Get
            Return mFileMappings
        End Get
    End Property
#End Region

    Public Sub New(fileName As String, udtJobInfo As udtDataPackageJobInfoType)
        MyBase.New(fileName, udtJobInfo)
        mFileMappings = New List(Of Integer)
    End Sub

    Public Sub AddFileMapping(intPXFileID As Integer)
        If Not mFileMappings.Contains(intPXFileID) Then
            mFileMappings.Add(intPXFileID)
        End If
    End Sub

End Class
