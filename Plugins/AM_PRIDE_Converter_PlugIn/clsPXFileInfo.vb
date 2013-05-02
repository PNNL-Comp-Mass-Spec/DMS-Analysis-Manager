
Public Class clsPXFileInfo
	Inherits clsPXFileInfoBase

#Region "Module Variables"
	Protected mFileMappings As Generic.List(Of Integer)
#End Region

#Region "Auto-properties"
	Public Property PXFileType As ePXFileType
#End Region


#Region "Properties"
	Public ReadOnly Property FileMappings() As Generic.List(Of Integer)
		Get
			Return mFileMappings
		End Get
	End Property
#End Region

	Public Sub New(ByVal FileName As String)
		MyBase.New(FileName)
		mFileMappings = New Generic.List(Of Integer)
	End Sub

	Public Sub AddFileMapping(ByVal intPXFileID As Integer)
		If Not mFileMappings.Contains(intPXFileID) Then
			mFileMappings.Add(intPXFileID)
		End If
	End Sub

End Class
