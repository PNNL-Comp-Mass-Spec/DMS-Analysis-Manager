Public Class clsPXFileInfoBase

#Region "Module Variables"
	Protected mFileName As String
#End Region

#Region "Structures and Enums"
	Public Enum ePXFileType
		Undefined = 0
		Result = 1				' .msgf-pride.xml files
		Raw = 2					' Instrument data files (typically .raw files)
		Search = 3				' .mzid files from MSGF+
		Peak = 4				' _dta.txt or .mgf files
	End Enum
#End Region

#Region "Auto-properties"
	Public Property FileID As Integer
	Public Property Length As Int64
	Public Property MD5Hash As String
	Public Property Job As Integer
#End Region
	
#Region "Properties"
	Public ReadOnly Property Filename() As String
		Get
			Return mFileName
		End Get
	End Property
#End Region
	
	Public Sub New(ByVal FileName As String)
		mFileName = FileName
	End Sub

	Public Sub Update(ByVal oSource As clsPXFileInfoBase)
		Me.mFileName = oSource.mFileName
		Me.FileID = oSource.FileID
		Me.Length = oSource.Length
		Me.MD5Hash = oSource.MD5Hash
		Me.Job = oSource.Job
	End Sub
End Class
