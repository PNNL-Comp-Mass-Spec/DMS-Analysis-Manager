Public Class AMFileNotFoundException
	Inherits ApplicationException

	Private m_FileName As String

	Public Sub New(ByVal FileName As String, ByVal Message As String)

		MyBase.New(Message)
		m_FileName = FileName

	End Sub

	Public ReadOnly Property FileName() As String
		Get
			Return m_FileName
		End Get
	End Property

End Class

Public Class AMFolderNotFoundException
	Inherits ApplicationException

	Private m_FolderName As String

	Public Sub New(ByVal FolderName As String, ByVal Message As String)

		MyBase.New(Message)
		m_FolderName = FolderName

	End Sub

	Public ReadOnly Property FolderName() As String
		Get
			Return m_FolderName
		End Get
	End Property

End Class

Public Class AMFileNotDeletedAfterRetryException
	Inherits ApplicationException

	Public Enum RetryExceptionType
		IO_Exception
		Unauthorized_Access_Exception
	End Enum

	Private m_FileName As String
	Private m_ExceptionType As RetryExceptionType

	Public Sub New(ByVal FileName As String, ByVal ExceptionType As RetryExceptionType, ByVal Message As String)

		MyBase.New(Message)
		m_FileName = FileName
		m_ExceptionType = ExceptionType

	End Sub

	Public ReadOnly Property FileName() As String
		Get
			Return m_FileName
		End Get
	End Property

	Public ReadOnly Property ExcType() As RetryExceptionType
		Get
			Return m_ExceptionType
		End Get
	End Property

End Class

Public Class AMFileNotDeletedException
	Inherits ApplicationException

	Private m_FileName As String

	Public Sub New(ByVal FileName As String, ByVal Message As String)

		MyBase.New(Message)
		m_FileName = FileName

	End Sub

	Public ReadOnly Property FileName() As String
		Get
			Return m_FileName
		End Get
	End Property

End Class