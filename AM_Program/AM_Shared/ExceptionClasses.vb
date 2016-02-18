'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2006, Battelle Memorial Institute
' Created 06/07/2006
'
'*********************************************************************************************************

Option Strict On

Public Class AMFileNotFoundException
	Inherits ApplicationException

	'*********************************************************************************************************
	'Specialized handler for "file not found" exception
	'*********************************************************************************************************

#Region "Module variables"
	Private m_FileName As String
#End Region

#Region "Properties"
	Public ReadOnly Property FileName() As String
		Get
			Return m_FileName
		End Get
	End Property
#End Region

#Region "Methods"
	''' <summary>
	''' Constructor
	''' </summary>
	''' <param name="FileName">Name of file being processed when exception occurred</param>
	''' <param name="Message">Message to be returned in exception</param>
	''' <remarks></remarks>
	Public Sub New(ByVal FileName As String, ByVal Message As String)

		MyBase.New(Message)
		m_FileName = FileName

	End Sub
#End Region

End Class


Public Class AMFolderNotFoundException
	Inherits ApplicationException

	'*********************************************************************************************************
	'Specialized handler for "folder not found" exception
	'*********************************************************************************************************

#Region "Module variables"
	Private m_FolderName As String
#End Region

#Region "Properties"
	Public ReadOnly Property FolderName() As String
		Get
			Return m_FolderName
		End Get
	End Property
#End Region

#Region "Methods"
	''' <summary>
	''' Constructor
	''' </summary>
	''' <param name="FolderName">Name of unfound folder</param>
	''' <param name="Message">Message for exception to return</param>
	''' <remarks></remarks>
	Public Sub New(ByVal FolderName As String, ByVal Message As String)

		MyBase.New(Message)
		m_FolderName = FolderName

	End Sub
#End Region

End Class

Public Class AMFileNotDeletedAfterRetryException
	Inherits ApplicationException

	'*********************************************************************************************************
	'Specialized handler for file deletion exception after multiple retries
	'*********************************************************************************************************

#Region "Enums"
	Public Enum RetryExceptionType
		IO_Exception
		Unauthorized_Access_Exception
	End Enum
#End Region

#Region "Module variables"
	Private m_FileName As String
	Private m_ExceptionType As RetryExceptionType
#End Region

#Region "Properties"
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
#End Region

#Region "Methods"
	''' <summary>
	''' Constructor
	''' </summary>
	''' <param name="FileName">Name of file causing exception</param>
	''' <param name="ExceptionType">Exception type</param>
	''' <param name="Message">Message to be returned by exception</param>
	''' <remarks></remarks>
	Public Sub New(ByVal FileName As String, ByVal ExceptionType As RetryExceptionType, ByVal Message As String)

		MyBase.New(Message)
		m_FileName = FileName
		m_ExceptionType = ExceptionType

	End Sub
#End Region

End Class

Public Class AMFileNotDeletedException
	Inherits ApplicationException

	'*********************************************************************************************************
	'Specialized handler for file deletion exception
	'*********************************************************************************************************

#Region "Module variables"
	Private m_FileName As String
#End Region

#Region "Properties"
	Public ReadOnly Property FileName() As String
		Get
			Return m_FileName
		End Get
	End Property
#End Region

#Region "Methods"
	''' <summary>
	''' Constructor
	''' </summary>
	''' <param name="FileName">Name of file causing exception</param>
	''' <param name="Message">Message to be returned by exception</param>
	''' <remarks></remarks>
	Public Sub New(ByVal FileName As String, ByVal Message As String)

		MyBase.New(Message)
		m_FileName = FileName

	End Sub
#End Region

End Class


