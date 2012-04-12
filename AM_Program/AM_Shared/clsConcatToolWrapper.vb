'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2005, Battelle Memorial Institute
' Started 11/03/2005
'
' Last modified 01/16/2008
'*********************************************************************************************************

Option Strict On

Imports FileConcatenator

Public Class clsConcatToolWrapper

	'*********************************************************************************************************
	'Provides a wrapper around Ken Auberry's file concatenator dll to simplify use
	'Requires FileConcatenator.dll to be referenced in project
	'*********************************************************************************************************

#Region "Enums"
	Public Enum ConcatFileTypes
		CONCAT_DTA
		CONCAT_OUT
		CONCAT_ALL
	End Enum
#End Region

#Region "Module variables"
	Private m_CatInProgress As Boolean = False
	Private WithEvents m_CatTools As IConcatenateFiles
	Private m_ErrMsg As String = ""
	Private m_DataPath As String = ""
	Private m_Progress As Single = 0.0 'Percent complete, 0-100
#End Region

#Region "Properties"
	Public ReadOnly Property Progress() As Single
		Get
			Return m_Progress
		End Get
	End Property

	Public ReadOnly Property ErrMsg() As String
		Get
			Return m_ErrMsg
		End Get
	End Property

	Public Property DataPath() As String
		Get
			Return m_DataPath
		End Get
		Set(ByVal Value As String)
			m_DataPath = Value
		End Set
	End Property
#End Region

#Region "Public Methods"
	Public Sub New(ByVal DataPath As String)

		m_DataPath = DataPath

	End Sub

    Public Function ConcatenateFiles(ByVal FileType As ConcatFileTypes, ByVal RootFileName As String) As Boolean
        Return ConcatenateFiles(FileType, RootFileName, False)
    End Function

    Public Function ConcatenateFiles(ByVal FileType As ConcatFileTypes, _
                                     ByVal RootFileName As String, _
                                     ByVal blnDeleteSourceFilesWhenConcatenating As Boolean) As Boolean

        Try
            'Perform the concatenation
            m_CatTools = New clsConcatenateFiles(m_DataPath, RootFileName)
            m_CatTools.DeleteSourceFilesWhenConcatenating = blnDeleteSourceFilesWhenConcatenating

            m_CatInProgress = True

            'Call the dll based on the concatenation type
            Select Case FileType
                Case ConcatFileTypes.CONCAT_ALL
                    m_CatTools.MakeCattedDTAsAndOUTs()
                Case ConcatFileTypes.CONCAT_DTA
                    m_CatTools.MakeCattedDTAsOnly()
                Case ConcatFileTypes.CONCAT_OUT
                    m_CatTools.MakeCattedOUTsOnly()
                Case Else
                    'Shouldn't ever get here
                    m_ErrMsg = "Invalid concatenation selection: " & FileType.ToString
                    Return False
            End Select

            'Loop until the concatenation finishes
            While m_CatInProgress
                System.Threading.Thread.Sleep(1000)
            End While

            'Concatenation must have finished successfully, so exit
            Return True
        Catch ex As Exception
			m_ErrMsg = "Exception while concatenating files: " & ex.Message & "; " & clsGlobal.GetExceptionStackTrace(ex)
            Return False
        End Try

    End Function
#End Region

#Region "Private methods"
	Private Sub m_CatTools_ErrorNotification(ByVal errorMessage As String) Handles m_CatTools.ErrorNotification
		m_CatInProgress = False
		m_ErrMsg = errorMessage
	End Sub

	'Private Sub m_CatTools_StartingTask(ByVal taskIdentString As String) Handles m_CatTools.StartingTask
	'	m_CatInProgress = True
	'End Sub

	Private Sub m_CatTools_EndingTask() Handles m_CatTools.EndTask
		m_CatInProgress = False
	End Sub

	Private Sub m_CatTools_Progress(ByVal fractionDone As Double) Handles m_CatTools.Progress
		m_Progress = CSng(100.0 * fractionDone)
	End Sub
#End Region

End Class
