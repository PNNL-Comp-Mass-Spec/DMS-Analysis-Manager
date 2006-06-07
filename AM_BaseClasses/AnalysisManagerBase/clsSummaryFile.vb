Imports System.IO
Imports System.Collections.Specialized

Public Class clsSummaryFile

	Private Shared m_lines As New StringCollection
	Private Shared m_FolderNamePath As String

	Public Shared Property Path() As String
		Get
			Return m_FolderNamePath
		End Get
		Set(ByVal path As String)
			m_FolderNamePath = path
		End Set
	End Property

	Public Shared Sub Clear()
		m_lines.Clear()
		m_FolderNamePath = ""
	End Sub

	Public Shared Function SaveSummaryFile(ByVal ResultFolderNamePath As String) As Boolean
		Dim LogFile As StreamWriter

		Try
			LogFile = File.CreateText(ResultFolderNamePath)

			For Each DumString As String In m_lines
				LogFile.WriteLine(DumString)
			Next

			LogFile.Close()
			LogFile = Nothing
			Return True
		Catch Err As Exception
			Return False
		End Try

	End Function

	Public Shared Sub Add(ByVal line As String)
		m_lines.Add(line)
	End Sub

End Class
