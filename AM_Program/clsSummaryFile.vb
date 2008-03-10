'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2006, Battelle Memorial Institute
' Created 06/07/2006
'
' Last modified 01/16/2008
'*********************************************************************************************************

Imports System.IO
Imports System.Collections.Specialized

Namespace AnalysisManagerBase

	Public Class clsSummaryFile

		'*********************************************************************************************************
		'Provides tools for creating an analysis job summary file
		'*********************************************************************************************************

#Region "Module Variables"
		Private Shared m_lines As New StringCollection
		Private Shared m_FolderNamePath As String
#End Region

#Region "Properties"
		Public Shared Property Path() As String
			Get
				Return m_FolderNamePath
			End Get
			Set(ByVal path As String)
				m_FolderNamePath = path
			End Set
		End Property
#End Region

#Region "Methods"
		''' <summary>
		''' Clears summary file data
		''' </summary>
		''' <remarks></remarks>
		Public Shared Sub Clear()
			m_lines.Clear()
			m_FolderNamePath = ""
		End Sub

		''' <summary>
		''' Writes the summary file to the specified location
		''' </summary>
		''' <param name="ResultFolderNamePath">Location where summary file is to be written</param>
		''' <returns>TRUE for success; FALSE for failure</returns>
		''' <remarks></remarks>
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

		''' <summary>
		''' Adds a line of data to summary file
		''' </summary>
		''' <param name="line">Data to be added</param>
		''' <remarks></remarks>
		Public Shared Sub Add(ByVal line As String)
			m_lines.Add(line)
		End Sub
#End Region

	End Class

End Namespace
