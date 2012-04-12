'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2006, Battelle Memorial Institute
' Created 06/07/2006
'
' Last modified 01/16/2008
'*********************************************************************************************************

Option Strict On

Imports System.IO



Public Class clsSummaryFile

	'*********************************************************************************************************
	'Provides tools for creating an analysis job summary file
	'*********************************************************************************************************

#Region "Module Variables"
	Private m_lines As New System.Collections.Generic.List(Of String)
	Private m_FolderNamePath As String
#End Region

#Region "Methods"
	''' <summary>
	''' Clears summary file data
	''' </summary>
	''' <remarks></remarks>
	Public Sub Clear()
		m_lines.Clear()
	End Sub

	''' <summary>
	''' Writes the summary file to the specified location
	''' </summary>
	''' <param name="AnalysisSummaryFilePath">Full path of summary file to create</param>
	''' <returns>TRUE for success; FALSE for failure</returns>
	''' <remarks></remarks>
	Public Function SaveSummaryFile(ByVal AnalysisSummaryFilePath As String) As Boolean
		Dim LogFile As StreamWriter

		Try
			LogFile = File.CreateText(AnalysisSummaryFilePath)

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
	Public Sub Add(ByVal line As String)
		m_lines.Add(line)
	End Sub
#End Region

End Class


