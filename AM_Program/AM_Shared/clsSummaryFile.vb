'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2006, Battelle Memorial Institute
' Created 06/07/2006
'
'*********************************************************************************************************

Option Strict On

Imports System.IO

Public Class clsSummaryFile

	'*********************************************************************************************************
	'Provides tools for creating an analysis job summary file
	'*********************************************************************************************************

#Region "Module Variables"
	' ReSharper disable once FieldCanBeMadeReadOnly.Local
	Private m_lines As New List(Of String)
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

		Try
			Using swSummaryFile = New StreamWriter(New FileStream(AnalysisSummaryFilePath, FileMode.Create, FileAccess.Write, FileShare.Read))

				For Each DumString As String In m_lines
					swSummaryFile.WriteLine(DumString)
				Next

			End Using

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


