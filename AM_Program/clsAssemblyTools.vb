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
Imports System.Reflection

Namespace AnalysisManagerBase

	Public Class clsAssemblyTools

		'*********************************************************************************************************
		'Tools for manipulating and documenting the assemblies used for each analysis job
		'*********************************************************************************************************

#Region "Methods"
		Public Shared Sub GetLoadedAssemblyInfo()
			Dim currentDomain As AppDomain = AppDomain.CurrentDomain

			'Make an array for the list of assemblies.
			Dim assems As [Assembly]() = currentDomain.GetAssemblies()

			'List the assemblies in the current application domain.
			Console.WriteLine("List of assemblies loaded in current appdomain:")
			Dim assem As [Assembly]
			For Each assem In assems
				''Console.WriteLine(assem.ToString())
				clsSummaryFile.Add(assem.ToString())
			Next assem
		End Sub

		Public Shared Sub GetComponentFileVersionInfo()
			' Create a reference to the current directory.
			Dim di As New DirectoryInfo(Path.GetDirectoryName(System.Windows.Forms.Application.ExecutablePath))

			' Create an array representing the files in the current directory.
			Dim fi As FileInfo() = di.GetFiles("*.dll")

			' get file version info for files
			Dim fiTemp As FileInfo
			Dim myFVI As FileVersionInfo
			For Each fiTemp In fi
				myFVI = FileVersionInfo.GetVersionInfo(fiTemp.FullName)
				''Console.WriteLine(myFVI.ToString)
				clsSummaryFile.Add(myFVI.ToString)
			Next fiTemp

		End Sub
#End Region

	End Class

End Namespace
