Imports System.IO
Imports System.Reflection

Public Class clsAssemblyTools

	'Tools for manipulating and documenting the assemblies used for each analysis job
	Public Shared Sub GetLoadedAssemblyInfo()
		Dim currentDomain As AppDomain = AppDomain.CurrentDomain

		'Make an array for the list of assemblies.
		Dim assems As [Assembly]() = currentDomain.GetAssemblies()

		'List the assemblies in the current application domain.
		Console.WriteLine("List of assemblies loaded in current appdomain:")
		Dim assem As [Assembly]
		For Each assem In assems
			''Console.WriteLine(assem.ToString())
			AnalysisManagerBase.clsSummaryFile.Add(assem.ToString())
		Next assem
	End Sub

	Public Shared Sub GetComponentFileVersionInfo()
		' Create a reference to the current directory.
		Dim di As New DirectoryInfo(Environment.CurrentDirectory)

		' Create an array representing the files in the current directory.
		Dim fi As FileInfo() = di.GetFiles("*.dll")

		' get file version info for files
		Dim fiTemp As FileInfo
		Dim myFVI As FileVersionInfo
		For Each fiTemp In fi
			myFVI = FileVersionInfo.GetVersionInfo(Path.GetFullPath(fiTemp.Name))
			''Console.WriteLine(myFVI.ToString)
			AnalysisManagerBase.clsSummaryFile.Add(myFVI.ToString)
		Next fiTemp
	End Sub

End Class
