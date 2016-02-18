'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2006, Battelle Memorial Institute
' Created 06/07/2006
'
'*********************************************************************************************************

Option Strict On

Imports System.IO
Imports System.Reflection

Public Class clsAssemblyTools

	'*********************************************************************************************************
	'Tools for manipulating and documenting the assemblies used for each analysis job
	'*********************************************************************************************************

#Region "Methods"
	Public Sub GetLoadedAssemblyInfo(ByRef objSummaryFile As clsSummaryFile)
		Dim currentDomain As AppDomain = AppDomain.CurrentDomain

		'Make an array for the list of assemblies.
		Dim assems As System.Reflection.Assembly() = currentDomain.GetAssemblies()

		'List the assemblies in the current application domain.
		Console.WriteLine("List of assemblies loaded in current appdomain:")
		Dim assem As System.Reflection.Assembly
		For Each assem In assems
			''Console.WriteLine(assem.ToString())
			objSummaryFile.Add(assem.ToString())
		Next assem
	End Sub

	Public Sub GetComponentFileVersionInfo(ByRef objSummaryFile As clsSummaryFile)
		' Create a reference to the current directory.
		Dim di As New DirectoryInfo(clsGlobal.GetAppFolderPath())

		' Create an array representing the files in the current directory.
		Dim fi As FileInfo() = di.GetFiles("*.dll")

		' get file version info for files
		Dim fiTemp As FileInfo
		Dim myFVI As FileVersionInfo
		For Each fiTemp In fi
			myFVI = FileVersionInfo.GetVersionInfo(fiTemp.FullName)

			Dim strFileInfo As String
			strFileInfo = "File:             " & fiTemp.FullName & Environment.NewLine

			If Not String.IsNullOrWhiteSpace(myFVI.InternalName) AndAlso myFVI.InternalName <> fiTemp.Name Then
				strFileInfo &= "InternalName:     " & myFVI.InternalName & Environment.NewLine
			End If

			If myFVI.InternalName <> myFVI.OriginalFilename Then
				strFileInfo &= "OriginalFilename: " & myFVI.OriginalFilename & Environment.NewLine
			End If

			If Not String.IsNullOrWhiteSpace(myFVI.ProductName) Then
				strFileInfo &= "Product:          " & myFVI.ProductName & Environment.NewLine
			End If

			strFileInfo &= "ProductVersion:   " & myFVI.ProductVersion & Environment.NewLine

			If myFVI.FileVersion <> myFVI.ProductVersion Then
				strFileInfo &= "FileVersion:      " & myFVI.FileVersion & Environment.NewLine
			End If

			If Not String.IsNullOrWhiteSpace(myFVI.FileDescription) AndAlso myFVI.FileDescription <> myFVI.ProductName Then
				strFileInfo &= "FileDescription:  " & myFVI.FileDescription & Environment.NewLine
			End If

			objSummaryFile.Add(strFileInfo)
		Next fiTemp

	End Sub
#End Region

End Class


