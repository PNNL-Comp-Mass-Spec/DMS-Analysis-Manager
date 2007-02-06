Imports System.IO
Imports ICSharpCode.SharpZipLib.Zip

Public Class clsSharpZipWrapper

	'Provides wrapper around #ZipLib file zip utility

#Region "Module variables"
	Dim m_ErrMsg As String
#End Region

#Region "Properties"
	Public ReadOnly Property ErrMsg() As String
		Get
			Return m_ErrMsg
		End Get
	End Property
#End Region

	Public Function ExtractAllFilesToOneFolder(ByVal ZipFileName As String, ByVal TargetDir As String) As Boolean

		'Unzips all files in specified input zip into the target directory

		m_ErrMsg = ""

		'Verify input parameters
		If Not File.Exists(ZipFileName) Then
			m_ErrMsg = "File not found: " & ZipFileName
			Return False
		End If

		If Not Directory.Exists(TargetDir) Then
			m_ErrMsg = "Target directory not found: " & TargetDir
			Return False
		End If

		Dim UnZipper As New FastZip
		Try
			UnZipper.ExtractZip(ZipFileName, TargetDir, String.Empty)
			Return True
		Catch ex As Exception
			m_ErrMsg = "Exception unzipping file " & ZipFileName & " : " & ex.Message
			Return False
		End Try

	End Function

	Public Function ZipFilesInFolder(ByVal ZipFileNamePath As String, ByVal SourceDir As String, ByVal Recurse As Boolean, ByVal FileFilter As String) As Boolean

		'Zips all files specified in source directory and file folder to a single zip file

		'Verify input parameters
		If Not Directory.Exists(SourceDir) Then
			m_ErrMsg = "Source directory not found: " & SourceDir
			Return False
		End If

		If Not Directory.Exists(Path.GetDirectoryName(ZipFileNamePath)) Then
			m_ErrMsg = "Target path not found: " & Path.GetFullPath(ZipFileNamePath)
			Return False
		End If

		'Zip the files
		Dim Zipper As New FastZip
		Try
			Zipper.CreateZip(ZipFileNamePath, SourceDir, Recurse, FileFilter)
			Return True
		Catch ex As Exception
			m_ErrMsg = "Exception creating zip file " & ZipFileNamePath & ": " & ex.Message
			Return False
		End Try

	End Function

End Class
