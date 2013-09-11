Option Strict On

Imports System.IO
Imports System.Text.RegularExpressions

Public Class clsMyEMSLDatasetInfo

	Protected Const MYEMSL_FILEID_TAG As String = "@MyEMSLID_"

	Protected mDatasetName As String
	Protected mDatasetID As Integer
	Protected mErrorMessages As List(Of String)

	Protected mArchivedFiles As List(Of MyEMSLReader.ArchivedFileInfo)
	Protected mCacheDate As System.DateTime

	' Keys are the full paths to the downloaded file, values are extended file info
	Protected mDownloadedFiles As Dictionary(Of String, MyEMSLReader.ArchivedFileInfo)

	' Keys are MyEMSL File IDs, values are True to Unzip, false otherwise
	Protected mFilesToDownload As Dictionary(Of Int64, Boolean)
	Protected mLastProgressWriteTime As System.DateTime

	Protected WithEvents mReader As MyEMSLReader.Reader
	Protected WithEvents mDownloader As MyEMSLReader.Downloader

	Public Structure udtMyEMSLFileInfoType
		Public FileID As Int64					' Will be 0 if this is a folder
		Public IsFolder As Boolean
		Public FileInfo As MyEMSLReader.ArchivedFileInfo
	End Structure

#Region "Properties"

	Public ReadOnly Property ArchivedFiles As List(Of MyEMSLReader.ArchivedFileInfo)
		Get
			Return mArchivedFiles
		End Get
	End Property

	Public ReadOnly Property DatasetID As Integer
		Get
			Return mDatasetID
		End Get
	End Property

	Public ReadOnly Property DatasetName As String
		Get
			Return mDatasetName
		End Get
	End Property

	''' <summary>
	''' The most recently downloaded files; keys are the full paths to the downloaded file, values are extended file info
	''' </summary>
	''' <value></value>
	''' <returns></returns>
	''' <remarks></remarks>
	Public ReadOnly Property DownloadedFiles As Dictionary(Of String, MyEMSLReader.ArchivedFileInfo)
		Get
			Return mDownloadedFiles
		End Get
	End Property

	Public ReadOnly Property ErrorMessages As List(Of String)
		Get
			Return mErrorMessages
		End Get
	End Property

	Public ReadOnly Property FilesToDownload As Dictionary(Of Int64, Boolean)
		Get
			Return mFilesToDownload
		End Get
	End Property
#End Region

	''' <summary>
	''' Constructor
	''' </summary>
	''' <param name="datasetName"></param>
	''' <remarks></remarks>
	Public Sub New(ByVal datasetName As String)

		mDatasetName = String.Empty
		mDatasetID = 0
		mErrorMessages = New List(Of String)

		mReader = New MyEMSLReader.Reader()
		mArchivedFiles = New List(Of MyEMSLReader.ArchivedFileInfo)

		mDownloadedFiles = New Dictionary(Of String, MyEMSLReader.ArchivedFileInfo)

		mFilesToDownload = New Dictionary(Of Int64, Boolean)
		mLastProgressWriteTime = System.DateTime.UtcNow

		RefreshInfo(datasetName)
	End Sub

	Public Sub AddFileToDownloadQueue(ByVal myEMSLFileID As Int64)
		AddFileToDownloadQueue(myEMSLFileID, False)
	End Sub

	Public Sub AddFileToDownloadQueue(ByVal myEMSLFileID As Int64, ByVal Unzip As Boolean)
		If Not mFilesToDownload.ContainsKey(myEMSLFileID) Then
			mFilesToDownload.Add(myEMSLFileID, Unzip)
		End If
	End Sub

	''' <summary>
	''' Appends the MyEMSL File ID tag to a given file path
	''' </summary>
	''' <param name="filePath">Path to which the MyEMSL FileID should be appended</param>
	''' <param name="myEmslFileID">MyEMSL File ID</param>
	''' <returns>New path, for example QC_Shew_13-04_pt1_1_1_31Jul13_Cheetah_13-07-01.raw@MyEMSLID_84327</returns>
	''' <remarks></remarks>
	Public Shared Function AppendMyEMSLFileID(ByVal filePath As String, ByVal myEmslFileID As Int64) As String
		Return filePath & MYEMSL_FILEID_TAG & myEmslFileID.ToString()
	End Function

	Public Sub ClearDownloadQueue()
		mFilesToDownload.Clear()
		mDownloadedFiles.Clear()
	End Sub

	''' <summary>
	''' Parses a path that contains the MyEMSL FileID tag
	''' </summary>
	''' <param name="filePath">Path to parse, for example QC_Shew_13-04_pt1_1_1_31Jul13_Cheetah_13-07-01.raw@MyEMSLID_84327</param>
	''' <returns>MyEMSL File ID if successfully parsed, 0 if not present or a problem</returns>
	''' <remarks></remarks>
	Public Shared Function ExtractMyEMSLFileID(ByVal filePath As String) As Int64
		Dim newFilePath As String = String.Empty
		Return ExtractMyEMSLFileID(filePath, newFilePath)
	End Function

	''' <summary>
	''' Parses a path that contains the MyEMSL FileID tag
	''' </summary>
	''' <param name="filePath">Path to parse, for example QC_Shew_13-04_pt1_1_1_31Jul13_Cheetah_13-07-01.raw@MyEMSLID_84327</param>
	''' <param name="newFilePath">Path with the MyEMSL FileID tag removed, for example QC_Shew_13-04_pt1_1_1_31Jul13_Cheetah_13-07-01.raw</param>
	''' <returns>MyEMSL File ID if successfully parsed, 0 if not present or a problem</returns>
	''' <remarks></remarks>
	Public Shared Function ExtractMyEMSLFileID(ByVal filePath As String, ByRef newFilePath As String) As Int64

		Dim charIndex As Integer = filePath.LastIndexOf(MYEMSL_FILEID_TAG)
		newFilePath = String.Empty

		If charIndex > 0 Then
			newFilePath = filePath.Substring(0, charIndex)

			Dim myEmslFileID As Int64
			Dim myEmslFileIdText As String = filePath.Substring(charIndex + MYEMSL_FILEID_TAG.Length)

			If Int64.TryParse(myEmslFileIdText, myEmslFileID) Then
				Return myEmslFileID
			End If

		End If

		Return 0

	End Function

	''' <summary>
	''' Looks for the given file, returning any matches as a list
	''' </summary>
	''' <param name="fileName">File name to find; can contain a wildcard, e.g. *.zip</param>
	''' <returns>List of matching files</returns>
	''' <remarks></remarks>
	Public Function FindFiles(ByVal fileName As String) As List(Of udtMyEMSLFileInfoType)
		Dim subFolderName As String = String.Empty
		Dim recurse As Boolean = True
		Return FindFiles(fileName, subFolderName, recurse)
	End Function

	''' <summary>
	''' Looks for the given file, returning any matches as a list
	''' </summary>
	''' <param name="fileName">File name to find; can contain a wildcard, e.g. *.zip</param>
	''' <param name="subFolderName">Subfolder in which the file must reside; can contain a wildcard, e.g. SIC*</param>
	''' <param name="recurse">True to search all subfolders; false to only search the root folder (or only subFolderName)</param>
	''' <returns>List of matching files</returns>
	''' <remarks></remarks>
	Public Function FindFiles(ByVal fileName As String, ByVal subFolderName As String, ByVal recurse As Boolean) As List(Of udtMyEMSLFileInfoType)

		' Re-query the web service if the information is out-of-date
		RefreshInfoIfStale()

		Dim lstMatches As New List(Of udtMyEMSLFileInfoType)

		If String.IsNullOrEmpty(fileName) Then
			Return lstMatches
		End If

		If mArchivedFiles.Count = 0 Then
			Return lstMatches
		End If

		Dim reFile As Regex = GetFileSearchRegEx(fileName)
		Dim reFolder As Regex

		If Not String.IsNullOrEmpty(subFolderName) Then
			reFolder = GetFileSearchRegEx(subFolderName)
		Else
			reFolder = GetFileSearchRegEx("*")
		End If

		For Each archivedFile In mArchivedFiles
			If reFile.IsMatch(archivedFile.Filename) Then
				Dim fiFile = New FileInfo(archivedFile.RelativePathWindows)
				Dim isMatch As Boolean = True

				If String.IsNullOrEmpty(subFolderName) Then
					' Validate that the file resides in the appropriate folder
					If Not recurse AndAlso archivedFile.RelativePathWindows.Contains("\") Then
						' Invalid match
						isMatch = False
					End If
				Else
					' Require a subfolder match
					isMatch = reFolder.IsMatch(fiFile.Directory.Name)
					If recurse And Not isMatch Then
						' Need to test all of the folders for a match to the specified folder
						Dim pathParts As List(Of String) = archivedFile.RelativePathWindows.Split("\"c).ToList()
						For Each pathPart In pathParts
							If reFolder.IsMatch(pathPart) Then
								isMatch = True
								Exit For
							End If
						Next
					End If
				End If

				If isMatch Then
					Dim udtMatch = New udtMyEMSLFileInfoType

					udtMatch.FileID = archivedFile.FileID
					udtMatch.IsFolder = False
					udtMatch.FileInfo = archivedFile

					lstMatches.Add(udtMatch)
				End If
			End If

		Next

		Return lstMatches

	End Function

	''' <summary>
	''' Looks for the given folder, returning any matches as a list
	''' </summary>
	''' <param name="folderName">Folder name to find; can contain a wildcard, e.g. SIC*</param>
	''' <returns>List of matching folders</returns>
	''' <remarks></remarks>
	Public Function FindFolders(ByVal folderName As String) As List(Of udtMyEMSLFileInfoType)

		' Re-query the web service if the information is out-of-date
		RefreshInfoIfStale()

		Dim lstMatches As New List(Of udtMyEMSLFileInfoType)

		If String.IsNullOrEmpty(folderName) Then
			Return lstMatches
		End If

		Dim reFolder As Regex = GetFileSearchRegEx(folderName)

		For Each archivedFile In mArchivedFiles
			If archivedFile.RelativePathWindows.IndexOf("\") < 0 Then
				Continue For
			End If

			Dim fiFile = New FileInfo(archivedFile.RelativePathWindows)
			If Not reFolder.IsMatch(fiFile.Directory.Name) Then
				Continue For
			End If

			Dim relativeFolderPath As String = String.Copy(archivedFile.RelativePathWindows)
			Dim charIndex = relativeFolderPath.LastIndexOf("\")

			If charIndex > 0 Then
				relativeFolderPath = relativeFolderPath.Substring(0, charIndex)
			Else
				' This is a programming bug
				Throw New ArgumentOutOfRangeException("Forward slash not found in the relative file path; this code should not be reached")
			End If

			Dim udtMatch = New udtMyEMSLFileInfoType
			udtMatch.FileID = 0
			udtMatch.IsFolder = True
			udtMatch.FileInfo = New MyEMSLReader.ArchivedFileInfo(mDatasetName, folderName, relativeFolderPath)

			lstMatches.Add(udtMatch)

		Next

		Return lstMatches

	End Function

	Protected Function GetFileSearchRegEx(ByVal name As String) As Regex
		Dim strSearchSpec As String = "^" & name & "$"
		strSearchSpec = strSearchSpec.Replace("*", ".*")

		Return New Regex(strSearchSpec, RegexOptions.Compiled Or RegexOptions.IgnoreCase)
	End Function

	Public Function ProcessDownloadQueue(ByVal downloadFolderPath As String, ByVal folderLayout As MyEMSLReader.Downloader.DownloadFolderLayout) As Boolean

		mErrorMessages.Clear()

		If mFilesToDownload.Count = 0 Then
			mErrorMessages.Add("Download queue is empty; nothing to download")
			Return False
		End If

		Try
			mDownloader = New MyEMSLReader.Downloader()

			Dim success = mDownloader.DownloadFiles(mFilesToDownload.Keys.ToList(), downloadFolderPath, folderLayout)

			If success Then
				mDownloadedFiles = mDownloader.DownloadedFiles()

				For Each file In mFilesToDownload
					If file.Value Then
						' Unzip this file if it exists and ends in .zip

						Dim ionicZipTools = New clsIonicZipTools(1, downloadFolderPath)

						For Each archivedFile In mDownloadedFiles
							If archivedFile.Value.FileID = file.Key Then
								Dim fiFileToUnzip = New FileInfo(Path.Combine(downloadFolderPath, archivedFile.Value.Filename))

								If fiFileToUnzip.Exists AndAlso fiFileToUnzip.Extension.ToLower() = ".zip" Then
									clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Unzipping file " + fiFileToUnzip.Name)

									ionicZipTools.UnzipFile(fiFileToUnzip.FullName, downloadFolderPath)

								End If
								
								Exit For
							End If
						Next

					End If
				Next
				mFilesToDownload.Clear()

			End If

			Return success

		Catch ex As Exception
			mErrorMessages.Add("Error in RefreshInfo: " & ex.Message)
			Return False
		End Try

	End Function

	''' <summary>
	''' Refresh the cached file info
	''' </summary>
	''' <returns>True if success, false if an error</returns>
	''' <remarks></remarks>
	Public Function RefreshInfo() As Boolean
		Return RefreshInfo(mDatasetName)
	End Function

	''' <summary>
	''' Refresh the cached file info
	''' </summary>
	''' <param name="strDatasetName">Dataset name to lookup</param>
	''' <returns>True if success, false if an error</returns>
	''' <remarks></remarks>
	Public Function RefreshInfo(ByVal strDatasetName As String) As Boolean

		Try
			mErrorMessages.Clear()

			If strDatasetName <> mDatasetName Then
				mDatasetName = strDatasetName
				mDatasetID = 0
			End If

			mArchivedFiles = mReader.FindFilesByDatasetName(mDatasetName)
			mCacheDate = System.DateTime.UtcNow

			If mArchivedFiles.Count = 0 Then
				If mErrorMessages.Count = 0 Then
					Return True
				Else
					Return False
				End If
			End If

			mDatasetID = mArchivedFiles.First.DatasetID

			Return True

		Catch ex As Exception
			mErrorMessages.Add("Error in RefreshInfo: " & ex.Message)
			Return False
		End Try

	End Function

	''' <summary>
	''' Refresh the cached file info if over 5 minutes have elapsed
	''' </summary>
	''' <returns>True if success, false if an error</returns>
	''' <remarks></remarks>
	Protected Function RefreshInfoIfStale() As Boolean
		If System.DateTime.UtcNow.Subtract(mCacheDate).TotalMinutes > 5 Then
			Return RefreshInfo()
		Else
			Return True
		End If
	End Function

#Region "Event handlers"
	Private Sub mReader_ErrorEvent(sender As Object, e As MyEMSLReader.MessageEventArgs) Handles mReader.ErrorEvent
		mErrorMessages.Add(e.Message)
		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "MyEMSL reader error in clsMyEMSLDatasetInfo: " + e.Message)
	End Sub

	Private Sub mReader_MessageEvent(sender As Object, e As MyEMSLReader.MessageEventArgs) Handles mReader.MessageEvent
		Console.WriteLine(e.Message)
		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "MyEMSL reader: " + e.Message)
	End Sub

	Private Sub mReader_ProgressEvent(sender As Object, e As MyEMSLReader.ProgressEventArgs) Handles mReader.ProgressEvent
		' Do not log anything here
	End Sub

	Private Sub mDownloader_ErrorEvent(sender As Object, e As MyEMSLReader.MessageEventArgs) Handles mDownloader.ErrorEvent
		mErrorMessages.Add(e.Message)
		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "MyEMSL downloader error in clsMyEMSLDatasetInfo: " + e.Message)
	End Sub

	Private Sub mDownloader_MessageEvent(sender As Object, e As MyEMSLReader.MessageEventArgs) Handles mDownloader.MessageEvent
		Console.WriteLine(e.Message)
		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "MyEMSL downloader: " + e.Message)
	End Sub

	Private Sub mDownloader_ProgressEvent(sender As Object, e As MyEMSLReader.ProgressEventArgs) Handles mDownloader.ProgressEvent
		If System.DateTime.UtcNow.Subtract(mLastProgressWriteTime).TotalMinutes > 0.2 Then
			mLastProgressWriteTime = System.DateTime.UtcNow
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "MyEMSL downloader: " & e.PercentComplete & "% complete")
		End If
	End Sub
#End Region


End Class
