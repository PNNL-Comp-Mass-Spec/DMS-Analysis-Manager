Imports System.IO
Imports System.Runtime.InteropServices
Imports MyEMSLReader

Public Class clsMyEMSLUtilities
    Inherits clsEventNotifier

    Public Const MYEMSL_PATH_FLAG As String = "\\MyEMSL"

    Private ReadOnly m_IonicZipTools As clsIonicZipTools

    Private WithEvents m_MyEMSLDatasetListInfo As DatasetListInfo

    Private ReadOnly m_AllFoundMyEMSLFiles As List(Of DatasetFolderOrFileInfo)

    Private m_RecentlyFoundMyEMSLFiles As List(Of DatasetFolderOrFileInfo)
    
    Private m_LastMyEMSLProgressWriteTime As DateTime = DateTime.UtcNow

    Private ReadOnly mFileIDComparer As clsMyEMSLFileIDComparer

    Private ReadOnly m_MostRecentUnzippedFiles As List(Of KeyValuePair(Of String, String))

#Region "Events"
    Public Event FileDownloaded As FileDownloadedEventHandler

#End Region

#Region "Properties"

    ''' <summary>
    ''' The most recently downloaded files; keys are the full paths to the downloaded file, values are extended file info
    ''' </summary>
    ''' <value></value>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public ReadOnly Property DownloadedFiles() As Dictionary(Of String, ArchivedFileInfo)
        Get
            Return m_MyEMSLDatasetListInfo.DownloadedFiles
        End Get
    End Property

    ''' <summary>
    ''' MyEMSL IDs of files queued to be downloaded
    ''' </summary>
    ''' <value></value>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public ReadOnly Property FilesToDownload As Dictionary(Of Int64, DownloadQueue.udtFileToDownload)
        Get
            Return m_MyEMSLDatasetListInfo.FilesToDownload
        End Get
    End Property

    ''' <summary>
    ''' All files found in MyEMSL via calls to FindFiles
    ''' </summary>
    ''' <value></value>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public ReadOnly Property AllFoundMyEMSLFiles As List(Of DatasetFolderOrFileInfo)
        Get
            Return m_AllFoundMyEMSLFiles
        End Get
    End Property

    ''' <summary>
    ''' Returns the files most recently unzipped
    ''' Keys in the KeyValuePairs are filenames while values are relative paths (in case the .zip file has folders)
    ''' </summary>
    ''' <value></value>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public ReadOnly Property MostRecentUnzippedFiles() As List(Of KeyValuePair(Of String, String))
        Get
            Return m_MostRecentUnzippedFiles
        End Get
    End Property

    ''' <summary>
    ''' Files most recently found via a call to FindFiles
    ''' </summary>
    ''' <value></value>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public ReadOnly Property RecentlyFoundMyEMSLFiles As List(Of DatasetFolderOrFileInfo)
        Get
            Return m_RecentlyFoundMyEMSLFiles
        End Get
    End Property
#End Region

    ''' <summary>
    ''' Constructor
    ''' </summary>
    ''' <param name="debugLevel">Debug level (higher number means more messages)</param>
    ''' <param name="workingDir">Working directory path</param>
    ''' <remarks></remarks>
    Public Sub New(debugLevel As Integer, workingDir As String)
        m_MyEMSLDatasetListInfo = New DatasetListInfo()

        m_AllFoundMyEMSLFiles = New List(Of DatasetFolderOrFileInfo)
        m_RecentlyFoundMyEMSLFiles = New List(Of DatasetFolderOrFileInfo)

        m_IonicZipTools = New clsIonicZipTools(debugLevel, workingDir)

        mFileIDComparer = New clsMyEMSLFileIDComparer()

        m_MostRecentUnzippedFiles = New List(Of KeyValuePair(Of String, String))
    End Sub

    ''' <summary>
    ''' Append a file to a folder path that ends with @MyEMSLID_12345
    ''' </summary>
    ''' <param name="myEmslFolderPath">Folder path to which fileName will be appended</param>
    ''' <param name="fileName">Filename to append</param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public Shared Function AddFileToMyEMSLFolderPath(myEmslFolderPath As String, fileName As String) As String

        Dim folderPathClean As String = Nothing
        Dim myEMSLFileID As Int64 = DatasetInfoBase.ExtractMyEMSLFileID(myEmslFolderPath, folderPathClean)

        Dim filePath = Path.Combine(folderPathClean, fileName)

        If myEMSLFileID = 0 Then
            Return filePath
        Else
            Return DatasetInfoBase.AppendMyEMSLFileID(filePath, myEMSLFileID)
        End If

    End Function

    ''' <summary>
    ''' Make sure that the MyEMSL DatasetListInfo class knows to search for the given dataset
    ''' </summary>
    ''' <param name="datasetName">Dataset name</param>
    ''' <remarks></remarks>
    Public Sub AddDataset(datasetName As String)
        If (Not m_MyEMSLDatasetListInfo.ContainsDataset(datasetName)) Then
            m_MyEMSLDatasetListInfo.AddDataset(datasetName)
        End If
    End Sub

    ''' <summary>
    ''' Queue a file to be downloaded
    ''' </summary>
    ''' <param name="fileInfo">Archive File Info</param>
    ''' <remarks></remarks>
    Public Sub AddFileToDownloadQueue(fileInfo As ArchivedFileInfo)
        m_MyEMSLDatasetListInfo.AddFileToDownloadQueue(fileInfo)
    End Sub

    ''' <summary>
    ''' Queue a file to be downloaded
    ''' </summary>
    ''' <param name="encodedFilePath">File path that includes @MyEMSLID_12345</param>
    ''' <remarks></remarks>
    Public Function AddFileToDownloadQueue(encodedFilePath As String, Optional unzipRequired As Boolean = False) As Boolean

        Dim myEMSLFileID As Int64 = DatasetInfoBase.ExtractMyEMSLFileID(encodedFilePath)

        If myEMSLFileID > 0 Then

            Dim matchingFileInfo As ArchivedFileInfo = Nothing

            If Not GetCachedArchivedFileInfo(myEMSLFileID, matchingFileInfo) Then
                ' File not found in m_RecentlyFoundMyEMSLFiles
                ' Instead check m_AllFoundMyEMSLFiles

                Dim fileInfoQuery = (
                  From item In m_AllFoundMyEMSLFiles
                  Where item.FileID = myEMSLFileID
                  Select item.FileInfo).ToList()

                If fileInfoQuery.Count = 0 Then
                    OnErrorEvent("Cached ArchiveFileInfo does not contain MyEMSL File ID " & myEMSLFileID)
                    Return False
                Else
                    matchingFileInfo = fileInfoQuery.First()
                End If
            End If

            AddDataset(matchingFileInfo.Dataset)
            m_MyEMSLDatasetListInfo.AddFileToDownloadQueue(matchingFileInfo, unzipRequired)
            Return True

        Else
            OnErrorEvent("MyEMSL File ID not found in path: " & encodedFilePath)
            Return False
        End If

    End Function

    ''' <summary>
    ''' Clear the list of MyEMSL files found via calls to FindFiles
    ''' </summary>
    ''' <remarks></remarks>
    Public Sub ClearAllFoundfiles()
        m_AllFoundMyEMSLFiles.Clear()
    End Sub

    ''' <summary>
    ''' Clear the queue of files to download
    ''' </summary>
    ''' <remarks></remarks>
    Public Sub ClearDownloadQueue()
        m_MyEMSLDatasetListInfo.FilesToDownload.Clear()
    End Sub
    
    ''' <summary>
    ''' Look for the given file (optionally in a given subfolder) for the given dataset
    ''' </summary>
    ''' <param name="fileName">File name to find; can contain a wildcard, e.g. *.zip</param>
    ''' <param name="subFolderName">Subfolder in which the file must reside; can contain a wildcard, e.g. SIC*</param>
    ''' <param name="datasetName">Dataset name filter</param>
    ''' <param name="recurse">True to search all subfolders; false to only search the root folder (or only subFolderName)</param>
    ''' <returns>List of matching files</returns>
    ''' <remarks>subFolderName can contain a partial path, for example 2013_09_10_DPB_Unwashed_Media_25um.d\2013_09_10_In_1sec_1MW.m</remarks>
    Public Function FindFiles(fileName As String, subFolderName As String, datasetName As String, recurse As Boolean) As List(Of DatasetFolderOrFileInfo)

        ' Make sure the dataset name is being tracked by m_MyEMSLDatasetListInfo
        AddDataset(datasetName)

        m_RecentlyFoundMyEMSLFiles = m_MyEMSLDatasetListInfo.FindFiles(fileName, subFolderName, datasetName, recurse)

        Dim filesToAdd = m_RecentlyFoundMyEMSLFiles.Except(m_AllFoundMyEMSLFiles, mFileIDComparer)

        m_AllFoundMyEMSLFiles.AddRange(filesToAdd)

        Return m_RecentlyFoundMyEMSLFiles

    End Function

    Private Function GetCachedArchivedFileInfo(myEMSLFileID As Int64, <Out()> ByRef matchingFileInfo As ArchivedFileInfo) As Boolean

        matchingFileInfo = Nothing

        Dim fileInfoQuery = (
          From item In m_RecentlyFoundMyEMSLFiles
          Where item.FileID = myEMSLFileID
          Select item.FileInfo).ToList()

        If fileInfoQuery.Count = 0 Then
            Return False
        Else
            matchingFileInfo = fileInfoQuery.First()
            Return True
        End If

    End Function

    ''' <summary>
    ''' Retrieve queued files from MyEMSL
    ''' </summary>
    ''' <param name="downloadFolderPath">Target folder path (ignored for files defined in dctDestFilePathOverride)</param>
    ''' <param name="folderLayout">Folder Layout (ignored for files defined in dctDestFilePathOverride)</param>
    ''' <returns>True if success, false if an error</returns>
    ''' <remarks>Returns True if the download queue is empty</remarks>
    Public Function ProcessMyEMSLDownloadQueue(downloadFolderPath As String, folderLayout As Downloader.DownloadFolderLayout) As Boolean

        If m_MyEMSLDatasetListInfo.FilesToDownload.Count = 0 Then
            ' Nothing to download; that's OK
            Return True
        End If

        m_MostRecentUnzippedFiles.Clear()

        Dim success = m_MyEMSLDatasetListInfo.ProcessDownloadQueue(downloadFolderPath, folderLayout)
        If success Then Return True

        If m_MyEMSLDatasetListInfo.ErrorMessages.Count > 0 Then
            OnErrorEvent("Error in ProcessMyEMSLDownloadQueue: " & m_MyEMSLDatasetListInfo.ErrorMessages.First())
        Else
            OnErrorEvent("Unknown error in ProcessMyEMSLDownloadQueue")
        End If

        Return False

    End Function

#Region "MyEMSL Event Handlers"

    Private Sub m_MyEMSLDatasetListInfo_ErrorEvent(sender As Object, e As MessageEventArgs) Handles m_MyEMSLDatasetListInfo.ErrorEvent
        OnErrorEvent(e.Message)
    End Sub

    Private Sub m_MyEMSLDatasetListInfo_MessageEvent(sender As Object, e As MessageEventArgs) Handles m_MyEMSLDatasetListInfo.MessageEvent
        Console.WriteLine(e.Message)
        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, e.Message)
    End Sub

    Private Sub m_MyEMSLDatasetListInfo_ProgressEvent(sender As Object, e As ProgressEventArgs) Handles m_MyEMSLDatasetListInfo.ProgressEvent
        If DateTime.UtcNow.Subtract(m_LastMyEMSLProgressWriteTime).TotalMinutes > 0.2 Then
            m_LastMyEMSLProgressWriteTime = DateTime.UtcNow
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "MyEMSL downloader: " & e.PercentComplete & "% complete")
        End If
    End Sub

    Private Sub m_MyEMSLDatasetListInfo_FileDownloadedEvent(sender As Object, e As FileDownloadedEventArgs) Handles m_MyEMSLDatasetListInfo.FileDownloadedEvent

        If e.UnzipRequired Then
            Dim fiFileToUnzip = New FileInfo(Path.Combine(e.DownloadFolderPath, e.ArchivedFile.Filename))

            If Not fiFileToUnzip.Exists Then Return

            If fiFileToUnzip.Extension.ToLower() = ".zip" Then
                ' Decompress the .zip file
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Unzipping file " + fiFileToUnzip.Name)
                m_IonicZipTools.UnzipFile(fiFileToUnzip.FullName, e.DownloadFolderPath)
                m_MostRecentUnzippedFiles.AddRange(m_IonicZipTools.MostRecentUnzippedFiles)
            ElseIf fiFileToUnzip.Extension.ToLower() = ".gz" Then
                ' Decompress the .gz file
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Unzipping file " + fiFileToUnzip.Name)
                m_IonicZipTools.GUnzipFile(fiFileToUnzip.FullName, e.DownloadFolderPath)
                m_MostRecentUnzippedFiles.AddRange(m_IonicZipTools.MostRecentUnzippedFiles)
            End If

        End If

        RaiseEvent FileDownloaded(sender, e)
    End Sub
#End Region

    ''' <summary>
    ''' Determines whether two DatasetFolderOrFileInfo instances refer to the same file in MyEMSL
    ''' </summary>
    ''' <remarks>Compares the value of FileID in the two instances</remarks>
    Private Class clsMyEMSLFileIDComparer
        Implements IEqualityComparer(Of DatasetFolderOrFileInfo)

        Public Function ItemsAreEqual(x As DatasetFolderOrFileInfo, y As DatasetFolderOrFileInfo) As Boolean Implements IEqualityComparer(Of DatasetFolderOrFileInfo).Equals
            Return x.FileID = y.FileID
        End Function

        Public Function GetHashCodeForItem(obj As DatasetFolderOrFileInfo) As Integer Implements IEqualityComparer(Of DatasetFolderOrFileInfo).GetHashCode
            Return obj.FileID.GetHashCode()
        End Function
    End Class

End Class
