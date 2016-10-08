Option Strict On

Imports System.IO
Imports System.Threading

Public Class clsIonicZipTools

    Public Const IONIC_ZIP_NAME As String = "IonicZip (DotNetZip)"

    Protected m_DebugLevel As Integer
    Protected m_WorkDir As String = String.Empty

    Protected m_MostRecentZipFilePath As String = String.Empty

    ' This variable tracks the files most recently unzipped
    ' Keys in the KeyValuePairs are filenames while values are relative paths (in case the .zip file has folders)
    Protected m_MostRecentUnzippedFiles As List(Of KeyValuePair(Of String, String)) = New List(Of KeyValuePair(Of String, String))

    Protected m_Message As String = String.Empty

#Region "Properties"

    Public Property DebugLevel() As Integer
        Get
            Return m_DebugLevel
        End Get
        Set(value As Integer)
            m_DebugLevel = value
        End Set
    End Property

    Public ReadOnly Property Message() As String
        Get
            Return m_Message
        End Get
    End Property

    Public ReadOnly Property MostRecentZipFilePath() As String
        Get
            Return m_MostRecentZipFilePath
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
#End Region

    Public Sub New(DebugLevel As Integer, WorkDir As String)
        m_DebugLevel = DebugLevel
        m_WorkDir = WorkDir
    End Sub

    ''' <summary>
    ''' Dispose of the zipper and call the garbage collector to assure the handle to the .zip file is released
    ''' </summary>
    ''' <param name="objZipper"></param>
    ''' <remarks></remarks>
    Protected Sub DisposeZipper(ByRef objZipper As Ionic.Zip.ZipFile)

        objZipper = Nothing

        PRISM.Processes.clsProgRunner.GarbageCollectNow()
        Thread.Sleep(100)

    End Sub

    Protected Sub DeleteFolder(diFolder As DirectoryInfo)
        Try

            If m_DebugLevel >= 3 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Deleting folder: " & diFolder.FullName)
            End If

            diFolder.Refresh()

            If diFolder.Exists() Then
                ' Now delete the source file
                diFolder.Delete(True)
            End If

            ' Wait 100 msec
            Thread.Sleep(100)

        Catch ex As Exception
            ' Log this as an error, but don't treat this as fatal
            m_Message = "Error deleting " & diFolder.FullName & ": " & ex.Message
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_Message)
        End Try

    End Sub

    Protected Sub DeleteFile(fiFile As FileInfo)
        Try

            If m_DebugLevel >= 3 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Deleting source file: " & fiFile.FullName)
            End If

            fiFile.Refresh()

            If fiFile.Exists() Then
                ' Now delete the source file
                fiFile.Delete()
            End If


            ' Wait 250 msec
            Thread.Sleep(250)

        Catch ex As Exception
            ' Log this as an error, but don't treat this as fatal
            m_Message = "Error deleting " & fiFile.FullName & ": " & ex.Message
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_Message)
        End Try

    End Sub

    ''' <summary>
    ''' Gets the .zip file path to create when zipping a single file
    ''' </summary>
    ''' <param name="sourceFilePath"></param>
    ''' <returns></returns>
    Public Shared Function GetZipFilePathForFile(sourceFilePath As String) As String
        Dim fiFile = New FileInfo(sourceFilePath)
        Return Path.Combine(fiFile.DirectoryName, Path.GetFileNameWithoutExtension(fiFile.Name) & ".zip")
    End Function

    ''' <summary>
    ''' Unzip GZipFilePath into the working directory defined when this class was instantiated
    ''' Existing files will be overwritten
    ''' </summary>
    ''' <param name="GZipFilePath">.gz file to unzip</param>
    ''' <returns>True if success; false if an error</returns>
    Public Function GUnzipFile(GZipFilePath As String) As Boolean
        Return GUnzipFile(GZipFilePath, m_WorkDir)
    End Function

    ''' <summary>
    ''' Unzip GZipFilePath into the specified target directory
    ''' Existing files will be overwritten
    ''' </summary>
    ''' <param name="GZipFilePath">.gz file to unzip</param>
    ''' <param name="TargetDirectory">Folder to place the unzipped files</param>
    ''' <returns>True if success; false if an error</returns>
    Public Function GUnzipFile(GZipFilePath As String, TargetDirectory As String) As Boolean
        Return GUnzipFile(GZipFilePath, TargetDirectory, Ionic.Zip.ExtractExistingFileAction.OverwriteSilently)
    End Function


    ''' <summary>
    ''' Unzip GZipFilePath into the specified target directory, applying the specified file filter
    ''' </summary>
    ''' <param name="GZipFilePath">.gz file to unzip</param>
    ''' <param name="TargetDirectory">Folder to place the unzipped files</param>
    ''' <param name="eOverwriteBehavior">Defines what to do when existing files could be ovewritten</param>
    ''' <returns>True if success; false if an error</returns>
    Public Function GUnzipFile(
      GZipFilePath As String,
      TargetDirectory As String,
      eOverwriteBehavior As Ionic.Zip.ExtractExistingFileAction) As Boolean

        Dim dtStartTime As DateTime
        Dim dtEndTime As DateTime

        m_Message = String.Empty
        m_MostRecentZipFilePath = String.Copy(GZipFilePath)
        m_MostRecentUnzippedFiles.Clear()

        Try
            Dim fiFile = New FileInfo(GZipFilePath)

            If Not fiFile.Exists Then
                m_Message = "GZip file not found: " & fiFile.FullName
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_Message)
                Return False
            End If

            If fiFile.Extension.ToLower() <> ".gz" Then
                m_Message = "Not a GZipped file; must have extension .gz: " & fiFile.FullName
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_Message)
                Return False
            End If

            If m_DebugLevel >= 3 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Unzipping file: " & fiFile.FullName)
            End If

            dtStartTime = Date.UtcNow

            ' Get original file extension, for example "doc" from report.doc.gz
            Dim curFile As String = fiFile.Name
            Dim decompressedFilePath = Path.Combine(TargetDirectory, curFile.Remove(curFile.Length - fiFile.Extension.Length))

            Dim fiDecompressedFile = New FileInfo(decompressedFilePath)

            If fiDecompressedFile.Exists Then
                If eOverwriteBehavior = Ionic.Zip.ExtractExistingFileAction.DoNotOverwrite Then
                    m_Message = "Decompressed file already exists; will not overwrite: " & fiDecompressedFile.FullName
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, m_Message)
                    Return True
                ElseIf eOverwriteBehavior = Ionic.Zip.ExtractExistingFileAction.Throw Then
                    Throw New Exception("Decompressed file already exists: " & fiDecompressedFile.FullName)
                End If
            Else
                ' Make sure the target directory exists
                fiDecompressedFile.Directory.Create()
            End If

            Using inFile As FileStream = fiFile.OpenRead()

                ' Create the decompressed file.
                Using outFile As FileStream = File.Create(decompressedFilePath)
                    Using Decompress = New Ionic.Zlib.GZipStream(inFile, Ionic.Zlib.CompressionMode.Decompress)

                        ' Copy the decompression stream into the output file.
                        Decompress.CopyTo(outFile)
                        m_MostRecentUnzippedFiles.Add(New KeyValuePair(Of String, String)(fiDecompressedFile.Name, fiDecompressedFile.FullName))
                    End Using
                End Using
            End Using

            dtEndTime = Date.UtcNow

            If m_DebugLevel >= 2 Then
                ReportZipStats(fiFile, dtStartTime, dtEndTime, False)
            End If

            ' Update the file modification time of the decompressed file
            fiDecompressedFile.Refresh()
            If fiDecompressedFile.LastWriteTimeUtc > fiFile.LastWriteTimeUtc Then
                fiDecompressedFile.LastWriteTimeUtc = fiFile.LastWriteTimeUtc
            End If

            ' Call the garbage collector to assure the handle to the .gz file is released
            PRISM.Processes.clsProgRunner.GarbageCollectNow()

        Catch ex As Exception
            m_Message = "Error unzipping .gz file " & GZipFilePath & ": " & ex.Message
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_Message)
            Return False
        End Try

        Return True

    End Function

    ''' <summary>
    ''' Stores SourceFilePath in a zip file with the same name, but with extension .gz appended to the name (e.g. Dataset.mzid.gz)
    ''' </summary>
    ''' <param name="SourceFilePath">Full path to the file to be zipped</param>
    ''' <param name="DeleteSourceAfterZip">If True, then will delete the source file after zipping it</param>
    ''' <returns>True if success; false if an error</returns>
    Public Function GZipFile(SourceFilePath As String, DeleteSourceAfterZip As Boolean) As Boolean
        Dim fiFile = New FileInfo(SourceFilePath)
        Return GZipFile(SourceFilePath, fiFile.Directory.FullName, DeleteSourceAfterZip)
    End Function

    ''' <summary>
    ''' Stores SourceFilePath in a zip file with the same name, but with extension .gz appended to the name (e.g. Dataset.mzid.gz)
    ''' </summary>
    ''' <param name="SourceFilePath">Full path to the file to be zipped</param>
    ''' <param name="TargetFolderPath">Target directory to create the .gz file</param>
    ''' <param name="DeleteSourceAfterZip">If True, then will delete the source file after zipping it</param>
    ''' <returns>True if success; false if an error</returns>
    ''' <remarks>Preferably uses the external gzip.exe software, since that software properly stores the original filename and date in the .gz file</remarks>
    Public Function GZipFile(SourceFilePath As String, TargetFolderPath As String, DeleteSourceAfterZip As Boolean) As Boolean

        Dim fiFile = New FileInfo(SourceFilePath)

        Dim GZipFilePath = Path.Combine(TargetFolderPath, fiFile.Name & ".gz")

        m_Message = String.Empty
        m_MostRecentZipFilePath = String.Copy(GZipFilePath)

        Try
            If File.Exists(GZipFilePath) Then

                If m_DebugLevel >= 3 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Deleting target .gz file: " & GZipFilePath)
                End If

                File.Delete(GZipFilePath)
                Thread.Sleep(250)

            End If
        Catch ex As Exception
            m_Message = "Error deleting target .gz file prior to zipping " & SourceFilePath & ": " & ex.Message
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_Message)
            Return False
        End Try

        ' Look for gzip.exe
        Dim fiGZip = New FileInfo(Path.Combine(clsGlobal.GetAppFolderPath(), "gzip.exe"))
        Dim success As Boolean

        If fiGZip.Exists Then
            success = GZipUsingExe(fiFile, GZipFilePath, fiGZip)
        Else
            success = GZipUsingIonicZip(fiFile, GZipFilePath)
        End If

        If Not success Then
            Return False
        End If

        ' Call the garbage collector to assure the handle to the .gz file is released
        PRISM.Processes.clsProgRunner.GarbageCollectNow()

        If DeleteSourceAfterZip Then
            DeleteFile(fiFile)
        End If

        Return True

    End Function

    ''' <summary>
    ''' Compress a file using the external GZip.exe software
    ''' </summary>
    ''' <param name="fiFile">File to compress</param>
    ''' <param name="GZipFilePath">Full path to the .gz file to be created</param>
    ''' <param name="fiGZip">GZip.exe fileinfo object</param>
    ''' <returns></returns>
    ''' <remarks>The .gz file will initially be created in the same folder as the original file.  If GZipFilePath points to a different folder, then the file will be moved to that new location</remarks>
    Private Function GZipUsingExe(fiFile As FileInfo, GZipFilePath As String, fiGZip As FileInfo) As Boolean

        Dim dtStartTime As Date
        Dim dtEndTime As Date

        Try
            If m_DebugLevel >= 3 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Creating .gz file using " & fiGZip.Name & ": " & GZipFilePath)
            End If

            dtStartTime = Date.UtcNow

            Dim blnSuccess As Boolean

            Dim strArgs = "-f -k " & clsGlobal.PossiblyQuotePath(fiFile.FullName)

            If m_DebugLevel >= 3 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, fiGZip.FullName & " " & strArgs)
            End If

            Dim objProgRunner = New clsRunDosProgram(clsGlobal.GetAppFolderPath()) With {
                .CacheStandardOutput = False,
                .CreateNoWindow = True,
                .EchoOutputToConsole = True,
                .WriteConsoleOutputToFile = False,
                .DebugLevel = 1,
                .MonitorInterval = 250
            }

            blnSuccess = objProgRunner.RunProgram(fiGZip.FullName, strArgs, "GZip", False)

            If Not blnSuccess Then
                m_Message = "GZip.exe reported an error code"
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_Message)
                Return False
            End If

            Thread.Sleep(100)

            dtEndTime = Date.UtcNow

            ' Confirm that the .gz file was created

            Dim fiCompressedFile = New FileInfo(fiFile.FullName & ".gz")
            If Not fiCompressedFile.Exists Then
                m_Message = "GZip.exe did not create a .gz file: " & fiCompressedFile.FullName
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_Message)
                Return False
            End If

            If m_DebugLevel >= 2 Then
                ReportZipStats(fiFile, dtStartTime, dtEndTime, True)
            End If

            Dim fiCompressedFileFinal = New FileInfo(GZipFilePath)

            If Not clsGlobal.IsMatch(fiCompressedFile.FullName, fiCompressedFileFinal.FullName) Then

                If fiCompressedFileFinal.Exists Then
                    fiCompressedFileFinal.Delete()
                Else
                    fiCompressedFileFinal.Directory.Create()
                End If

                fiCompressedFile.MoveTo(fiCompressedFileFinal.FullName)
            End If

        Catch ex As Exception
            m_Message = "Error gzipping file " & fiFile.FullName & " using gzip.exe: " & ex.Message
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_Message)
            Return False
        End Try

        Return True

    End Function

    ''' <summary>
    ''' Compress the file using IonicZip
    ''' </summary>
    ''' <param name="fiFile"></param>
    ''' <param name="GZipFilePath"></param>
    ''' <returns></returns>
    ''' <remarks>IonicZip creates a valid .gz file, but it does not include the header information (filename and timestamp of the original file)</remarks>
    Private Function GZipUsingIonicZip(fiFile As FileInfo, GZipFilePath As String) As Boolean
        Dim dtStartTime As Date
        Dim dtEndTime As Date

        Try
            If m_DebugLevel >= 3 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Creating .gz file using IonicZip: " & GZipFilePath)
            End If

            dtStartTime = Date.UtcNow

            Using inFile As Stream = fiFile.OpenRead()
                Using outFile = File.Create(GZipFilePath)
                    Using gzippedStream = New Ionic.Zlib.GZipStream(outFile, Ionic.Zlib.CompressionMode.Compress)

                        inFile.CopyTo(gzippedStream)

                    End Using
                End Using
            End Using

            dtEndTime = Date.UtcNow

            If m_DebugLevel >= 2 Then
                ReportZipStats(fiFile, dtStartTime, dtEndTime, True)
            End If

            ' Update the file modification time of the .gz file to use the modification time of the original file
            Dim fiGZippedFile = New FileInfo(GZipFilePath)

            If Not fiGZippedFile.Exists Then
                m_Message = "IonicZip did not create a .gz file: " & GZipFilePath
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_Message)
                Return False
            End If

            fiGZippedFile.LastWriteTimeUtc = fiFile.LastWriteTimeUtc

        Catch ex As Exception
            m_Message = "Error gzipping file " & fiFile.FullName & ": " & ex.Message
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_Message)
            Return False
        End Try

        Return True

    End Function

    Protected Sub ReportZipStats(
      fiFileSystemInfo As FileSystemInfo,
      dtStartTime As DateTime,
      dtEndTime As DateTime,
      FileWasZipped As Boolean)

        ReportZipStats(fiFileSystemInfo, dtStartTime, dtEndTime, FileWasZipped, IONIC_ZIP_NAME)

    End Sub

    Public Sub ReportZipStats(
      fiFileSystemInfo As FileSystemInfo,
      dtStartTime As DateTime,
      dtEndTime As DateTime,
      FileWasZipped As Boolean,
      ZipProgramName As String)

        Dim dblUnzipTimeSeconds As Double
        Dim dblUnzipSpeedMBPerSec As Double

        Dim lngTotalSizeBytes As Int64

        Dim strZipAction As String

        If ZipProgramName Is Nothing Then ZipProgramName = "??"

        dblUnzipTimeSeconds = dtEndTime.Subtract(dtStartTime).TotalSeconds

        If TypeOf (fiFileSystemInfo) Is FileInfo Then
            lngTotalSizeBytes = CType(fiFileSystemInfo, FileInfo).Length

        ElseIf TypeOf (fiFileSystemInfo) Is DirectoryInfo Then
            Dim diFolderInfo As DirectoryInfo
            diFolderInfo = CType(fiFileSystemInfo, DirectoryInfo)

            lngTotalSizeBytes = 0
            For Each fiEntry As FileInfo In diFolderInfo.GetFiles("*", SearchOption.AllDirectories)
                lngTotalSizeBytes += fiEntry.Length
            Next
        End If

        If dblUnzipTimeSeconds > 0 Then
            dblUnzipSpeedMBPerSec = (lngTotalSizeBytes / 1024.0 / 1024.0) / dblUnzipTimeSeconds
        Else
            dblUnzipSpeedMBPerSec = 0
        End If

        If FileWasZipped Then
            strZipAction = "Zipped "
        Else
            strZipAction = "Unzipped "
        End If

        m_Message = strZipAction & fiFileSystemInfo.Name & " using " & ZipProgramName & "; elapsed time = " & dblUnzipTimeSeconds.ToString("0.0") & " seconds; rate = " & dblUnzipSpeedMBPerSec.ToString("0.0") & " MB/sec"

        If m_DebugLevel >= 2 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, m_Message)
        End If

    End Sub

    ''' <summary>
    ''' Unzip ZipFilePath into the working directory defined when this class was instantiated
    ''' Existing files will be overwritten
    ''' </summary>
    ''' <param name="ZipFilePath">File to unzip</param>
    ''' <returns>True if success; false if an error</returns>
    Public Function UnzipFile(ZipFilePath As String) As Boolean
        Return UnzipFile(ZipFilePath, m_WorkDir)
    End Function

    ''' <summary>
    ''' Unzip ZipFilePath into the specified target directory
    ''' Existing files will be overwritten
    ''' </summary>
    ''' <param name="ZipFilePath">File to unzip</param>
    ''' <param name="TargetDirectory">Folder to place the unzipped files</param>
    ''' <returns>True if success; false if an error</returns>
    Public Function UnzipFile(ZipFilePath As String, TargetDirectory As String) As Boolean
        Return UnzipFile(ZipFilePath, TargetDirectory, String.Empty, Ionic.Zip.ExtractExistingFileAction.OverwriteSilently)
    End Function

    ''' <summary>
    ''' Unzip ZipFilePath into the specified target directory, applying the specified file filter
    ''' Existing files will be overwritten
    ''' </summary>
    ''' <param name="ZipFilePath">File to unzip</param>
    ''' <param name="TargetDirectory">Folder to place the unzipped files</param>
    ''' <param name="FileFilter">Filter to apply when unzipping</param>
    ''' <returns>True if success; false if an error</returns>
    Public Function UnzipFile(
      ZipFilePath As String,
      TargetDirectory As String,
      FileFilter As String) As Boolean

        Return UnzipFile(ZipFilePath, TargetDirectory, FileFilter, Ionic.Zip.ExtractExistingFileAction.OverwriteSilently)
    End Function


    ''' <summary>
    ''' Unzip ZipFilePath into the specified target directory, applying the specified file filter
    ''' </summary>
    ''' <param name="ZipFilePath">File to unzip</param>
    ''' <param name="TargetDirectory">Folder to place the unzipped files</param>
    ''' <param name="FileFilter">Filter to apply when unzipping</param>
    ''' <param name="eOverwriteBehavior">Defines what to do when existing files could be ovewritten</param>
    ''' <returns>True if success; false if an error</returns>
    Public Function UnzipFile(
      ZipFilePath As String,
      TargetDirectory As String,
      FileFilter As String,
      eOverwriteBehavior As Ionic.Zip.ExtractExistingFileAction) As Boolean

        Dim dtStartTime As DateTime
        Dim dtEndTime As DateTime

        m_Message = String.Empty
        m_MostRecentZipFilePath = String.Copy(ZipFilePath)
        m_MostRecentUnzippedFiles.Clear()

        Try
            Dim fiFile = New FileInfo(ZipFilePath)

            If Not File.Exists(ZipFilePath) Then
                m_Message = "Zip file not found: " & fiFile.FullName
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_Message)
                Return False
            End If

            If m_DebugLevel >= 3 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Unzipping file: " & fiFile.FullName)
            End If

            Using objZipper = New Ionic.Zip.ZipFile(ZipFilePath)

                dtStartTime = Date.UtcNow

                If String.IsNullOrEmpty(FileFilter) Then
                    objZipper.ExtractAll(TargetDirectory, eOverwriteBehavior)

                    For Each objItem As Ionic.Zip.ZipEntry In objZipper.Entries
                        If Not objItem.IsDirectory Then
                            ' Note that objItem.FileName contains the relative path of the file, for example "Filename.txt" or "Subfolder/Filename.txt"
                            Dim fiUnzippedItem = New FileInfo(Path.Combine(TargetDirectory, objItem.FileName.Replace("/"c, Path.DirectorySeparatorChar)))
                            m_MostRecentUnzippedFiles.Add(New KeyValuePair(Of String, String)(fiUnzippedItem.Name, fiUnzippedItem.FullName))
                        End If
                    Next
                Else
                    Dim objEntries As ICollection(Of Ionic.Zip.ZipEntry)
                    objEntries = objZipper.SelectEntries(FileFilter)

                    For Each objItem As Ionic.Zip.ZipEntry In objEntries
                        objItem.Extract(TargetDirectory, eOverwriteBehavior)
                        If Not objItem.IsDirectory Then
                            ' Note that objItem.FileName contains the relative path of the file, for example "Filename.txt" or "Subfolder/Filename.txt"
                            Dim fiUnzippedItem = New FileInfo(Path.Combine(TargetDirectory, objItem.FileName.Replace("/"c, Path.DirectorySeparatorChar)))
                            m_MostRecentUnzippedFiles.Add(New KeyValuePair(Of String, String)(fiUnzippedItem.Name, fiUnzippedItem.FullName))
                        End If
                    Next
                End If

                dtEndTime = Date.UtcNow

                If m_DebugLevel >= 2 Then
                    ReportZipStats(fiFile, dtStartTime, dtEndTime, False)
                End If

                ' Dispose of the zipper and call the garbage collector to assure the handle to the .zip file is released
                DisposeZipper(objZipper)
            End Using

        Catch ex As Exception
            m_Message = "Error unzipping file " & ZipFilePath & ": " & ex.Message
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_Message)
            Return False
        End Try

        Return True

    End Function

    ''' <summary>
    ''' Verifies that the zip file exists and is not corrupt
    ''' If the file size is less than 4 GB, then also performs a full CRC check of the data
    ''' </summary>
    ''' <param name="zipFilePath">Zip file to check</param>
    ''' <returns>True if a valid zip file, otherwise false</returns>	
    Public Function VerifyZipFile(zipFilePath As String) As Boolean
        Return VerifyZipFile(zipFilePath, crcCheckThresholdGB:=4)
    End Function

    ''' <summary>
    ''' Verifies that the zip file exists.  
    ''' If the file size is less than crcCheckThresholdGB, then also performs a full CRC check of the data
    ''' </summary>
    ''' <param name="zipFilePath">Zip file to check</param>
    ''' <param name="crcCheckThresholdGB">Threshold (in GB) below which a full CRC check should be performed</param>
    ''' <returns>True if a valid zip file, otherwise false</returns>
    Public Function VerifyZipFile(zipFilePath As String, crcCheckThresholdGB As Single) As Boolean

        Try
            ' Wait 150 msec
            Thread.Sleep(150)

            ' Confirm that the zip file was created
            Dim fiZipFile = New FileInfo(zipFilePath)
            If Not fiZipFile.Exists Then
                m_Message = "Zip file not found: " & zipFilePath
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_Message)
                Return False
            End If

            ' Perform a quick check of the zip file (simply iterates over the directory entries)
            Dim blnsuccess = Ionic.Zip.ZipFile.CheckZip(zipFilePath)

            If Not blnsuccess Then
                If String.IsNullOrEmpty(m_Message) Then
                    m_Message = "Zip quick check failed for " & zipFilePath
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_Message)
                End If
                Return False
            End If

            ' For zip files less than 4 GB in size, perform a full unzip test to confirm that the file is not corrupted
            Dim crcCheckThresholdBytes = CLng(crcCheckThresholdGB * 1024 * 1024 * 1024)

            If fiZipFile.Length < crcCheckThresholdBytes Then

                ' Unzip each zipped file to a byte buffer (no need to actually write to disk)

                Using objZipper = New Ionic.Zip.ZipFile(zipFilePath)

                    Dim objEntries As ICollection(Of Ionic.Zip.ZipEntry)
                    objEntries = objZipper.SelectEntries("*")

                    For Each objItem As Ionic.Zip.ZipEntry In objEntries

                        If Not objItem.IsDirectory Then

                            Dim bytBuffer = New Byte(8095) {}
                            Dim n As Integer
                            Dim totalBytesRead As Int64 = 0

                            Using srReader = objItem.OpenReader()

                                Do
                                    n = srReader.Read(bytBuffer, 0, bytBuffer.Length)
                                    totalBytesRead += n
                                Loop While (n > 0)

                                If (srReader.Crc <> objItem.Crc) Then
                                    m_Message = String.Format("Zip entry " & objItem.FileName & " failed the CRC Check in " & zipFilePath & " (0x{0:X8} != 0x{1:X8})", srReader.Crc, objItem.Crc)
                                    Return False
                                End If

                                If (totalBytesRead <> objItem.UncompressedSize) Then
                                    m_Message = String.Format("Unexpected number of bytes for entry " & objItem.FileName & " in " & zipFilePath & " ({0} != {1})", totalBytesRead, objItem.UncompressedSize)
                                    Return False
                                End If

                            End Using

                        End If

                    Next

                End Using

            End If

        Catch ex As Exception
            m_Message = "Error verifying zip file " & zipFilePath & ": " & ex.Message
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_Message)
            Return False
        End Try

        Return True

    End Function

    ''' <summary>
    ''' Stores SourceFilePath in a zip file with the same name, but extension .zip
    ''' </summary>
    ''' <param name="sourceFilePath">Full path to the file to be zipped</param>
    ''' <param name="deleteSourceAfterZip">If True, then will delete the source file after zipping it</param>
    ''' <returns>True if success; false if an error</returns>
    Public Function ZipFile(sourceFilePath As String, deleteSourceAfterZip As Boolean) As Boolean

        Dim zipFilePath = GetZipFilePathForFile(sourceFilePath)

        Return ZipFile(sourceFilePath, deleteSourceAfterZip, zipFilePath)

    End Function

    ''' <summary>
    ''' Stores SourceFilePath in a zip file named ZipFilePath
    ''' </summary>
    ''' <param name="SourceFilePath">Full path to the file to be zipped</param>
    ''' <param name="DeleteSourceAfterZip">If True, then will delete the source file after zipping it</param>
    ''' <param name="ZipFilePath">Full path to the .zip file to be created.  Existing files will be overwritten</param>
    ''' <returns>True if success; false if an error</returns>
    Public Function ZipFile(
      SourceFilePath As String,
      DeleteSourceAfterZip As Boolean,
      ZipFilePath As String) As Boolean

        Dim dtStartTime As DateTime
        Dim dtEndTime As DateTime

        Dim fiFile = New FileInfo(SourceFilePath)

        m_Message = String.Empty
        m_MostRecentZipFilePath = String.Copy(ZipFilePath)

        Try
            If File.Exists(ZipFilePath) Then

                If m_DebugLevel >= 3 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Deleting target .zip file: " & ZipFilePath)
                End If

                File.Delete(ZipFilePath)
                Thread.Sleep(250)

            End If
        Catch ex As Exception
            m_Message = "Error deleting target .zip file prior to zipping file " & SourceFilePath & " using IonicZip: " & ex.Message
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_Message)
            Return False
        End Try

        Try
            If m_DebugLevel >= 3 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Creating .zip file: " & ZipFilePath)
            End If

            Using objZipper = New Ionic.Zip.ZipFile(ZipFilePath)
                objZipper.UseZip64WhenSaving = Ionic.Zip.Zip64Option.AsNecessary

                dtStartTime = Date.UtcNow
                objZipper.AddItem(fiFile.FullName, String.Empty)
                objZipper.Save()
                dtEndTime = Date.UtcNow

                If m_DebugLevel >= 2 Then
                    ReportZipStats(fiFile, dtStartTime, dtEndTime, True)
                End If

                ' Dispose of the zipper and call the garbage collector to assure the handle to the .zip file is released
                DisposeZipper(objZipper)
            End Using

        Catch ex As Exception
            m_Message = "Error zipping file " & fiFile.FullName & ": " & ex.Message
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_Message)
            Return False
        End Try

        ' Verify that the zip file is not corrupt
        ' Files less than 4 GB get a full CRC check
        ' Large files get a quick check
        If Not VerifyZipFile(ZipFilePath) Then
            Return False
        End If

        If DeleteSourceAfterZip Then
            DeleteFile(fiFile)
        End If

        Return True

    End Function

    Public Function ZipDirectory(
      SourceDirectoryPath As String,
      ZipFilePath As String) As Boolean

        Return ZipDirectory(SourceDirectoryPath, ZipFilePath, True, String.Empty)

    End Function

    Public Function ZipDirectory(
      SourceDirectoryPath As String,
      ZipFilePath As String,
      Recurse As Boolean) As Boolean

        Return ZipDirectory(SourceDirectoryPath, ZipFilePath, Recurse, String.Empty)

    End Function


    ''' <summary>
    ''' Stores all files in a source directory into a zip file named ZipFilePath
    ''' </summary>
    ''' <param name="SourceDirectoryPath">Full path to the directory to be zipped</param>    
    ''' <param name="ZipFilePath">Full path to the .zip file to be created.  Existing files will be overwritten</param>
    ''' <param name="Recurse">If True, then recurse through all subfolders</param>
    ''' <param name="FileFilter">Filter to apply when zipping</param>
    ''' <returns>True if success; false if an error</returns>
    Public Function ZipDirectory(
      SourceDirectoryPath As String,
      ZipFilePath As String,
      Recurse As Boolean,
      FileFilter As String) As Boolean

        Dim dtStartTime As DateTime
        Dim dtEndTime As DateTime

        Dim diDirectory As DirectoryInfo
        diDirectory = New DirectoryInfo(SourceDirectoryPath)

        m_Message = String.Empty
        m_MostRecentZipFilePath = String.Copy(ZipFilePath)

        Try
            If File.Exists(ZipFilePath) Then
                If m_DebugLevel >= 3 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Deleting target .zip file: " & ZipFilePath)
                End If

                File.Delete(ZipFilePath)
                Thread.Sleep(250)
            End If
        Catch ex As Exception
            m_Message = "Error deleting target .zip file prior to zipping folder " & SourceDirectoryPath & " using IonicZip: " & ex.Message
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_Message)
            Return False
        End Try

        Try
            If m_DebugLevel >= 3 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Creating .zip file: " & ZipFilePath)
            End If

            Using objZipper = New Ionic.Zip.ZipFile(ZipFilePath)
                objZipper.UseZip64WhenSaving = Ionic.Zip.Zip64Option.AsNecessary

                dtStartTime = Date.UtcNow

                If String.IsNullOrEmpty(FileFilter) AndAlso Recurse Then
                    objZipper.AddDirectory(diDirectory.FullName)
                Else
                    If String.IsNullOrEmpty(FileFilter) Then
                        FileFilter = "*"
                    End If

                    objZipper.AddSelectedFiles(FileFilter, diDirectory.FullName, String.Empty, Recurse)
                End If

                objZipper.Save()

                dtEndTime = Date.UtcNow

                If m_DebugLevel >= 2 Then
                    ReportZipStats(diDirectory, dtStartTime, dtEndTime, True)
                End If

                ' Dispose of the zipper and call the garbage collector to assure the handle to the .zip file is released
                DisposeZipper(objZipper)

            End Using

        Catch ex As Exception
            m_Message = "Error zipping directory " & diDirectory.FullName & ": " & ex.Message
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_Message)
            Return False
        End Try

        ' Verify that the zip file is not corrupt
        ' Files less than 4 GB get a full CRC check
        ' Large files get a quick check
        If Not VerifyZipFile(ZipFilePath) Then
            Return False
        End If

        Return True

    End Function
End Class

