
Option Strict On

Public Class clsIonicZipTools

    Public Const IONIC_ZIP_NAME As String = "IonicZip (DotNetZip)"

    Protected m_DebugLevel As Integer
    Protected m_WorkDir As String = String.Empty

    Protected m_MostRecentZipFilePath As String = String.Empty
    Protected m_Message As String = String.Empty

#Region "Properties"

    Public Property DebugLevel() As Integer
        Get
            Return m_DebugLevel
        End Get
        Set(ByVal value As Integer)
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
#End Region

    Public Sub New(ByVal DebugLevel As Integer, ByVal WorkDir As String)
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

		GC.Collect()
		GC.WaitForPendingFinalizers()
		System.Threading.Thread.Sleep(100)

	End Sub

    Protected Sub ReportZipStats(ByVal fiFileSystemInfo As System.IO.FileSystemInfo, _
                                 ByVal dtStartTime As System.DateTime, _
                                 ByVal dtEndTime As System.DateTime, _
                                 ByVal FileWasZipped As Boolean)

        ReportZipStats(fiFileSystemInfo, dtStartTime, dtEndTime, FileWasZipped, IONIC_ZIP_NAME)

    End Sub

    Public Sub ReportZipStats(ByVal fiFileSystemInfo As System.IO.FileSystemInfo, _
                              ByVal dtStartTime As System.DateTime, _
                              ByVal dtEndTime As System.DateTime, _
                              ByVal FileWasZipped As Boolean, _
                              ByVal ZipProgramName As String)

        Dim dblUnzipTimeSeconds As Double
        Dim dblUnzipSpeedMBPerSec As Double

        Dim lngTotalSizeBytes As System.Int64

        Dim strZipAction As String

        If ZipProgramName Is Nothing Then ZipProgramName = "??"

        dblUnzipTimeSeconds = dtEndTime.Subtract(dtStartTime).TotalSeconds

        If TypeOf (fiFileSystemInfo) Is System.IO.FileInfo Then
            lngTotalSizeBytes = CType(fiFileSystemInfo, System.IO.FileInfo).Length

        ElseIf TypeOf (fiFileSystemInfo) Is System.IO.DirectoryInfo Then
            Dim diFolderInfo As System.IO.DirectoryInfo
            diFolderInfo = CType(fiFileSystemInfo, System.IO.DirectoryInfo)

            lngTotalSizeBytes = 0
            For Each fiEntry As System.IO.FileInfo In diFolderInfo.GetFiles("*.*", IO.SearchOption.AllDirectories)
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
    Public Function UnzipFile(ByVal ZipFilePath As String) As Boolean
        Return UnzipFile(ZipFilePath, m_WorkDir)
    End Function

    ''' <summary>
    ''' Unzip ZipFilePath into the specified target directory
    ''' Existing files will be overwritten
    ''' </summary>
    ''' <param name="ZipFilePath">File to unzip</param>
    ''' <param name="TargetDirectory">Folder to place the unzipped files</param>
    ''' <returns>True if success; false if an error</returns>
    Public Function UnzipFile(ByVal ZipFilePath As String, ByVal TargetDirectory As String) As Boolean
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
    Public Function UnzipFile(ByVal ZipFilePath As String, _
                              ByVal TargetDirectory As String, _
                              ByVal FileFilter As String) As Boolean

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
    Public Function UnzipFile(ByVal ZipFilePath As String, _
                              ByVal TargetDirectory As String, _
                              ByVal FileFilter As String, _
                              ByVal eOverwriteBehavior As Ionic.Zip.ExtractExistingFileAction) As Boolean

        Dim dtStartTime As System.DateTime
        Dim dtEndTime As System.DateTime

        Dim objZipper As Ionic.Zip.ZipFile

        Dim fiFile As System.IO.FileInfo

        m_Message = String.Empty
        m_MostRecentZipFilePath = String.Copy(ZipFilePath)

        Try
            fiFile = New System.IO.FileInfo(ZipFilePath)

            If Not System.IO.File.Exists(ZipFilePath) Then
                m_Message = "Zip file not found: " & fiFile.FullName
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_Message)
                Return False
            End If

            If m_DebugLevel >= 3 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Unzipping file: " & fiFile.FullName)
            End If
            objZipper = New Ionic.Zip.ZipFile(ZipFilePath)

            dtStartTime = System.DateTime.UtcNow

            If String.IsNullOrEmpty(FileFilter) Then
                objZipper.ExtractAll(TargetDirectory, eOverwriteBehavior)
            Else
                Dim objEntries As System.Collections.Generic.ICollection(Of Ionic.Zip.ZipEntry)
                objEntries = objZipper.SelectEntries(FileFilter)

                For Each objItem As Ionic.Zip.ZipEntry In objEntries
                    objItem.Extract(TargetDirectory, eOverwriteBehavior)
                Next
            End If

            dtEndTime = System.DateTime.UtcNow

            ReportZipStats(fiFile, dtStartTime, dtEndTime, False)

            ' Dispose of the zipper and call the garbage collector to assure the handle to the .zip file is released
            DisposeZipper(objZipper)

        Catch ex As Exception
            m_Message = "Error unzipping " & ZipFilePath & ": " & ex.Message
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_Message)
            Return False
        End Try

        Return True

    End Function

    ''' <summary>
    ''' Stores SourceFilePath in a zip file with the same name, but extension .zip
    ''' </summary>
    ''' <param name="SourceFilePath">Full path to the file to be zipped</param>
    ''' <param name="DeleteSourceAfterZip">If True, then will delete the source file after zipping it</param>
    ''' <returns>True if success; false if an error</returns>
    Public Function ZipFile(ByVal SourceFilePath As String, ByVal DeleteSourceAfterZip As Boolean) As Boolean
        Dim ZipFilePath As String
        Dim fiFile As System.IO.FileInfo

        fiFile = New System.IO.FileInfo(SourceFilePath)

        ZipFilePath = System.IO.Path.Combine(fiFile.DirectoryName, System.IO.Path.GetFileNameWithoutExtension(fiFile.Name) & ".zip")

        Return ZipFile(SourceFilePath, DeleteSourceAfterZip, ZipFilePath)

    End Function

    ''' <summary>
    ''' Stores SourceFilePath in a zip file named ZipFilePath
    ''' </summary>
    ''' <param name="SourceFilePath">Full path to the file to be zipped</param>
    ''' <param name="DeleteSourceAfterZip">If True, then will delete the source file after zipping it</param>
    ''' <param name="ZipFilePath">Full path to the .zip file to be created.  Existing files will be overwritten</param>
    ''' <returns>True if success; false if an error</returns>
    Public Function ZipFile(ByVal SourceFilePath As String, _
                            ByVal DeleteSourceAfterZip As Boolean, _
                            ByVal ZipFilePath As String) As Boolean

        Dim dtStartTime As System.DateTime
        Dim dtEndTime As System.DateTime

        Dim objZipper As Ionic.Zip.ZipFile

        Dim fiFile As System.IO.FileInfo
        fiFile = New System.IO.FileInfo(SourceFilePath)

        m_Message = String.Empty
        m_MostRecentZipFilePath = String.Copy(ZipFilePath)

        Try
            If System.IO.File.Exists(ZipFilePath) Then

                If m_DebugLevel >= 3 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Deleting target .zip file: " & ZipFilePath)
                End If

                System.IO.File.Delete(ZipFilePath)
                System.Threading.Thread.Sleep(500)

            End If
        Catch ex As Exception
            m_Message = "Error deleting target .zip file prior to zipping " & SourceFilePath & ": " & ex.Message
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_Message)
            Return False
        End Try

        Try
            If m_DebugLevel >= 3 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Creating .zip file: " & ZipFilePath)
            End If

            objZipper = New Ionic.Zip.ZipFile(ZipFilePath)
            objZipper.UseZip64WhenSaving = Ionic.Zip.Zip64Option.AsNecessary

            dtStartTime = System.DateTime.UtcNow
            objZipper.AddItem(fiFile.FullName, String.Empty)
            objZipper.Save()
            dtEndTime = System.DateTime.UtcNow

            ReportZipStats(fiFile, dtStartTime, dtEndTime, True)

            ' Dispose of the zipper and call the garbage collector to assure the handle to the .zip file is released
            DisposeZipper(objZipper)

        Catch ex As Exception
            m_Message = "Error zipping file " & fiFile.FullName & ": " & ex.Message
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_Message)
            Return False
        End Try

        Try

            If DeleteSourceAfterZip Then
                If m_DebugLevel >= 3 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Deleting source file: " & fiFile.FullName)
                End If

                ' Now delete the source file
                fiFile.Delete()

                ' Wait 500 msec
                System.Threading.Thread.Sleep(500)
            End If

        Catch ex As Exception
            ' Log this as an error, but still return True
            m_Message = "Error deleting " & fiFile.FullName & ": " & ex.Message
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_Message)
        End Try

        Return True

    End Function

    Public Function ZipDirectory(ByVal SourceDirectoryPath As String, _
                                 ByVal ZipFilePath As String) As Boolean

        Return ZipDirectory(SourceDirectoryPath, ZipFilePath, True, String.Empty)

    End Function

    Public Function ZipDirectory(ByVal SourceDirectoryPath As String, _
                             ByVal ZipFilePath As String, _
                             ByVal Recurse As Boolean) As Boolean

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
    Public Function ZipDirectory(ByVal SourceDirectoryPath As String, _
                                 ByVal ZipFilePath As String, _
                                 ByVal Recurse As Boolean, _
                                 ByVal FileFilter As String) As Boolean

        Dim dtStartTime As System.DateTime
        Dim dtEndTime As System.DateTime

        Dim objZipper As Ionic.Zip.ZipFile

        Dim diDirectory As System.IO.DirectoryInfo
        diDirectory = New System.IO.DirectoryInfo(SourceDirectoryPath)

        m_Message = String.Empty
        m_MostRecentZipFilePath = String.Copy(ZipFilePath)

        Try
            If System.IO.File.Exists(ZipFilePath) Then
                If m_DebugLevel >= 3 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Deleting target .zip file: " & ZipFilePath)
                End If

                System.IO.File.Delete(ZipFilePath)
                System.Threading.Thread.Sleep(500)
            End If
        Catch ex As Exception
            m_Message = "Error deleting target .zip file prior to zipping " & SourceDirectoryPath & ": " & ex.Message
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_Message)
            Return False
        End Try

        Try
            If m_DebugLevel >= 3 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Creating .zip file: " & ZipFilePath)
            End If

            objZipper = New Ionic.Zip.ZipFile(ZipFilePath)
            objZipper.UseZip64WhenSaving = Ionic.Zip.Zip64Option.AsNecessary

            dtStartTime = System.DateTime.UtcNow

            If String.IsNullOrEmpty(FileFilter) AndAlso Recurse Then
                objZipper.AddDirectory(diDirectory.FullName)
            Else
                If String.IsNullOrEmpty(FileFilter) Then
                    FileFilter = "*"
                End If

                objZipper.AddSelectedFiles(FileFilter, diDirectory.FullName, String.Empty, Recurse)
            End If

            objZipper.Save()

            dtEndTime = System.DateTime.UtcNow

            ReportZipStats(diDirectory, dtStartTime, dtEndTime, True)

            ' Dispose of the zipper and call the garbage collector to assure the handle to the .zip file is released
            DisposeZipper(objZipper)

        Catch ex As Exception
            m_Message = "Error zipping directory " & diDirectory.FullName & ": " & ex.Message
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_Message)
            Return False
        End Try

        Return True

    End Function
End Class

