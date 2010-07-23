Option Strict On

Public Class clsSharpZipTools

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

    Protected Sub ReportZipStats(ByVal fiFileInfo As System.IO.FileInfo, _
                                ByVal dtStartTime As System.DateTime, _
                                ByVal dtEndTime As System.DateTime, _
                                ByVal FileWasZipped As Boolean)

        ReportZipStats(fiFileInfo, dtStartTime, dtEndTime, FileWasZipped, "SharpZipLib")

    End Sub

    Public Sub ReportZipStats(ByVal fiFileInfo As System.IO.FileInfo, _
                              ByVal dtStartTime As System.DateTime, _
                              ByVal dtEndTime As System.DateTime, _
                              ByVal FileWasZipped As Boolean, _
                              ByVal ZipProgramName As String)

        Dim dblUnzipTimeSeconds As Double
        Dim dblUnzipSpeedMBPerSec As Double
        Dim strZipAction As String

        If ZipProgramName Is Nothing Then ZipProgramName = "??"

        dblUnzipTimeSeconds = dtEndTime.Subtract(dtStartTime).TotalSeconds

        If dblUnzipTimeSeconds > 0 Then
            dblUnzipSpeedMBPerSec = (fiFileInfo.Length / 1024.0 / 1024.0) / dblUnzipTimeSeconds
        Else
            dblUnzipSpeedMBPerSec = 0
        End If

        If FileWasZipped Then
            strZipAction = "Zipped "
        Else
            strZipAction = "Unzipped "
        End If

        m_Message = strZipAction & fiFileInfo.Name & " using " & ZipProgramName & "; elapsed time = " & dblUnzipTimeSeconds.ToString("0.0") & " seconds; rate = " & dblUnzipSpeedMBPerSec.ToString("0.0") & " MB/sec"

        If m_DebugLevel >= 2 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, m_Message)
        End If

    End Sub

    ''' <summary>
    ''' Unzip ZipFilePath into the working directory defined when this class was instantiated
    ''' </summary>
    ''' <param name="ZipFilePath">File to unzip</param>
    ''' <returns>True if success; false if an error</returns>
    Public Function UnzipFile(ByVal ZipFilePath As String) As Boolean
        Return UnzipFile(ZipFilePath, m_WorkDir, String.Empty)
    End Function

    ''' <summary>
    ''' Unzip ZipFilePath into the working directory defined when this class was instantiated
    ''' </summary>
    ''' <param name="ZipFilePath">File to unzip</param>
    ''' <param name="TargetDirectory">Folder to place the unzipped files</param>
    ''' <returns>True if success; false if an error</returns>
    Public Function UnzipFile(ByVal ZipFilePath As String, ByVal TargetDirectory As String) As Boolean
        Return UnzipFile(ZipFilePath, TargetDirectory, String.Empty)
    End Function

    ''' <summary>
    ''' Unzip ZipFilePath into the working directory defined when this class was instantiated
    ''' </summary>
    ''' <param name="ZipFilePath">File to unzip</param>
    ''' <param name="TargetDirectory">Folder to place the unzipped files</param>
    ''' <param name="FileFilter">Filter to apply when unzipping</param>
    ''' <returns>True if success; false if an error</returns>
    Public Function UnzipFile(ByVal ZipFilePath As String, ByVal TargetDirectory As String, ByVal FileFilter As String) As Boolean

        Dim dtStartTime As System.DateTime
        Dim dtEndTime As System.DateTime

        Dim objZipper As ICSharpCode.SharpZipLib.Zip.FastZip

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
            objZipper = New ICSharpCode.SharpZipLib.Zip.FastZip

            dtStartTime = System.DateTime.Now
            objZipper.ExtractZip(ZipFilePath, TargetDirectory, FileFilter)
            dtEndTime = System.DateTime.Now

            ReportZipStats(fiFile, dtStartTime, dtEndTime, False)

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
    ''' <param name="DeleteSourceAfterZip">If True, then will delete the file after zipping it</param>
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
    ''' <param name="DeleteSourceAfterZip">If True, then will delete the file after zipping it</param>
    ''' <param name="ZipFilePath">Full path to the .zip file to be created.  Existing files will be overwritten</param>
    ''' <returns>True if success; false if an error</returns>
    Public Function ZipFile(ByVal SourceFilePath As String, _
                            ByVal DeleteSourceAfterZip As Boolean, _
                            ByVal ZipFilePath As String) As Boolean

        Dim dtStartTime As System.DateTime
        Dim dtEndTime As System.DateTime

        Dim objZipper As ICSharpCode.SharpZipLib.Zip.FastZip

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

            objZipper = New ICSharpCode.SharpZipLib.Zip.FastZip

            dtStartTime = System.DateTime.Now
            objZipper.CreateZip(ZipFilePath, fiFile.DirectoryName, False, fiFile.Name)
            dtEndTime = System.DateTime.Now

            ReportZipStats(fiFile, dtStartTime, dtEndTime, True)

        Catch ex As Exception
            m_Message = "Error zipping " & fiFile.FullName & ": " & ex.Message
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

End Class
