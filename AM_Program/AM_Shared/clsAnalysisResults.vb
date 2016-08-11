'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2007, Battelle Memorial Institute
' Created 12/19/2007
'
'*********************************************************************************************************

Option Strict On

Imports System.IO
Imports System.Threading

Public Class clsAnalysisResults
    Inherits clsAnalysisMgrBase

    '*********************************************************************************************************
    'Analysis job results handling class
    '*********************************************************************************************************

#Region "Module variables"
    Private Const FAILED_RESULTS_FOLDER_INFO_TEXT As String = "FailedResultsFolderInfo_"
    Private Const FAILED_RESULTS_FOLDER_RETAIN_DAYS As Integer = 31

    Private Const DEFAULT_RETRY_COUNT As Integer = 3
    Private Const DEFAULT_RETRY_HOLDOFF_SEC As Integer = 15

    ' access to the job parameters
    Private ReadOnly m_jobParams As IJobParams

    ' access to mgr parameters
    Private ReadOnly m_mgrParams As IMgrParams
    Private ReadOnly m_MgrName As String

    ' for posting a general explanation for external consumption
    Protected m_message As String

#End Region

#Region "Properties"
    ' explanation of what happened to last operation this class performed
    Public ReadOnly Property Message() As String
        Get
            Return m_message
        End Get
    End Property
#End Region

#Region "Methods"
    ''' <summary>
    ''' Constructor
    ''' </summary>
    ''' <param name="mgrParams">Manager parameter object</param>
    ''' <param name="jobParams">Job parameter object</param>
    ''' <remarks></remarks>
    Public Sub New(mgrParams As IMgrParams, jobParams As IJobParams)

        MyBase.New("clsAnalysisResults")

        m_mgrParams = mgrParams
        m_jobParams = jobParams
        m_MgrName = m_mgrParams.GetParam("MgrName", "Undefined-Manager")
        m_DebugLevel = CShort(m_mgrParams.GetParam("debuglevel", 1))

        MyBase.InitFileTools(m_MgrName, m_DebugLevel)

    End Sub

    ''' <summary>
    ''' Copies a source directory to the destination directory. Allows overwriting.
    ''' </summary>
    ''' <param name="SourcePath">The source directory path.</param>
    ''' <param name="DestPath">The destination directory path.</param>
    ''' <param name="Overwrite">True if the destination file can be overwritten; otherwise, false.</param>
    ''' <remarks></remarks>
    Public Sub CopyDirectory(SourcePath As String, DestPath As String, Overwrite As Boolean)
        CopyDirectory(SourcePath, DestPath, Overwrite, MaxRetryCount:=DEFAULT_RETRY_COUNT, ContinueOnError:=True)
    End Sub

    ''' <summary>
    ''' Copies a source directory to the destination directory. Allows overwriting.
    ''' </summary>
    ''' <param name="SourcePath">The source directory path.</param>
    ''' <param name="DestPath">The destination directory path.</param>
    ''' <param name="Overwrite">True if the destination file can be overwritten; otherwise, false.</param>
    ''' <param name="MaxRetryCount">The number of times to retry a failed copy of a file; if 0 or 1 then only tries once</param>
    ''' <param name="ContinueOnError">When true, then will continue copying even if an error occurs</param>
    ''' <remarks></remarks>
    Public Sub CopyDirectory(
      SourcePath As String, DestPath As String,
      Overwrite As Boolean, MaxRetryCount As Integer,
      ContinueOnError As Boolean)

        Dim diSourceDir = New DirectoryInfo(SourcePath)
        Dim diDestDir = New DirectoryInfo(DestPath)

        Dim strTargetPath As String
        Dim strMessage As String

        ' The source directory must exist, otherwise throw an exception
        If Not FolderExistsWithRetry(diSourceDir.FullName, 3, 3) Then
            strMessage = "Source directory does not exist: " + diSourceDir.FullName
            If ContinueOnError Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, strMessage)
                Exit Sub
            Else
                Throw New DirectoryNotFoundException(strMessage)
            End If
        Else
            ' If destination SubDir's parent SubDir does not exist throw an exception
            If Not FolderExistsWithRetry(diDestDir.Parent.FullName, 1, 1) Then
                strMessage = "Destination directory does not exist: " + diDestDir.Parent.FullName
                If ContinueOnError Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, strMessage)
                    Exit Sub
                Else
                    Throw New DirectoryNotFoundException(strMessage)
                End If
            End If

            If Not FolderExistsWithRetry(diDestDir.FullName, 3, 3) Then
                CreateFolderWithRetry(DestPath, MaxRetryCount, DEFAULT_RETRY_HOLDOFF_SEC)
            End If

            ' Copy all the files of the current directory
            Dim ChildFile As FileInfo
            For Each ChildFile In diSourceDir.GetFiles()
                Try
                    strTargetPath = Path.Combine(diDestDir.FullName, ChildFile.Name)
                    If Overwrite Then
                        CopyFileWithRetry(ChildFile.FullName, strTargetPath, True, MaxRetryCount, DEFAULT_RETRY_HOLDOFF_SEC)

                    Else
                        ' Only copy if the file does not yet exist
                        ' We are not throwing an error if the file exists in the target
                        If Not File.Exists(strTargetPath) Then
                            CopyFileWithRetry(ChildFile.FullName, strTargetPath, False, MaxRetryCount, DEFAULT_RETRY_HOLDOFF_SEC)
                        End If
                    End If

                Catch ex As Exception
                    If ContinueOnError Then
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisResults,CopyDirectory", ex)
                    Else
                        Throw
                    End If
                End Try
            Next

            ' Copy all the sub-directories by recursively calling this same routine
            Dim SubDir As DirectoryInfo
            For Each SubDir In diSourceDir.GetDirectories()
                CopyDirectory(SubDir.FullName, Path.Combine(diDestDir.FullName, SubDir.Name), Overwrite, MaxRetryCount, ContinueOnError)
            Next
        End If

    End Sub

    Public Sub CopyFileWithRetry(SrcFilePath As String, DestFilePath As String, Overwrite As Boolean)
        Const blnIncreaseHoldoffOnEachRetry = False
        CopyFileWithRetry(SrcFilePath, DestFilePath, Overwrite, DEFAULT_RETRY_COUNT, DEFAULT_RETRY_HOLDOFF_SEC, blnIncreaseHoldoffOnEachRetry)
    End Sub

    Public Sub CopyFileWithRetry(
      SrcFilePath As String, DestFilePath As String,
      Overwrite As Boolean, blnIncreaseHoldoffOnEachRetry As Boolean)
        CopyFileWithRetry(SrcFilePath, DestFilePath, Overwrite, DEFAULT_RETRY_COUNT, DEFAULT_RETRY_HOLDOFF_SEC, blnIncreaseHoldoffOnEachRetry)
    End Sub

    Public Sub CopyFileWithRetry(
      SrcFilePath As String, DestFilePath As String, Overwrite As Boolean,
      MaxRetryCount As Integer, RetryHoldoffSeconds As Integer)
        Const blnIncreaseHoldoffOnEachRetry = False
        CopyFileWithRetry(SrcFilePath, DestFilePath, Overwrite, MaxRetryCount, RetryHoldoffSeconds, blnIncreaseHoldoffOnEachRetry)
    End Sub

    Public Sub CopyFileWithRetry(
      SrcFilePath As String, DestFilePath As String, Overwrite As Boolean,
      MaxRetryCount As Integer, RetryHoldoffSeconds As Integer,
      blnIncreaseHoldoffOnEachRetry As Boolean)

        Dim AttemptCount = 0
        Dim blnSuccess = False
        Dim sngRetryHoldoffSeconds As Single = RetryHoldoffSeconds

        If sngRetryHoldoffSeconds < 1 Then sngRetryHoldoffSeconds = 1
        If MaxRetryCount < 1 Then MaxRetryCount = 1

        ' First make sure the source file exists
        If Not File.Exists(SrcFilePath) Then
            Throw New IOException("clsAnalysisResults,CopyFileWithRetry: Source file not found for copy operation: " & SrcFilePath)
        End If

        Do While AttemptCount <= MaxRetryCount And Not blnSuccess
            AttemptCount += 1

            Try
                ResetTimestampForQueueWaitTimeLogging()
                If m_FileTools.CopyFileUsingLocks(SrcFilePath, DestFilePath, m_MgrName, Overwrite) Then
                    blnSuccess = True
                Else
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "CopyFileUsingLocks returned false copying " & SrcFilePath & " to " & DestFilePath)
                End If

            Catch ex As Exception
                Dim ErrMsg As String = "clsAnalysisResults,CopyFileWithRetry: error copying " & SrcFilePath & " to " & DestFilePath & ": " & ex.Message
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, ErrMsg)

                If Not Overwrite AndAlso File.Exists(DestFilePath) Then
                    Throw New IOException("Tried to overwrite an existing file when Overwrite = False: " & DestFilePath)
                End If

                Thread.Sleep(CInt(Math.Floor(sngRetryHoldoffSeconds * 1000)))     'Wait several seconds before retrying

                PRISM.Processes.clsProgRunner.GarbageCollectNow()
            End Try

            If Not blnSuccess AndAlso blnIncreaseHoldoffOnEachRetry Then
                sngRetryHoldoffSeconds *= 1.5!
            End If
        Loop

        If Not blnSuccess Then
            Throw New IOException("Excessive failures during file copy")
        End If

    End Sub

    Public Sub CopyFailedResultsToArchiveFolder(ResultsFolderPath As String)

        Dim diSourceFolder As DirectoryInfo
        Dim diTargetFolder As DirectoryInfo

        Dim strFailedResultsFolderPath As String = String.Empty

        Dim strFolderInfoFilePath As String = String.Empty

        Try
            strFailedResultsFolderPath = m_mgrParams.GetParam("FailedResultsFolderPath")

            If String.IsNullOrEmpty(strFailedResultsFolderPath) Then
                ' Failed results folder path is not defined; don't try to copy the results anywhere
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "FailedResultsFolderPath is not defined for this manager; cannot copy results")
                Exit Sub
            End If

            ' Make sure the target folder exists
            CreateFolderWithRetry(strFailedResultsFolderPath, 2, 5)

            diSourceFolder = New DirectoryInfo(ResultsFolderPath)
            diTargetFolder = New DirectoryInfo(strFailedResultsFolderPath)

            ' Create an info file that describes the saved results
            Try
                strFolderInfoFilePath = Path.Combine(diTargetFolder.FullName, FAILED_RESULTS_FOLDER_INFO_TEXT & diSourceFolder.Name & ".txt")
                CopyFailedResultsCreateInfoFile(strFolderInfoFilePath, diSourceFolder.Name)
            Catch ex As Exception
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error creating the results folder info file '" & strFolderInfoFilePath, ex)
            End Try

            ' Make sure the source folder exists
            If Not diSourceFolder.Exists Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Results folder not found; cannot copy results: " & ResultsFolderPath)
            Else
                ' Look for failed results folders that were archived over FAILED_RESULTS_FOLDER_RETAIN_DAYS days ago
                DeleteOldFailedResultsFolders(diTargetFolder)

                Dim strTargetFolderPath As String
                strTargetFolderPath = Path.Combine(diTargetFolder.FullName, diSourceFolder.Name)

                ' Actually copy the results folder
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Copying results folder to failed results archive: " & strTargetFolderPath)

                CopyDirectory(diSourceFolder.FullName, strTargetFolderPath, True, 2, True)

                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Copy complete")
            End If

        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error copying results from " & ResultsFolderPath & " to " & strFailedResultsFolderPath, ex)
        End Try

    End Sub

    Private Sub CopyFailedResultsCreateInfoFile(strFolderInfoFilePath As String, strResultsFolderName As String)

        Dim swArchivedFolderInfoFile As StreamWriter
        swArchivedFolderInfoFile = New StreamWriter(New FileStream(strFolderInfoFilePath, FileMode.Create, FileAccess.Write, FileShare.Read))

        With swArchivedFolderInfoFile
            .WriteLine("Date" & ControlChars.Tab & DateTime.Now())
            .WriteLine("ResultsFolderName" & ControlChars.Tab & strResultsFolderName)
            .WriteLine("Manager" & ControlChars.Tab & m_mgrParams.GetParam("MgrName"))
            If Not m_jobParams Is Nothing Then
                .WriteLine("JobToolDescription" & ControlChars.Tab & m_jobParams.GetCurrentJobToolDescription())
                .WriteLine("Job" & ControlChars.Tab & m_jobParams.GetParam("StepParameters", "Job"))
                .WriteLine("Step" & ControlChars.Tab & m_jobParams.GetParam("StepParameters", "Step"))
            End If
            .WriteLine("Date" & ControlChars.Tab & DateTime.Now().ToString)
            If Not m_jobParams Is Nothing Then
                .WriteLine("Tool" & ControlChars.Tab & m_jobParams.GetParam("toolname"))
                .WriteLine("StepTool" & ControlChars.Tab & m_jobParams.GetParam("StepTool"))
                .WriteLine("Dataset" & ControlChars.Tab & m_jobParams.GetParam("JobParameters", "DatasetNum"))
                .WriteLine("XferFolder" & ControlChars.Tab & m_jobParams.GetParam("transferFolderPath"))
                .WriteLine("ParamFileName" & ControlChars.Tab & m_jobParams.GetParam("parmFileName"))
                .WriteLine("SettingsFileName" & ControlChars.Tab & m_jobParams.GetParam("settingsFileName"))
                .WriteLine("LegacyOrganismDBName" & ControlChars.Tab & m_jobParams.GetParam("LegacyFastaFileName"))
                .WriteLine("ProteinCollectionList" & ControlChars.Tab & m_jobParams.GetParam("ProteinCollectionList"))
                .WriteLine("ProteinOptionsList" & ControlChars.Tab & m_jobParams.GetParam("ProteinOptions"))
                .WriteLine("FastaFileName" & ControlChars.Tab & m_jobParams.GetParam("PeptideSearch", "generatedFastaName"))
            End If
        End With

        swArchivedFolderInfoFile.Close()

    End Sub

    Public Sub CreateFolderWithRetry(FolderPath As String)
        Const blnIncreaseHoldoffOnEachRetry = False
        CreateFolderWithRetry(FolderPath, DEFAULT_RETRY_COUNT, DEFAULT_RETRY_HOLDOFF_SEC, blnIncreaseHoldoffOnEachRetry)
    End Sub

    Public Sub CreateFolderWithRetry(
      FolderPath As String,
      MaxRetryCount As Integer,
      RetryHoldoffSeconds As Integer)
        Const blnIncreaseHoldoffOnEachRetry = False
        CreateFolderWithRetry(FolderPath, MaxRetryCount, RetryHoldoffSeconds, blnIncreaseHoldoffOnEachRetry)
    End Sub

    Public Sub CreateFolderWithRetry(
      FolderPath As String,
      MaxRetryCount As Integer,
      RetryHoldoffSeconds As Integer,
      blnIncreaseHoldoffOnEachRetry As Boolean)

        Dim AttemptCount = 0
        Dim blnSuccess = False
        Dim sngRetryHoldoffSeconds As Single = RetryHoldoffSeconds

        If sngRetryHoldoffSeconds < 1 Then sngRetryHoldoffSeconds = 1
        If MaxRetryCount < 1 Then MaxRetryCount = 1

        If String.IsNullOrWhiteSpace(FolderPath) Then
            Throw New DirectoryNotFoundException("Folder path cannot be empty when calling CreateFolderWithRetry")
        End If

        Do While AttemptCount <= MaxRetryCount And Not blnSuccess
            AttemptCount += 1

            Try
                If Directory.Exists(FolderPath) Then
                    ' If the folder already exists, then there is nothing to do
                    blnSuccess = True
                Else
                    Directory.CreateDirectory(FolderPath)
                    blnSuccess = True
                End If

            Catch ex As Exception
                Dim ErrMsg As String = "clsAnalysisResults: error creating folder " & FolderPath
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, ErrMsg, ex)

                Thread.Sleep(CInt(Math.Floor(sngRetryHoldoffSeconds * 1000)))     'Wait several seconds before retrying

                PRISM.Processes.clsProgRunner.GarbageCollectNow()
            End Try

            If Not blnSuccess AndAlso blnIncreaseHoldoffOnEachRetry Then
                sngRetryHoldoffSeconds *= 1.5!
            End If
        Loop

        If Not blnSuccess Then
            If Not FolderExistsWithRetry(FolderPath, 1, 3) Then
                Throw New IOException("Excessive failures during folder creation")
            End If
        End If

    End Sub

    Private Sub DeleteOldFailedResultsFolders(diTargetFolder As DirectoryInfo)

        Dim fiFileInfo As FileInfo
        Dim diOldResultsFolder As DirectoryInfo

        Dim strOldResultsFolderName As String
        Dim strTargetFilePath = ""

        ' Determine the folder archive time by reading the modification times on the ResultsFolderInfo_ files
        For Each fiFileInfo In diTargetFolder.GetFileSystemInfos(FAILED_RESULTS_FOLDER_INFO_TEXT & "*")
            If DateTime.UtcNow.Subtract(fiFileInfo.LastWriteTimeUtc).TotalDays > FAILED_RESULTS_FOLDER_RETAIN_DAYS Then
                ' File was modified before the threshold; delete the results folder, then rename this file

                Try
                    strOldResultsFolderName = Path.GetFileNameWithoutExtension(fiFileInfo.Name).Substring(FAILED_RESULTS_FOLDER_INFO_TEXT.Length)
                    diOldResultsFolder = New DirectoryInfo(Path.Combine(fiFileInfo.DirectoryName, strOldResultsFolderName))

                    If diOldResultsFolder.Exists Then
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Deleting old failed results folder: " & diOldResultsFolder.FullName)

                        diOldResultsFolder.Delete(True)
                    End If

                    Try
                        strTargetFilePath = Path.Combine(fiFileInfo.DirectoryName, "x_" & fiFileInfo.Name)
                        fiFileInfo.CopyTo(strTargetFilePath, True)
                        fiFileInfo.Delete()
                    Catch ex As Exception
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error renaming failed results info file to " & strTargetFilePath, ex)
                    End Try

                Catch ex As Exception
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error deleting old failed results folder", ex)

                End Try

            End If
        Next

    End Sub

    Public Function FolderExistsWithRetry(FolderPath As String) As Boolean
        Const blnIncreaseHoldoffOnEachRetry = False
        Return FolderExistsWithRetry(FolderPath, DEFAULT_RETRY_COUNT, DEFAULT_RETRY_HOLDOFF_SEC, blnIncreaseHoldoffOnEachRetry)
    End Function

    Public Function FolderExistsWithRetry(
      FolderPath As String,
      MaxRetryCount As Integer,
      RetryHoldoffSeconds As Integer) As Boolean
        Const blnIncreaseHoldoffOnEachRetry = False
        Return FolderExistsWithRetry(FolderPath, MaxRetryCount, RetryHoldoffSeconds, blnIncreaseHoldoffOnEachRetry)
    End Function

    Public Function FolderExistsWithRetry(
      FolderPath As String,
      MaxRetryCount As Integer,
      RetryHoldoffSeconds As Integer,
      blnIncreaseHoldoffOnEachRetry As Boolean) As Boolean

        Dim AttemptCount = 0
        Dim blnSuccess = False
        Dim blnFolderExists = False

        Dim sngRetryHoldoffSeconds As Single = RetryHoldoffSeconds

        If sngRetryHoldoffSeconds < 1 Then sngRetryHoldoffSeconds = 1
        If MaxRetryCount < 1 Then MaxRetryCount = 1

        Do While AttemptCount <= MaxRetryCount And Not blnSuccess
            AttemptCount += 1

            Try
                blnFolderExists = Directory.Exists(FolderPath)
                blnSuccess = True

            Catch ex As Exception
                Dim ErrMsg As String = "clsAnalysisResults: error looking for folder " & FolderPath
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, ErrMsg, ex)

                Thread.Sleep(CInt(Math.Floor(sngRetryHoldoffSeconds * 1000)))     'Wait several seconds before retrying

                PRISM.Processes.clsProgRunner.GarbageCollectNow()
            End Try

            If Not blnSuccess AndAlso blnIncreaseHoldoffOnEachRetry Then
                sngRetryHoldoffSeconds *= 1.5!
            End If

        Loop

        If Not blnSuccess Then
            ' Exception occurred; return False
            Return False
        End If

        Return blnFolderExists

    End Function

#End Region

End Class


