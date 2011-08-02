'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2007, Battelle Memorial Institute
' Created 12/19/2007
'
' Last modified 06/11/2009 JDS - Added logging using log4net
'*********************************************************************************************************

Option Strict On

Imports AnalysisManagerBase.clsGlobal

Namespace AnalysisManagerBase

	Public Class clsAnalysisResults

		'*********************************************************************************************************
		'Analysis job results handling class
		'*********************************************************************************************************

#Region "Module variables"
        Private Const FAILED_RESULTS_FOLDER_INFO_TEXT As String = "FailedResultsFolderInfo_"
        Private Const FAILED_RESULTS_FOLDER_RETAIN_DAYS As Integer = 31

        Private Const DEFAULT_RETRY_COUNT As Integer = 3
        Private Const DEFAULT_RETRY_HOLDOFF_SEC As Integer = 15

        ' access to the job parameters
        Private m_jobParams As IJobParams

        ' access to mgr parameters
        Private m_mgrParams As IMgrParams

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
        Public Sub New(ByVal mgrParams As IMgrParams, ByVal jobParams As IJobParams)
            m_mgrParams = mgrParams
            m_jobParams = jobParams
        End Sub


        '' <summary>
        '' Copies the results folder to the transfer directory
        '' Note: This sub is no longer used by the analysis tool manager
        '' </summary>
        '' <param name="ResultsFolderName">Name of results folder</param>
        '' <returns>CloseOutType enum indicating success or failure</returns>
        '' <remarks></remarks>
        'Public Function DeliverResults(ByVal ResultsFolderName As String) As IJobParams.CloseOutType

        '    Dim XferDir As String = m_jobParams.GetParam("transferFolderPath")
        '    Dim WorkDir As String = m_mgrParams.GetParam("workdir")
        '    Dim ResultsFolderPath As String

        '    ResultsFolderPath = System.IO.Path.Combine(WorkDir, ResultsFolderName)

        '    'Verify the results folder exists
        '    If Not System.IO.Directory.Exists(ResultsFolderPath) Then
        '        m_logger.PostEntry("Results folder not found, job " & m_jobParams.GetParam("Job"), _
        '            ILogger.logMsgType.logError, LOG_DATABASE)
        '        m_message = "Results folder not found"
        '        Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        '    End If

        '    'Verify xfer directory exists
        '    If Not System.IO.Directory.Exists(XferDir) Then
        '        m_logger.PostEntry("Transfer folder not found, job " & m_jobParams.GetParam("Job"), _
        '            ILogger.logMsgType.logError, LOG_DATABASE)
        '        m_message = "Transfer folder not found"
        '        CopyFailedResultsToArchiveFolder(ResultsFolderPath)
        '        Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        '    End If

        '    'Append machine name to xfer directory, if specified, and create directory if it doesn't exist
        '    Dim blnAppendManagerName As Boolean = True
        '    blnAppendManagerName = CBoolSafe(m_mgrParams.GetParam("AppendMgrNameToXferFolder"), True)

        '    If blnAppendManagerName Then
        '        XferDir = System.IO.Path.Combine(XferDir, m_mgrParams.GetParam("MgrName"))
        '    End If

        '    If Not System.IO.Directory.Exists(XferDir) Then
        '        Try
        '            System.IO.Directory.CreateDirectory(XferDir)
        '        Catch err As Exception
        '            m_logger.PostError("Unable to create server results folder, job " & m_jobParams.GetParam("Job"), _
        '                    err, LOG_DATABASE)
        '            m_message = "Unable to create server results folder"
        '            CopyFailedResultsToArchiveFolder(ResultsFolderPath)
        '            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        '        End Try
        '    End If

        '    'Copy results folder to xfer directory
        '    Try
        '        CopyDirectory(ResultsFolderPath, System.IO.Path.Combine(XferDir, ResultsFolderName), True, DEFAULT_RETRY_COUNT, False)
        '    Catch err As Exception
        '        m_logger.PostError("Error copying results folder, job " & m_jobParams.GetParam("Job"), err, LOG_DATABASE)
        '        m_message = "Unable to create server results folder"
        '        CopyFailedResultsToArchiveFolder(ResultsFolderPath)
        '        Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        '    End Try

        '    'Everything must be OK if we got to here
        '    Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

        'End Function

        ''' <summary>Copies a source directory to the destination directory. Allows overwriting.</summary>
        ''' <remarks>The last parameter specifies whether the files already present in the
        ''' destination directory will be overwritten
        ''' - Note: requires Imports System.IO
        ''' - Usage: CopyDirectory("C:\Misc", "D:\MiscBackup")
        '''
        ''' Original code obtained from vb2themax.com
        ''' </remarks>
        ''' <param name="SourcePath">The source directory path.</param>
        ''' <param name="DestPath">The destination directory path.</param>
        ''' <param name="Overwrite">true if the destination file can be overwritten; otherwise, false.</param>
        ''' <param name="MaxRetryCount">The number of times to retry a failed copy of a file; if 0 or 1 then only tries once</param>
        ''' <param name="ContinueOnError">When true, then will continue copying even if an error occurs</param>
        Public Sub CopyDirectory(ByVal SourcePath As String, ByVal DestPath As String, _
                                  ByVal Overwrite As Boolean, ByVal MaxRetryCount As Integer, _
                                  ByVal ContinueOnError As Boolean)

            Dim diSourceDir As System.IO.DirectoryInfo = New System.IO.DirectoryInfo(SourcePath)
            Dim diDestDir As System.IO.DirectoryInfo = New System.IO.DirectoryInfo(DestPath)

            Dim strTargetPath As String
            Dim strMessage As String

            ' the source directory must exist, otherwise throw an exception
            If Not FolderExistsWithRetry(diSourceDir.FullName, 3, 3) Then
                strMessage = "Source directory does not exist: " + diSourceDir.FullName
                If ContinueOnError Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, strMessage)
                    Exit Sub
                Else
                    Throw New System.IO.DirectoryNotFoundException(strMessage)
                End If
            Else
                ' if destination SubDir's parent SubDir does not exist throw an exception
                If Not FolderExistsWithRetry(diDestDir.Parent.FullName, 1, 1) Then
                    strMessage = "Destination directory does not exist: " + diDestDir.Parent.FullName
                    If ContinueOnError Then
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, strMessage)
                        Exit Sub
                    Else
                        Throw New System.IO.DirectoryNotFoundException(strMessage)
                    End If
                End If

                If Not FolderExistsWithRetry(diDestDir.FullName, 3, 3) Then
                    CreateFolderWithRetry(DestPath, MaxRetryCount, DEFAULT_RETRY_HOLDOFF_SEC)
                End If

                ' copy all the files of the current directory
                Dim ChildFile As System.IO.FileInfo
                For Each ChildFile In diSourceDir.GetFiles()
                    Try
                        strTargetPath = System.IO.Path.Combine(diDestDir.FullName, ChildFile.Name)
                        If Overwrite Then
                            CopyFileWithRetry(ChildFile.FullName, strTargetPath, _
                                              True, MaxRetryCount, DEFAULT_RETRY_HOLDOFF_SEC)

                        Else
                            ' Only copy if the file does not yet exist
                            ' We are not throwing an error if the file exists in the target
                            If Not System.IO.File.Exists(strTargetPath) Then
                                CopyFileWithRetry(ChildFile.FullName, strTargetPath, _
                                                  False, MaxRetryCount, DEFAULT_RETRY_HOLDOFF_SEC)
                            End If
                        End If

                    Catch ex As Exception
                        If ContinueOnError Then
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisResults,CopyDirectory", ex)
                        Else
                            Throw ex
                        End If
                    End Try
                Next

                ' copy all the sub-directories by recursively calling this same routine
                Dim SubDir As System.IO.DirectoryInfo
                For Each SubDir In diSourceDir.GetDirectories()
                    CopyDirectory(SubDir.FullName, _
                                  System.IO.Path.Combine(diDestDir.FullName, SubDir.Name), _
                                  Overwrite, MaxRetryCount, ContinueOnError)
                Next
            End If

        End Sub

        Public Sub CopyFileWithRetry(ByVal SrcFilePath As String, ByVal DestFilePath As String, ByVal Overwrite As Boolean)
            Dim blnIncreaseHoldoffOnEachRetry As Boolean = False
            CopyFileWithRetry(SrcFilePath, DestFilePath, Overwrite, DEFAULT_RETRY_COUNT, DEFAULT_RETRY_HOLDOFF_SEC, blnIncreaseHoldoffOnEachRetry)
        End Sub

        Public Sub CopyFileWithRetry(ByVal SrcFilePath As String, ByVal DestFilePath As String, _
                                     ByVal Overwrite As Boolean, ByVal blnIncreaseHoldoffOnEachRetry As Boolean)
            CopyFileWithRetry(SrcFilePath, DestFilePath, Overwrite, DEFAULT_RETRY_COUNT, DEFAULT_RETRY_HOLDOFF_SEC, blnIncreaseHoldoffOnEachRetry)
        End Sub

        Public Sub CopyFileWithRetry(ByVal SrcFilePath As String, ByVal DestFilePath As String, ByVal Overwrite As Boolean, _
                                     ByVal MaxRetryCount As Integer, ByVal RetryHoldoffSeconds As Integer)
            Dim blnIncreaseHoldoffOnEachRetry As Boolean = False
            CopyFileWithRetry(SrcFilePath, DestFilePath, Overwrite, MaxRetryCount, RetryHoldoffSeconds, blnIncreaseHoldoffOnEachRetry)
        End Sub

        Public Sub CopyFileWithRetry(ByVal SrcFilePath As String, ByVal DestFilePath As String, ByVal Overwrite As Boolean, _
                                     ByVal MaxRetryCount As Integer, ByVal RetryHoldoffSeconds As Integer, _
                                     ByVal blnIncreaseHoldoffOnEachRetry As Boolean)

            Dim AttemptCount As Integer = 0
            Dim blnSuccess As Boolean = False
            Dim sngRetryHoldoffSeconds As Single = RetryHoldoffSeconds

            If sngRetryHoldoffSeconds < 1 Then sngRetryHoldoffSeconds = 1
            If MaxRetryCount < 1 Then MaxRetryCount = 1

            ' First make sure the source file exists
            If Not System.IO.File.Exists(SrcFilePath) Then
                Throw New System.IO.IOException("clsAnalysisResults,CopyFileWithRetry: Source file not found for copy operation: " & SrcFilePath)
            End If

            Do While AttemptCount <= MaxRetryCount And Not blnSuccess
                AttemptCount += 1

                Try
                    System.IO.File.Copy(SrcFilePath, DestFilePath, Overwrite)
                    blnSuccess = True

                Catch ex As Exception
                    Dim ErrMsg As String = "clsAnalysisResults,CopyFileWithRetry: error copying " & SrcFilePath & " to " & DestFilePath & ": " & ex.Message
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, ErrMsg)

                    If Not Overwrite AndAlso System.IO.File.Exists(DestFilePath) Then
                        Throw New System.IO.IOException("Tried to overwrite an existing file when Overwrite = False: " & DestFilePath)
                    End If

                    System.Threading.Thread.Sleep(CInt(Math.Floor(sngRetryHoldoffSeconds * 1000)))    'Wait several seconds before retrying

                    GC.Collect()
                    GC.WaitForPendingFinalizers()
                End Try

                If Not blnSuccess AndAlso blnIncreaseHoldoffOnEachRetry Then
                    sngRetryHoldoffSeconds *= 1.5!
                End If
            Loop

            If Not blnSuccess Then
                Throw New System.IO.IOException("Excessive failures during file copy")
            End If

        End Sub

        Public Sub CopyFailedResultsToArchiveFolder(ByVal ResultsFolderPath As String)

            Dim diSourceFolder As System.IO.DirectoryInfo
            Dim diTargetFolder As System.IO.DirectoryInfo

            Dim strFailedResultsFolderPath As String = String.Empty

            Dim strFolderInfoFilePath As String = String.Empty

            Try
                strFailedResultsFolderPath = m_mgrParams.GetParam("FailedResultsFolderPath")

                If strFailedResultsFolderPath Is Nothing OrElse strFailedResultsFolderPath.Length = 0 Then
                    ' Failed results folder path is not defined; don't try to copy the results anywhere
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "FailedResultsFolderPath is not defined for this manager; cannot copy results")
                    Exit Sub
                End If

                ' Make sure the target folder exists
                CreateFolderWithRetry(strFailedResultsFolderPath, 2, 5)

                diSourceFolder = New System.IO.DirectoryInfo(ResultsFolderPath)
                diTargetFolder = New System.IO.DirectoryInfo(strFailedResultsFolderPath)

                ' Create an info file that describes the saved results
                Try
                    strFolderInfoFilePath = System.IO.Path.Combine(diTargetFolder.FullName, FAILED_RESULTS_FOLDER_INFO_TEXT & diSourceFolder.Name & ".txt")
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
                    strTargetFolderPath = System.IO.Path.Combine(diTargetFolder.FullName, diSourceFolder.Name)

                    ' Actually copy the results folder
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Copying results folder to failed results archive: " & strTargetFolderPath)

                    CopyDirectory(diSourceFolder.FullName, strTargetFolderPath, True, 2, True)

                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Copy complete")
                End If

            Catch ex As Exception
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error copying results from " & ResultsFolderPath & " to " & strFailedResultsFolderPath, ex)
            End Try

        End Sub

        Private Sub CopyFailedResultsCreateInfoFile(ByVal strFolderInfoFilePath As String, ByVal strResultsFolderName As String)

            Dim swArchivedFolderInfoFile As System.IO.StreamWriter
            swArchivedFolderInfoFile = New System.IO.StreamWriter(New System.IO.FileStream(strFolderInfoFilePath, IO.FileMode.Create, IO.FileAccess.Write, IO.FileShare.Read))

            With swArchivedFolderInfoFile
                .WriteLine("Date" & ControlChars.Tab & System.DateTime.Now())
                .WriteLine("ResultsFolderName" & ControlChars.Tab & strResultsFolderName)
                .WriteLine("Manager" & ControlChars.Tab & m_mgrParams.GetParam("MgrName"))
                If Not m_jobParams Is Nothing Then
                    .WriteLine("JobToolDescription" & ControlChars.Tab & m_jobParams.GetCurrentJobToolDescription())
                    .WriteLine("Job" & ControlChars.Tab & m_jobParams.GetParam("Job"))
                    .WriteLine("Step" & ControlChars.Tab & m_jobParams.GetParam("Step"))
                End If
                .WriteLine("Date" & ControlChars.Tab & System.DateTime.Now().ToString)
                If Not m_jobParams Is Nothing Then
                    .WriteLine("Tool" & ControlChars.Tab & m_jobParams.GetParam("toolname"))
                    .WriteLine("StepTool" & ControlChars.Tab & m_jobParams.GetParam("StepTool"))
                    .WriteLine("Dataset" & ControlChars.Tab & m_jobParams.GetParam("datasetNum"))
                    .WriteLine("XferFolder" & ControlChars.Tab & m_jobParams.GetParam("transferFolderPath"))
                    .WriteLine("ParamFileName" & ControlChars.Tab & m_jobParams.GetParam("parmFileName"))
                    .WriteLine("SettingsFileName" & ControlChars.Tab & m_jobParams.GetParam("settingsFileName"))
                    .WriteLine("LegacyOrganismDBName" & ControlChars.Tab & m_jobParams.GetParam("LegacyFastaFileName"))
                    .WriteLine("ProteinCollectionList" & ControlChars.Tab & m_jobParams.GetParam("ProteinCollectionList"))
                    .WriteLine("ProteinOptionsList" & ControlChars.Tab & m_jobParams.GetParam("ProteinOptions"))
                    .WriteLine("FastaFileName" & ControlChars.Tab & m_jobParams.GetParam("generatedFastaName"))
                End If
            End With

            swArchivedFolderInfoFile.Close()

        End Sub

        Public Sub CreateFolderWithRetry(ByVal FolderPath As String)
            Dim blnIncreaseHoldoffOnEachRetry As Boolean = False
            CreateFolderWithRetry(FolderPath, DEFAULT_RETRY_COUNT, DEFAULT_RETRY_HOLDOFF_SEC, blnIncreaseHoldoffOnEachRetry)
        End Sub

        Public Sub CreateFolderWithRetry(ByVal FolderPath As String, _
                                         ByVal MaxRetryCount As Integer, ByVal RetryHoldoffSeconds As Integer)
            Dim blnIncreaseHoldoffOnEachRetry As Boolean = False
            CreateFolderWithRetry(FolderPath, MaxRetryCount, RetryHoldoffSeconds, blnIncreaseHoldoffOnEachRetry)
        End Sub

        Public Sub CreateFolderWithRetry(ByVal FolderPath As String, _
                                         ByVal MaxRetryCount As Integer, ByVal RetryHoldoffSeconds As Integer, _
                                         ByVal blnIncreaseHoldoffOnEachRetry As Boolean)

            Dim AttemptCount As Integer = 0
            Dim blnSuccess As Boolean = False
            Dim sngRetryHoldoffSeconds As Single = RetryHoldoffSeconds

            If sngRetryHoldoffSeconds < 1 Then sngRetryHoldoffSeconds = 1
            If MaxRetryCount < 1 Then MaxRetryCount = 1

            Do While AttemptCount <= MaxRetryCount And Not blnSuccess
                AttemptCount += 1

                Try
                    If System.IO.Directory.Exists(FolderPath) Then
                        ' If the folder already exists, then there is nothing to do
                        blnSuccess = True
                    Else
                        System.IO.Directory.CreateDirectory(FolderPath)
                        blnSuccess = True
                    End If

                Catch ex As Exception
                    Dim ErrMsg As String = "clsAnalysisResults: error creating folder " & FolderPath
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, ErrMsg, ex)

                    System.Threading.Thread.Sleep(CInt(Math.Floor(sngRetryHoldoffSeconds * 1000)))    'Wait several seconds before retrying

                    GC.Collect()
                    GC.WaitForPendingFinalizers()
                End Try

                If Not blnSuccess AndAlso blnIncreaseHoldoffOnEachRetry Then
                    sngRetryHoldoffSeconds *= 1.5!
                End If
            Loop

            If Not blnSuccess Then
                If Not FolderExistsWithRetry(FolderPath, 1, 3) Then
                    Throw New System.IO.IOException("Excessive failures during folder creation")
                End If
            End If

        End Sub

        Private Sub DeleteOldFailedResultsFolders(ByVal diTargetFolder As System.IO.DirectoryInfo)

            Dim fiFileInfo As System.IO.FileInfo
            Dim diOldResultsFolder As System.IO.DirectoryInfo

            Dim strOldResultsFolderName As String
            Dim strTargetFilePath As String = ""

            ' Determine the folder archive time by reading the modification times on the ResultsFolderInfo_ files
            For Each fiFileInfo In diTargetFolder.GetFileSystemInfos(FAILED_RESULTS_FOLDER_INFO_TEXT & "*")
                If System.DateTime.Now.Subtract(fiFileInfo.LastWriteTime).TotalDays > FAILED_RESULTS_FOLDER_RETAIN_DAYS Then
                    ' File was modified before the threshold; delete the results folder, then rename this file

                    Try
                        strOldResultsFolderName = System.IO.Path.GetFileNameWithoutExtension(fiFileInfo.Name).Substring(FAILED_RESULTS_FOLDER_INFO_TEXT.Length)
                        diOldResultsFolder = New System.IO.DirectoryInfo(System.IO.Path.Combine(fiFileInfo.DirectoryName, strOldResultsFolderName))

                        If diOldResultsFolder.Exists Then
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Deleting old failed results folder: " & diOldResultsFolder.FullName)

                            diOldResultsFolder.Delete(True)
                        End If

                        Try
                            strTargetFilePath = System.IO.Path.Combine(fiFileInfo.DirectoryName, "x_" & fiFileInfo.Name)
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

        Public Function FolderExistsWithRetry(ByVal FolderPath As String) As Boolean
            Dim blnIncreaseHoldoffOnEachRetry As Boolean = False
            Return FolderExistsWithRetry(FolderPath, DEFAULT_RETRY_COUNT, DEFAULT_RETRY_HOLDOFF_SEC, blnIncreaseHoldoffOnEachRetry)
        End Function

        Public Function FolderExistsWithRetry(ByVal FolderPath As String, _
                                         ByVal MaxRetryCount As Integer, ByVal RetryHoldoffSeconds As Integer) As Boolean
            Dim blnIncreaseHoldoffOnEachRetry As Boolean = False
            Return FolderExistsWithRetry(FolderPath, MaxRetryCount, RetryHoldoffSeconds, blnIncreaseHoldoffOnEachRetry)
        End Function

        Public Function FolderExistsWithRetry(ByVal FolderPath As String, _
                                         ByVal MaxRetryCount As Integer, ByVal RetryHoldoffSeconds As Integer, _
                                         ByVal blnIncreaseHoldoffOnEachRetry As Boolean) As Boolean

            Dim AttemptCount As Integer = 0
            Dim blnSuccess As Boolean = False
            Dim blnFolderExists As Boolean = False

            Dim sngRetryHoldoffSeconds As Single = RetryHoldoffSeconds

            If sngRetryHoldoffSeconds < 1 Then sngRetryHoldoffSeconds = 1
            If MaxRetryCount < 1 Then MaxRetryCount = 1

            Do While AttemptCount <= MaxRetryCount And Not blnSuccess
                AttemptCount += 1

                Try
                    blnFolderExists = System.IO.Directory.Exists(FolderPath)
                    blnSuccess = True

                Catch ex As Exception
                    Dim ErrMsg As String = "clsAnalysisResults: error looking for folder " & FolderPath
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, ErrMsg, ex)

                    System.Threading.Thread.Sleep(CInt(Math.Floor(sngRetryHoldoffSeconds * 1000)))    'Wait several seconds before retrying

                    GC.Collect()
                    GC.WaitForPendingFinalizers()
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

End Namespace
