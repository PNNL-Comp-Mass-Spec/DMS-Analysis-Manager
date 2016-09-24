Option Strict On

Imports System.IO
Imports System.Text
Imports System.Text.RegularExpressions
Imports System.Threading
Imports AnalysisManagerBase

Public Class clsAnalysisToolRunnerLipidMapSearch
    Inherits clsAnalysisToolRunnerBase

    '*********************************************************************************************************
    'Class for running LipidMapSearch
    '*********************************************************************************************************

#Region "Module Variables"
    Private Const LIPID_MAPS_DB_FILENAME_PREFIX As String = "LipidMapsDB_"
    Private Const LIPID_MAPS_STALE_DB_AGE_DAYS As Integer = 5

    Private Const LIPID_TOOLS_RESULT_FILE_PREFIX As String = "LipidMap_"
    Private Const LIPID_TOOLS_CONSOLE_OUTPUT As String = "LipidTools_ConsoleOutput.txt"

    Private Const PROGRESS_PCT_UPDATING_LIPID_MAPS_DATABASE As Integer = 5
    Private Const PROGRESS_PCT_LIPID_TOOLS_STARTING As Integer = 10

    Private Const PROGRESS_PCT_LIPID_TOOLS_READING_DATABASE As Integer = 11
    Private Const PROGRESS_PCT_LIPID_TOOLS_READING_POSITIVE_DATA As Integer = 12
    Private Const PROGRESS_PCT_LIPID_TOOLS_READING_NEGATIVE_DATA As Integer = 13
    Private Const PROGRESS_PCT_LIPID_TOOLS_FINDING_POSITIVE_FEATURES As Integer = 15
    Private Const PROGRESS_PCT_LIPID_TOOLS_FINDING_NEGATIVE_FEATURES As Integer = 50
    Private Const PROGRESS_PCT_LIPID_TOOLS_ALIGNING_FEATURES As Integer = 90
    Private Const PROGRESS_PCT_LIPID_TOOLS_MATCHING_TO_DB As Integer = 92
    Private Const PROGRESS_PCT_LIPID_TOOLS_WRITING_RESULTS As Integer = 94
    Private Const PROGRESS_PCT_LIPID_TOOLS_WRITING_QC_DATA As Integer = 96

    Private Const PROGRESS_PCT_LIPID_TOOLS_COMPLETE As Integer = 98
    Private Const PROGRESS_PCT_COMPLETE As Integer = 99

    Private mConsoleOutputErrorMsg As String

    Private mLipidToolsProgLoc As String
    Private mConsoleOutputProgressMap As Dictionary(Of String, Integer)

    Private mLipidMapsDBFilename As String = String.Empty

    Private WithEvents CmdRunner As clsRunDosProgram
#End Region

#Region "Structures"
#End Region

#Region "Methods"
    ''' <summary>
    ''' Runs LipidMapSearch tool
    ''' </summary>
    ''' <returns>CloseOutType enum indicating success or failure</returns>
    ''' <remarks></remarks>
    Public Overrides Function RunTool() As IJobParams.CloseOutType

        Try
            'Call base class for initial setup
            If Not MyBase.RunTool = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            If m_DebugLevel > 4 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerLipidMapSearch.RunTool(): Enter")
            End If

            ' Determine the path to the LipidTools program
            mLipidToolsProgLoc = DetermineProgramLocation("LipidTools", "LipidToolsProgLoc", "LipidTools.exe")

            If String.IsNullOrWhiteSpace(mLipidToolsProgLoc) Then
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            ' Store the LipidTools version info in the database
            If Not StoreToolVersionInfo(mLipidToolsProgLoc) Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Aborting since StoreToolVersionInfo returned false")
                m_message = "Error determining LipidTools version"
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            ' Obtain the LipidMaps.txt database
            m_progress = PROGRESS_PCT_UPDATING_LIPID_MAPS_DATABASE

            If Not GetLipidMapsDatabase() Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Aborting since GetLipidMapsDatabase returned false")
                If String.IsNullOrEmpty(m_message) Then
                    m_message = "Error obtaining the LipidMaps database"
                End If
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            m_jobParams.AddResultFileToSkip(mLipidMapsDBFilename)               ' Don't keep the Lipid Maps Database since we keep the permanent copy on Gigasax

            mConsoleOutputErrorMsg = String.Empty

            ' The parameter file name specifies the values to pass to LipidTools.exe at the command line
            Dim strParameterFileName = m_jobParams.GetParam("parmFileName")
            Dim strParameterFilePath = Path.Combine(m_WorkDir, strParameterFileName)

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Running LipidTools")


            'Set up and execute a program runner to run LipidTools
            Dim cmdStr = " -db " & PossiblyQuotePath(Path.Combine(m_WorkDir, mLipidMapsDBFilename)) & " -NoDBUpdate"
            cmdStr &= " -rp " & PossiblyQuotePath(Path.Combine(m_WorkDir, m_Dataset & clsAnalysisResources.DOT_RAW_EXTENSION))   ' Positive-mode .Raw file

            Dim strFilePath = Path.Combine(m_WorkDir, m_Dataset & clsAnalysisResourcesLipidMapSearch.DECONTOOLS_PEAKS_FILE_SUFFIX)
            If File.Exists(strFilePath) Then
                cmdStr &= " -pp " & PossiblyQuotePath(strFilePath)                  ' Positive-mode peaks.txt file
            End If

            Dim strDataset2 = m_jobParams.GetParam("JobParameters", "SourceJob2Dataset")
            If Not String.IsNullOrEmpty(strDataset2) Then
                cmdStr &= " -rn " & PossiblyQuotePath(Path.Combine(m_WorkDir, strDataset2 & clsAnalysisResources.DOT_RAW_EXTENSION)) ' Negative-mode .Raw file

                strFilePath = Path.Combine(m_WorkDir, strDataset2 & clsAnalysisResourcesLipidMapSearch.DECONTOOLS_PEAKS_FILE_SUFFIX)
                If File.Exists(strFilePath) Then
                    cmdStr &= " -pn " & PossiblyQuotePath(strFilePath)                  ' Negative-mode peaks.txt file
                End If
            End If

            ' Append the remaining parameters
            cmdStr &= ParseLipidMapSearchParameterFile(strParameterFilePath)

            cmdStr &= " -o " & PossiblyQuotePath(Path.Combine(m_WorkDir, LIPID_TOOLS_RESULT_FILE_PREFIX))            ' Folder and prefix text for output files

            If m_DebugLevel >= 1 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, mLipidToolsProgLoc & cmdStr)
            End If

            CmdRunner = New clsRunDosProgram(m_WorkDir)

            With CmdRunner
                .CreateNoWindow = True
                .CacheStandardOutput = True
                .EchoOutputToConsole = True
                .WriteConsoleOutputToFile = True
                .ConsoleOutputFilePath = Path.Combine(m_WorkDir, LIPID_TOOLS_CONSOLE_OUTPUT)
            End With

            m_progress = PROGRESS_PCT_LIPID_TOOLS_STARTING

            Dim success = CmdRunner.RunProgram(mLipidToolsProgLoc, cmdStr, "LipidTools", True)
            Dim processingError = False

            If Not CmdRunner.WriteConsoleOutputToFile Then
                ' Write the console output to a text file
                Thread.Sleep(250)

                Dim swConsoleOutputfile As New StreamWriter(New FileStream(CmdRunner.ConsoleOutputFilePath, FileMode.Create, FileAccess.Write, FileShare.Read))
                swConsoleOutputfile.WriteLine(CmdRunner.CachedConsoleOutput)
                swConsoleOutputfile.Close()
            End If

            ' Parse the console output file one more time to check for errors
            Thread.Sleep(250)
            ParseConsoleOutputFile(CmdRunner.ConsoleOutputFilePath)

            If Not String.IsNullOrEmpty(mConsoleOutputErrorMsg) Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mConsoleOutputErrorMsg)
            End If

            ' Append a line to the console output file listing the name of the LipidMapsDB that we used
            Using swConsoleOutputFile = New StreamWriter(New FileStream(CmdRunner.ConsoleOutputFilePath, FileMode.Append, FileAccess.Write, FileShare.Read))
                swConsoleOutputFile.WriteLine("LipidMapsDB Name: " & mLipidMapsDBFilename)
                swConsoleOutputFile.WriteLine("LipidMapsDB Hash: " & clsGlobal.ComputeFileHashSha1(Path.Combine(m_WorkDir, mLipidMapsDBFilename)))
            End Using

            ' Update the evaluation message to include the lipid maps DB filename
            ' This message will appear in Evaluation_Message column of T_Job_Steps
            m_EvalMessage = String.Copy(mLipidMapsDBFilename)

            If Not success Then
                Dim msg = "Error running LipidTools"
                m_message = clsGlobal.AppendToComment(m_message, msg)

                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg & ", job " & m_JobNum)

                If CmdRunner.ExitCode <> 0 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "LipidTools returned a non-zero exit code: " & CmdRunner.ExitCode.ToString)
                Else
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Call to LipidTools failed (but exit code is 0)")
                End If

                processingError = True

            Else
                m_progress = PROGRESS_PCT_LIPID_TOOLS_COMPLETE
                m_StatusTools.UpdateAndWrite(m_progress)
                If m_DebugLevel >= 3 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "LipidTools Search Complete")
                End If
            End If

            m_progress = PROGRESS_PCT_COMPLETE

            'Stop the job timer
            m_StopTime = DateTime.UtcNow

            'Add the current job data to the summary file
            If Not UpdateSummaryFile() Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Error creating summary file, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step"))
            End If

            'Make sure objects are released
            Thread.Sleep(500)         ' 1 second delay
            PRISM.Processes.clsProgRunner.GarbageCollectNow()

            ' Zip up the text files that contain the data behind the plots
            ' In addition, rename file LipidMap_results.xlsx
            If Not PostProcessLipidToolsResults() Then
                processingError = True
            End If

            If processingError Then
                ' Something went wrong
                ' In order to help diagnose things, we will move whatever files were created into the result folder, 
                '  archive it using CopyFailedResultsToArchiveFolder, then return IJobParams.CloseOutType.CLOSEOUT_FAILED
                CopyFailedResultsToArchiveFolder()
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            Dim result = MakeResultsFolder()
            If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                'MakeResultsFolder handles posting to local log, so set database error message and exit
                m_message = "Error making results folder"
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            result = MoveResultFiles()
            If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                ' Note that MoveResultFiles should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
                m_message = "Error moving files into results folder"
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            result = CopyResultsFolderToServer()
            If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                ' Note that CopyResultsFolderToServer should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
                Return result
            End If

        Catch ex As Exception
            m_message = "Exception in LipidMapSearchPlugin->RunTool"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS 'No failures so everything must have succeeded

    End Function

    Private Sub CopyFailedResultsToArchiveFolder()

        Dim result As IJobParams.CloseOutType

        Dim strFailedResultsFolderPath As String = m_mgrParams.GetParam("FailedResultsFolderPath")
        If String.IsNullOrWhiteSpace(strFailedResultsFolderPath) Then strFailedResultsFolderPath = "??Not Defined??"

        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Processing interrupted; copying results to archive folder: " & strFailedResultsFolderPath)

        ' Bump up the debug level if less than 2
        If m_DebugLevel < 2 Then m_DebugLevel = 2

        ' Try to save whatever files are in the work directory
        Dim strFolderPathToArchive As String
        strFolderPathToArchive = String.Copy(m_WorkDir)

        ' Make the results folder
        result = MakeResultsFolder()
        If result = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            ' Move the result files into the result folder
            result = MoveResultFiles()
            If result = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                ' Move was a success; update strFolderPathToArchive
                strFolderPathToArchive = Path.Combine(m_WorkDir, m_ResFolderName)
            End If
        End If

        ' Copy the results folder to the Archive folder
        Dim objAnalysisResults = New clsAnalysisResults(m_mgrParams, m_jobParams)
        objAnalysisResults.CopyFailedResultsToArchiveFolder(strFolderPathToArchive)

    End Sub

    ''' <summary>
    ''' Downloads the latest version of the LipidMaps database
    ''' </summary>
    ''' <param name="diLipidMapsDBFolder">The folder to store the Lipid Maps DB file</param>
    ''' <param name="strNewestLipidMapsDBFileName">The name of the newest Lipid Maps DB in the Lipid Maps DB folder</param>
    ''' <returns>The filename of the latest version of the database</returns>
    ''' <remarks>If the newly downloaded LipidMaps.txt file has a hash that matches the computed hash for strNewestLipidMapsDBFileName, then we update the time stamp on the HashCheckFile instead of copying the downloaded data back to the server</remarks>
    Private Function DownloadNewLipidMapsDB(diLipidMapsDBFolder As DirectoryInfo, strNewestLipidMapsDBFileName As String) As String

        Dim lockFileFound = False
        Dim strLockFilePath As String = String.Empty

        Dim strHashCheckFilePath As String = String.Empty
        Dim strNewestLipidMapsDBFileHash As String = String.Empty

        Dim dtLipidMapsDBFileTime As DateTime

        ' Look for a recent .lock file

        For Each fiFile As FileInfo In diLipidMapsDBFolder.GetFileSystemInfos("*" & clsAnalysisResources.LOCK_FILE_EXTENSION)
            If DateTime.UtcNow.Subtract(fiFile.LastWriteTimeUtc).TotalHours < 2 Then
                lockFileFound = True
                strLockFilePath = fiFile.FullName
                Exit For
            Else
                ' Lock file has aged; delete it
                fiFile.Delete()
            End If
        Next

        If lockFileFound Then

            Dim dataFilePath = strLockFilePath.Substring(0, strLockFilePath.Length - clsAnalysisResources.LOCK_FILE_EXTENSION.Length)
            clsAnalysisResources.CheckForLockFile(dataFilePath, "LipidMapsDB", m_StatusTools, 120)

            strNewestLipidMapsDBFileName = FindNewestLipidMapsDB(diLipidMapsDBFolder, dtLipidMapsDBFileTime)

            If Not String.IsNullOrEmpty(strNewestLipidMapsDBFileName) Then
                If DateTime.UtcNow.Subtract(dtLipidMapsDBFileTime).TotalDays < LIPID_MAPS_STALE_DB_AGE_DAYS Then
                    ' File is now up-to-date
                    Return strNewestLipidMapsDBFileName
                End If
            End If

        End If

        If Not String.IsNullOrEmpty(strNewestLipidMapsDBFileName) Then

            ' Read the hash value stored in the hashcheck file for strNewestLipidMapsDBFileName
            strHashCheckFilePath = GetHashCheckFilePath(diLipidMapsDBFolder.FullName, strNewestLipidMapsDBFileName)

            Using srInFile = New StreamReader(New FileStream(strHashCheckFilePath, FileMode.Open, FileAccess.Read, FileShare.Read))
                strNewestLipidMapsDBFileHash = srInFile.ReadLine()
            End Using

            If String.IsNullOrEmpty(strNewestLipidMapsDBFileHash) Then strNewestLipidMapsDBFileHash = String.Empty
        End If

        ' Call the LipidTools.exe program to obtain the latest database

        Dim strTimeStamp As String = DateTime.Now.ToString("yyyy-MM-dd")
        Dim newLipidMapsDBFilePath = Path.Combine(diLipidMapsDBFolder.FullName, LIPID_MAPS_DB_FILENAME_PREFIX & strTimeStamp)

        ' Create a new lock file
        clsAnalysisResources.CreateLockFile(newLipidMapsDBFilePath, "Downloading LipidMaps.txt file via " & m_MachName)


        ' Call the LipidTools program to obtain the latest database from http://www.lipidmaps.org/
        Dim cmdStr As String
        Dim blnSuccess As Boolean
        Dim strLipidMapsDBFileLocal As String = Path.Combine(m_WorkDir, LIPID_MAPS_DB_FILENAME_PREFIX & strTimeStamp & ".txt")

        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Downloading latest LipidMaps database")

        cmdStr = " -UpdateDBOnly -db " & PossiblyQuotePath(strLipidMapsDBFileLocal)

        If m_DebugLevel >= 1 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, mLipidToolsProgLoc & cmdStr)
        End If

        CmdRunner = New clsRunDosProgram(m_WorkDir)

        With CmdRunner
            .CreateNoWindow = True
            .CacheStandardOutput = False
            .EchoOutputToConsole = True
            .WriteConsoleOutputToFile = False
        End With

        blnSuccess = CmdRunner.RunProgram(mLipidToolsProgLoc, cmdStr, "LipidTools", True)

        If Not blnSuccess Then
            m_message = "Error downloading the latest LipidMaps DB using LipidTools"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)

            If CmdRunner.ExitCode <> 0 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "LipidTools returned a non-zero exit code: " & CmdRunner.ExitCode.ToString)
            Else
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Call to LipidTools failed (but exit code is 0)")
            End If

            Return String.Empty
        End If

        ' Compute the MD5 hash value of the newly downloaded file
        Dim strHashCheckNew As String
        strHashCheckNew = clsGlobal.ComputeFileHashSha1(strLipidMapsDBFileLocal)

        If Not String.IsNullOrEmpty(strNewestLipidMapsDBFileHash) AndAlso strHashCheckNew = strNewestLipidMapsDBFileHash Then
            ' The hashes match; we'll update the timestamp of the hashcheck file below
            If m_DebugLevel >= 1 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Hash code of the newly downloaded database matches the hash for " & strNewestLipidMapsDBFileName & ": " & strNewestLipidMapsDBFileHash)
            End If

            If Path.GetFileName(strLipidMapsDBFileLocal) <> strNewestLipidMapsDBFileName Then
                ' Rename the newly downloaded file to strNewestLipidMapsDBFileName
                Thread.Sleep(500)
                File.Move(strLipidMapsDBFileLocal, Path.Combine(m_WorkDir, strNewestLipidMapsDBFileName))
            End If

        Else
            ' Copy the new file up to the server

            strNewestLipidMapsDBFileName = Path.GetFileName(strLipidMapsDBFileLocal)

            Dim intCopyAttempts = 0

            Do While intCopyAttempts <= 2

                Dim strLipidMapsDBFileTarget As String
                strLipidMapsDBFileTarget = diLipidMapsDBFolder.FullName & " plus " & strNewestLipidMapsDBFileName

                Try
                    intCopyAttempts += 1
                    strLipidMapsDBFileTarget = Path.Combine(diLipidMapsDBFolder.FullName, strNewestLipidMapsDBFileName)
                    File.Copy(strLipidMapsDBFileLocal, strLipidMapsDBFileTarget)
                    Exit Do
                Catch ex As Exception
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception copying Lipid Maps DB to server; attempt=" & intCopyAttempts & ": " & ex.Message)
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Source path: " & strLipidMapsDBFileLocal)
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Target path: " & strLipidMapsDBFileTarget)
                    ' Wait 5 seconds, then try again
                    Thread.Sleep(5000)
                End Try

            Loop

            strHashCheckFilePath = GetHashCheckFilePath(diLipidMapsDBFolder.FullName, strNewestLipidMapsDBFileName)
        End If

        ' Update the hash-check file (do this regardless of whether or not the newly downloaded file matched the most recent one)
        Using swOutFile = New StreamWriter(New FileStream(strHashCheckFilePath, FileMode.Create, FileAccess.Write, FileShare.Read))
            swOutFile.WriteLine(strHashCheckNew)
        End Using

        clsAnalysisResources.DeleteLockFile(newLipidMapsDBFilePath)

        Return strNewestLipidMapsDBFileName

    End Function

    Private Function FindNewestLipidMapsDB(diLipidMapsDBFolder As DirectoryInfo, ByRef dtLipidMapsDBFileTime As DateTime) As String

        Dim strNewestLipidMapsDBFileName As String
        strNewestLipidMapsDBFileName = String.Empty

        dtLipidMapsDBFileTime = DateTime.MinValue

        For Each fiFile As FileInfo In diLipidMapsDBFolder.GetFileSystemInfos(LIPID_MAPS_DB_FILENAME_PREFIX & "*.txt")
            If fiFile.LastWriteTimeUtc > dtLipidMapsDBFileTime Then
                dtLipidMapsDBFileTime = fiFile.LastWriteTimeUtc
                strNewestLipidMapsDBFileName = fiFile.Name
            End If
        Next

        If Not String.IsNullOrEmpty(strNewestLipidMapsDBFileName) Then
            ' Now look for a .hashcheck file for this LipidMapsDB.txt file
            Dim fiHashCheckFile As FileInfo
            fiHashCheckFile = New FileInfo(GetHashCheckFilePath(diLipidMapsDBFolder.FullName, strNewestLipidMapsDBFileName))

            If fiHashCheckFile.Exists Then
                ' Update the Lipid Maps DB file time
                If dtLipidMapsDBFileTime < fiHashCheckFile.LastWriteTimeUtc Then
                    dtLipidMapsDBFileTime = fiHashCheckFile.LastWriteTimeUtc
                End If
            End If

        End If

        Return strNewestLipidMapsDBFileName

    End Function

    Private Function GetHashCheckFilePath(strLipidMapsDBFolderPath As String, strNewestLipidMapsDBFileName As String) As String
        Return Path.Combine(strLipidMapsDBFolderPath, Path.GetFileNameWithoutExtension(strNewestLipidMapsDBFileName) & ".hashcheck")
    End Function

    Private Function GetLipidMapsDatabase() As Boolean

        Dim strParamFileFolderPath As String
        Dim diLipidMapsDBFolder As DirectoryInfo

        Dim strNewestLipidMapsDBFileName As String
        Dim dtLipidMapsDBFileTime As DateTime = DateTime.MinValue

        Dim strSourceFilePath As String
        Dim strTargetFilePath As String

        Dim blnUpdateDB = False

        Try

            strParamFileFolderPath = m_jobParams.GetJobParameter("ParmFileStoragePath", String.Empty)

            If String.IsNullOrEmpty(strParamFileFolderPath) Then
                m_message = "Parameter 'ParmFileStoragePath' is empty"
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & "; unable to get the LipidMaps database")
                Return False
            End If

            diLipidMapsDBFolder = New DirectoryInfo(Path.Combine(strParamFileFolderPath, "LipidMapsDB"))

            If Not diLipidMapsDBFolder.Exists Then
                m_message = "LipidMaps database folder not found"
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & diLipidMapsDBFolder.FullName)
                Return False
            End If

            ' Find the newest date-stamped file
            strNewestLipidMapsDBFileName = FindNewestLipidMapsDB(diLipidMapsDBFolder, dtLipidMapsDBFileTime)

            If String.IsNullOrEmpty(strNewestLipidMapsDBFileName) Then
                blnUpdateDB = True
            ElseIf DateTime.UtcNow.Subtract(dtLipidMapsDBFileTime).TotalDays > LIPID_MAPS_STALE_DB_AGE_DAYS Then
                blnUpdateDB = True
            End If

            If blnUpdateDB Then
                Dim intDownloadAttempts = 0

                Do While intDownloadAttempts <= 2

                    Try
                        intDownloadAttempts += 1
                        strNewestLipidMapsDBFileName = DownloadNewLipidMapsDB(diLipidMapsDBFolder, strNewestLipidMapsDBFileName)
                        Exit Do
                    Catch ex As Exception
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception downloading Lipid Maps DB; attempt=" & intDownloadAttempts & ": " & ex.Message)
                        ' Wait 5 seconds, then try again
                        Thread.Sleep(5000)
                    End Try

                Loop

            End If

            If String.IsNullOrEmpty(strNewestLipidMapsDBFileName) Then
                If String.IsNullOrEmpty(m_message) Then
                    m_message = "Unable to determine the LipidMapsDB file to copy locally"
                End If
                Return False
            End If

            ' File is now up-to-date; copy locally (if not already in the work dir)
            mLipidMapsDBFilename = String.Copy(strNewestLipidMapsDBFileName)
            strSourceFilePath = Path.Combine(diLipidMapsDBFolder.FullName, strNewestLipidMapsDBFileName)
            strTargetFilePath = Path.Combine(m_WorkDir, strNewestLipidMapsDBFileName)

            If Not File.Exists(strTargetFilePath) Then
                If m_DebugLevel >= 1 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Copying lipid Maps DB locally: " & strSourceFilePath)
                End If
                File.Copy(strSourceFilePath, strTargetFilePath)
            End If

        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception obtaining lipid Maps DB: " & ex.Message)
            Return False
        End Try

        Return True

    End Function

    Private Function GetLipidMapsParameterNames() As Dictionary(Of String, String)
        Dim dctParamNames As Dictionary(Of String, String)
        dctParamNames = New Dictionary(Of String, String)(25, StringComparer.CurrentCultureIgnoreCase)

        dctParamNames.Add("AlignmentToleranceNET", "an")
        dctParamNames.Add("AlignmentToleranceMassPPM", "am")
        dctParamNames.Add("DBMatchToleranceMassPPM", "mm")
        dctParamNames.Add("DBMatchToleranceMzPpmCID", "ct")
        dctParamNames.Add("DBMatchToleranceMzPpmHCD", "ht")

        Return dctParamNames
    End Function

    ''' <summary>
    ''' Parse the LipidTools console output file to track progress
    ''' </summary>
    ''' <param name="strConsoleOutputFilePath"></param>
    ''' <remarks></remarks>
    Private Sub ParseConsoleOutputFile(strConsoleOutputFilePath As String)

        ' Example Console output:
        '   Reading local Lipid Maps database...Done.
        '   Reading positive data...Done.
        '   Reading negative data...Done.
        '   Finding features (positive)...200 / 4778
        '   400 / 4778
        '   ...
        '   4600 / 4778
        '   Done (1048 found).
        '   Finding features (negative)...200 / 4558
        '   400 / 4558
        '   ...
        '   4400 / 4558
        '   Done (900 found).
        '   Aligning features...Done (221 alignments).
        '   Matching to Lipid Maps database...Done (2041 matches).
        '   Writing results...Done.
        '   Writing QC data...Done.
        '   Saving QC images...Done.

        ' ReSharper disable once UseImplicitlyTypedVariableEvident
        Static reSubProgress as Regex = New Regex("^(\d+) / (\d+)", RegexOptions.Compiled)

        Try

            If mConsoleOutputProgressMap Is Nothing OrElse mConsoleOutputProgressMap.Count = 0 Then
                mConsoleOutputProgressMap = New Dictionary(Of String, Integer)

                mConsoleOutputProgressMap.Add("Reading local Lipid Maps database", PROGRESS_PCT_LIPID_TOOLS_READING_DATABASE)
                mConsoleOutputProgressMap.Add("Reading positive data", PROGRESS_PCT_LIPID_TOOLS_READING_POSITIVE_DATA)
                mConsoleOutputProgressMap.Add("Reading negative data", PROGRESS_PCT_LIPID_TOOLS_READING_NEGATIVE_DATA)
                mConsoleOutputProgressMap.Add("Finding features (positive)", PROGRESS_PCT_LIPID_TOOLS_FINDING_POSITIVE_FEATURES)
                mConsoleOutputProgressMap.Add("Finding features (negative)", PROGRESS_PCT_LIPID_TOOLS_FINDING_NEGATIVE_FEATURES)
                mConsoleOutputProgressMap.Add("Aligning features", PROGRESS_PCT_LIPID_TOOLS_ALIGNING_FEATURES)
                mConsoleOutputProgressMap.Add("Matching to Lipid Maps database", PROGRESS_PCT_LIPID_TOOLS_MATCHING_TO_DB)
                mConsoleOutputProgressMap.Add("Writing results", PROGRESS_PCT_LIPID_TOOLS_WRITING_RESULTS)
                mConsoleOutputProgressMap.Add("Writing QC data", PROGRESS_PCT_LIPID_TOOLS_WRITING_QC_DATA)
            End If

            If Not File.Exists(strConsoleOutputFilePath) Then
                If m_DebugLevel >= 4 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Console output file not found: " & strConsoleOutputFilePath)
                End If

                Exit Sub
            End If

            If m_DebugLevel >= 4 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Parsing file " & strConsoleOutputFilePath)
            End If


            Dim strLineIn As String
            Dim oMatch As Match
            Dim dblSubProgressAddon As Double

            Dim intSubProgressCount As Integer
            Dim intSubProgressCountTotal As Integer

            Dim intEffectiveProgress As Integer
            intEffectiveProgress = PROGRESS_PCT_LIPID_TOOLS_STARTING

            Using srInFile = New StreamReader(New FileStream(strConsoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))

                Do While Not srInFile.EndOfStream()
                    strLineIn = srInFile.ReadLine()

                    If Not String.IsNullOrWhiteSpace(strLineIn) Then

                        ' Update progress if the line starts with one of the expected phrases
                        For Each oItem As KeyValuePair(Of String, Integer) In mConsoleOutputProgressMap
                            If strLineIn.StartsWith(oItem.Key) Then
                                If intEffectiveProgress < oItem.Value Then
                                    intEffectiveProgress = oItem.Value
                                End If
                            End If
                        Next

                        If intEffectiveProgress = PROGRESS_PCT_LIPID_TOOLS_FINDING_POSITIVE_FEATURES OrElse intEffectiveProgress = PROGRESS_PCT_LIPID_TOOLS_FINDING_NEGATIVE_FEATURES Then
                            oMatch = reSubProgress.Match(strLineIn)
                            If oMatch.Success Then
                                If Integer.TryParse(oMatch.Groups(1).Value, intSubProgressCount) Then
                                    If Integer.TryParse(oMatch.Groups(2).Value, intSubProgressCountTotal) Then
                                        dblSubProgressAddon = intSubProgressCount / CDbl(intSubProgressCountTotal)
                                    End If
                                End If
                            End If
                        End If

                    End If
                Loop

            End Using


            Dim sngEffectiveProgress As Single = intEffectiveProgress

            ' Bump up the effective progress if finding features in positive or negative data
            If intEffectiveProgress = PROGRESS_PCT_LIPID_TOOLS_FINDING_POSITIVE_FEATURES Then
                sngEffectiveProgress += CSng((PROGRESS_PCT_LIPID_TOOLS_FINDING_NEGATIVE_FEATURES - PROGRESS_PCT_LIPID_TOOLS_FINDING_POSITIVE_FEATURES) * dblSubProgressAddon)
            ElseIf intEffectiveProgress = PROGRESS_PCT_LIPID_TOOLS_FINDING_NEGATIVE_FEATURES Then
                sngEffectiveProgress += CSng((PROGRESS_PCT_LIPID_TOOLS_ALIGNING_FEATURES - PROGRESS_PCT_LIPID_TOOLS_FINDING_NEGATIVE_FEATURES) * dblSubProgressAddon)
            End If

            If m_progress < sngEffectiveProgress Then
                m_progress = sngEffectiveProgress
            End If

        Catch ex As Exception
            ' Ignore errors here
            If m_DebugLevel >= 2 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error parsing console output file (" & strConsoleOutputFilePath & "): " & ex.Message)
            End If
        End Try

    End Sub

    ''' <summary>
    ''' Read the LipidMapSearch options file and convert the options to command line switches
    ''' </summary>
    ''' <param name="strParameterFilePath">Path to the LipidMapSearch Parameter File</param>
    ''' <returns>Options string if success; empty string if an error</returns>
    ''' <remarks></remarks>
    Private Function ParseLipidMapSearchParameterFile(strParameterFilePath As String) As String

        Dim sbOptions As StringBuilder
        Dim strLineIn As String

        Dim strKey As String
        Dim strValue As String
        Dim blnValue As Boolean

        Dim dctParamNames As Dictionary(Of String, String)

        sbOptions = New StringBuilder(500)

        Try

            ' Initialize the Param Name dictionary
            dctParamNames = GetLipidMapsParameterNames()

            Using srParamFile = New StreamReader(New FileStream(strParameterFilePath, FileMode.Open, FileAccess.Read, FileShare.Read))

                Do While srParamFile.Peek > -1
                    strLineIn = srParamFile.ReadLine()
                    strKey = String.Empty
                    strValue = String.Empty

                    If Not String.IsNullOrWhiteSpace(strLineIn) Then
                        strLineIn = strLineIn.Trim()

                        If Not strLineIn.StartsWith("#") AndAlso strLineIn.Contains("="c) Then

                            Dim intCharIndex As Integer
                            intCharIndex = strLineIn.IndexOf("="c)
                            If intCharIndex > 0 Then
                                strKey = strLineIn.Substring(0, intCharIndex).Trim()
                                If intCharIndex < strLineIn.Length - 1 Then
                                    strValue = strLineIn.Substring(intCharIndex + 1).Trim()
                                Else
                                    strValue = String.Empty
                                End If
                            End If
                        End If

                    End If

                    If Not String.IsNullOrWhiteSpace(strKey) Then

                        Dim strArgumentSwitch As String = String.Empty

                        ' Check whether strKey is one of the standard keys defined in dctParamNames
                        If dctParamNames.TryGetValue(strKey, strArgumentSwitch) Then
                            sbOptions.Append(" -" & strArgumentSwitch & " " & strValue)

                        ElseIf strKey.ToLower() = "adducts" Then
                            sbOptions.Append(" -adducts " & """" & strValue & """")

                        ElseIf strKey.ToLower() = "noscangroups" Then
                            If Boolean.TryParse(strValue, blnValue) Then
                                If blnValue Then
                                    sbOptions.Append(" -NoScanGroups")
                                End If
                            End If

                        Else
                            ' Ignore the option
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Unrecognized setting in the LipidMaps parameter file: " & strKey)
                        End If

                    End If
                Loop

            End Using

        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception reading LipidMaps parameter file: " & ex.Message)
            Return String.Empty
        End Try

        Return sbOptions.ToString()

    End Function


    Private Function PostProcessLipidToolsResults() As Boolean

        Dim strFolderToZip As String

        Dim lstFilesToMove As List(Of String)
        Dim oIonicZipper As clsIonicZipTools

        Try
            ' Create the PlotData folder and move the plot data text files into that folder
            strFolderToZip = Path.Combine(m_WorkDir, "PlotData")
            Directory.CreateDirectory(strFolderToZip)

            lstFilesToMove = New List(Of String)
            lstFilesToMove.Add(LIPID_TOOLS_RESULT_FILE_PREFIX & "AlignMassError.txt")
            lstFilesToMove.Add(LIPID_TOOLS_RESULT_FILE_PREFIX & "AlignNETError.txt")
            lstFilesToMove.Add(LIPID_TOOLS_RESULT_FILE_PREFIX & "MatchMassError.txt")
            lstFilesToMove.Add(LIPID_TOOLS_RESULT_FILE_PREFIX & "Tiers.txt")
            lstFilesToMove.Add(LIPID_TOOLS_RESULT_FILE_PREFIX & "MassErrorComparison.txt")

            Thread.Sleep(500)

            For Each strFileName As String In lstFilesToMove
                Dim fiSourceFile As FileInfo
                fiSourceFile = New FileInfo(Path.Combine(m_WorkDir, strFileName))

                If fiSourceFile.Exists Then
                    fiSourceFile.MoveTo(Path.Combine(strFolderToZip, strFileName))
                End If
            Next

            Thread.Sleep(500)

            ' Zip up the files in the PlotData folder
            oIonicZipper = New clsIonicZipTools(m_DebugLevel, m_WorkDir)

            oIonicZipper.ZipDirectory(strFolderToZip, Path.Combine(m_WorkDir, "LipidMap_PlotData.zip"))

        Catch ex As Exception
            m_message = "Exception zipping the plot data text files"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message)
            Return False
        End Try

        Try
            Dim fiExcelFile As FileInfo

            fiExcelFile = New FileInfo(Path.Combine(m_WorkDir, LIPID_TOOLS_RESULT_FILE_PREFIX & "results.xlsx"))

            If Not fiExcelFile.Exists Then
                m_message = "Excel results file not found"
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & fiExcelFile.Name)
                Return False
            End If

            fiExcelFile.MoveTo(Path.Combine(m_WorkDir, LIPID_TOOLS_RESULT_FILE_PREFIX & "results_" & m_Dataset & ".xlsx"))

        Catch ex As Exception
            m_message = "Exception renaming Excel results file"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message)
            Return False
        End Try

        Return True

    End Function

    ''' <summary>
    ''' Stores the tool version info in the database
    ''' </summary>
    ''' <remarks></remarks>
    Private Function StoreToolVersionInfo(strLipidToolsProgLoc As String) As Boolean

        Dim strToolVersionInfo As String = String.Empty
        Dim ioLipidTools As FileInfo
        Dim blnSuccess As Boolean

        If m_DebugLevel >= 2 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info")
        End If

        ioLipidTools = New FileInfo(strLipidToolsProgLoc)
        If Not ioLipidTools.Exists Then
            Try
                strToolVersionInfo = "Unknown"
                Return MyBase.SetStepTaskToolVersion(strToolVersionInfo, New List(Of FileInfo))
            Catch ex As Exception
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion: " & ex.Message)
                Return False
            End Try

        End If

        ' Lookup the version of the LipidTools application
        blnSuccess = MyBase.StoreToolVersionInfoOneFile(strToolVersionInfo, ioLipidTools.FullName)
        If Not blnSuccess Then Return False

        ' Store paths to key DLLs in ioToolFiles
        Dim ioToolFiles As New List(Of FileInfo)
        ioToolFiles.Add(ioLipidTools)

        Try
            Return MyBase.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles)
        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion: " & ex.Message)
            Return False
        End Try

    End Function

#End Region

#Region "Event Handlers"

    ''' <summary>
    ''' Event handler for CmdRunner.LoopWaiting event
    ''' </summary>
    ''' <remarks></remarks>
    Private Sub CmdRunner_LoopWaiting() Handles CmdRunner.LoopWaiting

        Static dtLastConsoleOutputParse As DateTime = DateTime.UtcNow

        UpdateStatusFile()

        If DateTime.UtcNow.Subtract(dtLastConsoleOutputParse).TotalSeconds >= 15 Then
            dtLastConsoleOutputParse = DateTime.UtcNow

            ParseConsoleOutputFile(Path.Combine(m_WorkDir, LIPID_TOOLS_CONSOLE_OUTPUT))

            LogProgress("LipidMapSearch")
        End If

    End Sub

#End Region
End Class
