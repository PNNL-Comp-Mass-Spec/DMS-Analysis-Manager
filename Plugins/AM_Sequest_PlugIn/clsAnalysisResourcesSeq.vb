'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2006, Battelle Memorial Institute
' Created 06/07/06
'
'*********************************************************************************************************

Option Strict On

Imports AnalysisManagerBase
Imports System.Collections.Generic
Imports System.IO
Imports System.Text.RegularExpressions

Public Class clsAnalysisResourcesSeq
    Inherits clsAnalysisResources

    '*********************************************************************************************************
    'Subclass for Sequest-specific tasks:
    '	1) Distributes OrgDB files to cluster nodes if running on a cluster
    '	2) Uses ParamFileGenerator to create Sequest param file from database instead of copying it
    '	3) Retrieves zipped DTA files
    '   4) Retrieves _out.txt.tmp file (if it exists)
    '*********************************************************************************************************

#Region "Methods"

    Public Overrides Sub Setup(mgrParams As IMgrParams, jobParams As IJobParams, statusTools As IStatusFile, myEMSLUtilities As clsMyEMSLUtilities)
        MyBase.Setup(mgrParams, jobParams, statusTools, myEmslUtilities)
        SetOption(clsGlobal.eAnalysisResourceOptions.OrgDbRequired, True)
    End Sub

    Protected Sub ArchiveSequestParamFile()

        Dim strSrcFilePath As String = ""
        Dim strTargetFolderPath As String = ""

        Try
            strSrcFilePath = Path.Combine(m_WorkingDir, m_jobParams.GetParam("ParmFileName"))
            strTargetFolderPath = m_jobParams.GetParam("ParmFileStoragePath")

            If m_DebugLevel >= 3 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Verifying that the Sequest parameter file " & m_jobParams.GetParam("ParmFileName") & " exists in " & strTargetFolderPath)
            End If

            ArchiveSequestParamFile(strSrcFilePath, strTargetFolderPath)

        Catch ex As Exception
            If strSrcFilePath Is Nothing Then strSrcFilePath = "??"
            If strTargetFolderPath Is Nothing Then strTargetFolderPath = "??"

            m_message = "Error archiving param file to ParmFileStoragePath"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, m_message & ": " & strSrcFilePath & " --> " & strTargetFolderPath & ex.Message)
        End Try

    End Sub

    Public Sub ArchiveSequestParamFile(ByVal strSrcFilePath As String, ByVal strTargetFolderPath As String)

        Dim blnNeedToArchiveFile As Boolean
        Dim strTargetFilePath As String

        Dim strNewNameBase As String
        Dim strNewName As String
        Dim strNewPath As String

        Dim intRevisionNumber As Integer

        Dim fiArchivedFile As FileInfo

        Dim lstLineIgnoreRegExSpecs = New List(Of Regex)
        lstLineIgnoreRegExSpecs.Add(New Regex("mass_type_parent *=.*"))

        blnNeedToArchiveFile = False

        strTargetFilePath = Path.Combine(strTargetFolderPath, Path.GetFileName(strSrcFilePath))

        If Not File.Exists(strTargetFilePath) Then
            If m_DebugLevel >= 1 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Sequest parameter file not found in archive folder; copying to " & strTargetFilePath)
            End If

            blnNeedToArchiveFile = True
        Else

            ' Read the files line-by-line and compare
            ' Since the first 2 lines of a Sequest parameter file don't matter, and since the 3rd line can vary from computer to computer, we start the comparison at the 4th line

            Const ignoreWhitespace = True

            If Not clsGlobal.TextFilesMatch(strSrcFilePath, strTargetFilePath, 4, 0, ignoreWhitespace, lstLineIgnoreRegExSpecs) Then

                    If m_DebugLevel >= 1 Then
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Sequest parameter file in archive folder doesn't match parameter file for current job; renaming old file and copying new file to " & strTargetFilePath)
                    End If

                    ' Files don't match; rename the old file
                    fiArchivedFile = New FileInfo(strTargetFilePath)

                    strNewNameBase = Path.GetFileNameWithoutExtension(strTargetFilePath) & "_" & fiArchivedFile.LastWriteTime.ToString("yyyy-MM-dd")
                    strNewName = strNewNameBase & Path.GetExtension(strTargetFilePath)

                    ' See if the renamed file exists; if it does, we'll have to tweak the name
                    intRevisionNumber = 1
                    Do
                        strNewPath = Path.Combine(strTargetFolderPath, strNewName)
                        If Not File.Exists(strNewPath) Then
                            Exit Do
                        End If

                        intRevisionNumber += 1
                        strNewName = strNewNameBase & "_v" & intRevisionNumber.ToString & Path.GetExtension(strTargetFilePath)
                    Loop

                    If m_DebugLevel >= 2 Then
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Renaming " & strTargetFilePath & " to " & strNewPath)
                    End If

                    fiArchivedFile.MoveTo(strNewPath)

                    blnNeedToArchiveFile = True
                End If
            End If

            If blnNeedToArchiveFile Then
                ' Copy the new parameter file to the archive

                If m_DebugLevel >= 4 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Copying " & strSrcFilePath & " to " & strTargetFilePath)
                End If

                File.Copy(strSrcFilePath, strTargetFilePath, True)
            End If

    End Sub

    ''' <summary>
    ''' Look for file _out.txt.tmp in the transfer folder
    ''' Retrieves the file if it was found and if both JobParameters.xml file and the sequest param file match the 
    ''' JobParameters.xml and sequest param file in the local working directory
    ''' </summary>
    ''' <returns>
    ''' CLOSEOUT_SUCCESS if an existing file was found and copied, 
    ''' CLOSEOUT_FILE_NOT_FOUND if an existing file was not found, and 
    ''' CLOSEOUT_FAILURE if an error
    ''' </returns>
    Protected Function CheckForExistingConcatenatedOutFile() As IJobParams.CloseOutType

        Try

            Dim strJob = m_jobParams.GetParam("Job")
            Dim transferFolderPath = m_jobParams.GetParam("JobParameters", "transferFolderPath")

            If String.IsNullOrWhiteSpace(transferFolderPath) Then
                ' Transfer folder path is not defined
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "transferFolderPath is empty; this is unexpected")
                Exit Try
            Else
                transferFolderPath = Path.Combine(transferFolderPath, m_jobParams.GetParam("JobParameters", "DatasetFolderName"))
                transferFolderPath = Path.Combine(transferFolderPath, m_jobParams.GetParam("StepParameters", "OutputFolderName"))
            End If

            If m_DebugLevel >= 4 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Checking for " & clsAnalysisToolRunnerSeqBase.CONCATENATED_OUT_TEMP_FILE & " file at " & transferFolderPath)
            End If

            Dim diSourceFolder = New DirectoryInfo(transferFolderPath)

            If Not diSourceFolder.Exists Then
                ' Transfer folder not found; return false
                If m_DebugLevel >= 4 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "  ... Transfer folder not found: " & diSourceFolder.FullName)
                End If
                Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
            End If

            Dim concatenatedTempFilePath = Path.Combine(diSourceFolder.FullName, m_DatasetName & clsAnalysisToolRunnerSeqBase.CONCATENATED_OUT_TEMP_FILE)

            Dim fiTempOutFile = New FileInfo(concatenatedTempFilePath)
            If Not fiTempOutFile.Exists Then
                If m_DebugLevel >= 4 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "  ... " & clsAnalysisToolRunnerSeqBase.CONCATENATED_OUT_TEMP_FILE & " file not found")
                End If
                Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
            End If

            If m_DebugLevel >= 1 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, clsAnalysisToolRunnerSeqBase.CONCATENATED_OUT_TEMP_FILE & " file found for job " & strJob & " (file size = " & (fiTempOutFile.Length / 1024.0).ToString("#,##0") & " KB); comparing JobParameters.xml file and Sequest parameter file to local copies")
            End If

            ' Compare the remote and local copies of the JobParameters file
            Dim fileNameToCompare = "JobParameters_" & strJob & ".xml"
            Dim remoteFilePath = Path.Combine(diSourceFolder.FullName, fileNameToCompare & ".tmp")
            Dim localFilePath = Path.Combine(m_WorkingDir, fileNameToCompare)

            Dim filesMatch = CompareRemoteAndLocalFilesForResume(remoteFilePath, localFilePath, "JobParameters")
            If Not filesMatch Then
                ' Files don't match; do not resume
                Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
            End If

            ' Compare the remote and local copies of the Sequest Parameter file
            fileNameToCompare = m_jobParams.GetParam("ParmFileName")
            remoteFilePath = Path.Combine(diSourceFolder.FullName, fileNameToCompare & ".tmp")
            localFilePath = Path.Combine(m_WorkingDir, fileNameToCompare)

            filesMatch = CompareRemoteAndLocalFilesForResume(remoteFilePath, localFilePath, "Sequest Parameter")
            If Not filesMatch Then
                ' Files don't match; do not resume
                Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
            End If

            ' Everything matches up; copy fiTempOutFile locally
            Try
                fiTempOutFile.CopyTo(Path.Combine(m_WorkingDir, fiTempOutFile.Name), True)

                If m_DebugLevel >= 1 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Copied " & fiTempOutFile.Name & " locally; will resume Sequest analysis")
                End If

                ' If the job succeeds, we should delete the _out.txt.tmp file from the transfer folder
                ' Add the full path to m_ServerFilesToDelete using AddServerFileToDelete
                m_jobParams.AddServerFileToDelete(fiTempOutFile.FullName)

            Catch ex As Exception
                ' Error copying the file; treat this as a failed job
                m_message = " Exception copying " & clsAnalysisToolRunnerSeqBase.CONCATENATED_OUT_TEMP_FILE & " file locally"
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "  ... Exception copying " & fiTempOutFile.FullName & " locally; unable to resume: " & ex.Message)
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End Try

            ' Look for a sequest.log.tmp file
            Dim lstLogFiles = diSourceFolder.GetFiles("sequest.log.tmp").ToList()

            If lstLogFiles.Count > 0 Then
                Dim strExistingSeqLogFileRenamed As String
                Dim fiFirstLogFile = lstLogFiles.First()

                With fiFirstLogFile
                    ' Copy the sequest.log.tmp file to the work directory, but rename it to include a time stamp
                    strExistingSeqLogFileRenamed = Path.GetFileNameWithoutExtension(.Name)
                    strExistingSeqLogFileRenamed = Path.GetFileNameWithoutExtension(strExistingSeqLogFileRenamed)
                    strExistingSeqLogFileRenamed &= "_" & .LastWriteTime.ToString("yyyyMMdd_HHmm") & ".log"
                End With

                Try
                    localFilePath = Path.Combine(m_WorkingDir, strExistingSeqLogFileRenamed)
                    fiFirstLogFile.CopyTo(localFilePath, True)

                    If m_DebugLevel >= 3 Then
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Copied " & Path.GetFileName(fiFirstLogFile.Name) & " locally, renaming to " & strExistingSeqLogFileRenamed)
                    End If

                    m_jobParams.AddServerFileToDelete(fiFirstLogFile.FullName)

                    ' Copy the new file back to the transfer folder (necessary in case this job fails)
                    File.Copy(localFilePath, Path.Combine(transferFolderPath, strExistingSeqLogFileRenamed))

                Catch ex As Exception
                    ' Ignore errors here
                End Try
            End If

            Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

        Catch ex As Exception
            m_message = "Error in CheckForExistingConcatenatedOutFile"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error in CheckForExistingConcatenatedOutFile: " & ex.Message)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

    End Function

    Protected Function CompareRemoteAndLocalFilesForResume(ByVal strRemoteFilePath As String, ByVal strLocalFilePath As String, ByVal strFileDescription As String) As Boolean

        If Not File.Exists(strRemoteFilePath) Then
            If m_DebugLevel >= 1 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "  ... " & strFileDescription & " file not found remotely; unable to resume: " & strRemoteFilePath)
            End If
            Return False
        End If

        If Not File.Exists(strLocalFilePath) Then
            If m_DebugLevel >= 1 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "  ... " & strFileDescription & " file not found locally; unable to resume: " & strLocalFilePath)
            End If
            Return False
        End If

        Const ignoreWhitespace = True

        If clsGlobal.TextFilesMatch(strRemoteFilePath, strLocalFilePath, 0, 0, ignoreWhitespace) Then
            Return True
        Else
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "  ... " & strFileDescription & " file at " & strRemoteFilePath & " doesn't match local file; unable to resume")
            Return False
        End If

    End Function

    ''' <summary>
    ''' Retrieves files necessary for performance of Sequest analysis
    ''' </summary>
    ''' <returns>IJobParams.CloseOutType indicating success or failure</returns>
    ''' <remarks></remarks>
    Public Overrides Function GetResources() As IJobParams.CloseOutType

        Dim LocOrgDBFolder As String
        Dim eExistingOutFileResult As IJobParams.CloseOutType

        ' Retrieve Fasta file (we'll distribute it to the cluster nodes later in this function)
        LocOrgDBFolder = m_mgrParams.GetParam("orgdbdir")
        If Not RetrieveOrgDB(LocOrgDBFolder) Then Return IJobParams.CloseOutType.CLOSEOUT_FAILED

        ' Retrieve param file
        If Not RetrieveGeneratedParamFile(m_jobParams.GetParam("ParmFileName")) Then
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        ' Make sure the Sequest parameter file is present in the parameter file storage path
        ArchiveSequestParamFile()

        ' Look for an existing _out.txt.tmp file in the transfer folder on the storage server
        ' If one exists, and if the parameter file and settings file associated with the file match the ones in the work folder, then copy it locally
        eExistingOutFileResult = CheckForExistingConcatenatedOutFile()

        If eExistingOutFileResult = IJobParams.CloseOutType.CLOSEOUT_FAILED Then
            If String.IsNullOrEmpty(m_message) Then
                m_message = "Call to CheckForExistingConcatenatedOutFile failed"
            End If
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        ' Retrieve the _DTA.txt file
        ' Note that if the file was found in MyEMSL then RetrieveDtaFiles will auto-call ProcessMyEMSLDownloadQueue to download the file
        ' The file will be de-concatenated by function clsAnalysisToolRunnerSeqBase.CheckForExistingConcatenatedOutFile
        If Not RetrieveDtaFiles() Then
            ' Errors were reported in function call, so just return
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        ' If running on a cluster, then distribute the database file across the nodes
        ' We do this after we have successfully retrieved the DTA files and unzipped them
        If m_mgrParams.GetParam("cluster", True) Then
            ' Check the cluster nodes, updating local database copies as necessary
            Dim OrbDBName As String = m_jobParams.GetParam("PeptideSearch", "generatedFastaName")
            If String.IsNullOrEmpty(OrbDBName) Then
                m_message = "generatedFastaName parameter is empty; RetrieveOrgDB did not create a fasta file"
                Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
            End If

            If Not VerifyDatabase(OrbDBName, LocOrgDBFolder) Then
                ' Errors were reported in function call, so just return
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

        End If

        'Add all the extensions of the files to delete after run
        m_jobParams.AddResultFileExtensionToSkip("_dta.zip")    ' Zipped DTA
        m_jobParams.AddResultFileExtensionToSkip("_dta.txt")    ' Unzipped, concatenated DTA
        m_jobParams.AddResultFileExtensionToSkip(".dta")        ' DTA files
        m_jobParams.AddResultFileExtensionToSkip(".tmp")        ' Temp files

        'All finished
        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    ''' <summary>
    ''' Verifies the fasta file required by the job is distributed to all the cluster nodes
    ''' </summary>
    ''' <param name="OrgDBName">Fasta file name</param>
    ''' <param name="OrgDBPath">Fasta file location on analysis machine</param>
    ''' <returns>TRUE for success; FALSE for failure</returns>
    ''' <remarks></remarks>
    Private Function VerifyDatabase(ByVal OrgDBName As String, ByVal OrgDBPath As String) As Boolean

        Dim HostFilePath As String = m_mgrParams.GetParam("hostsfilelocation")
        Dim Nodes As List(Of String)
        Dim NodeDbLoc As String = m_mgrParams.GetParam("nodedblocation")

        Dim strLogMessage As String

        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Copying database to nodes: " & Path.GetFileName(OrgDBName))

        'Get the list of nodes from the hosts file
        Nodes = GetHostList(HostFilePath)
        If Nodes Is Nothing OrElse Nodes.Count = 0 Then
            m_message = "Unable to determine node names from host file"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & HostFilePath)
            Return False
        End If


        ' Define the path to the database on the head node
        Dim OrgDBFilePath As String = Path.Combine(OrgDBPath, OrgDBName)
        If Not File.Exists(OrgDBFilePath) Then
            m_message = "Database file can't be found on master"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & OrgDBFilePath)
            Return False
        End If

        ' For each node, verify specified database file is present and matches file on host
        ' Allow up to 25% of the nodes to fail (they should just get skipped when the Sequest search occurs)

        Dim blnFileAlreadyExists = False
        Dim blnNotEnoughFreeSpace = False

        Dim intNodeCountProcessed = 0
        Dim intNodeCountFailed = 0
        Dim intNodeCountFileAlreadyExists = 0
        Dim intNodeCountNotEnoughFreeSpace = 0

        For Each NodeName As String In Nodes
            If Not VerifyRemoteDatabase(OrgDBFilePath, "\\" & NodeName & "\" & NodeDbLoc, blnFileAlreadyExists, blnNotEnoughFreeSpace) Then
                intNodeCountFailed += 1
                blnFileAlreadyExists = True

                If blnNotEnoughFreeSpace Then
                    intNodeCountNotEnoughFreeSpace += 1
                End If
            End If

            intNodeCountProcessed += 1
            If blnFileAlreadyExists Then intNodeCountFileAlreadyExists += 1
        Next

        If intNodeCountProcessed = 0 Then
            m_message = "The Nodes collection is empty; unable to continue"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
            Return False
        End If

        If intNodeCountFailed > 0 Then
            Const MINIMUM_NODE_SUCCESS_PCT As Integer = 75
            Dim dblNodeCountSuccessPct As Double
            dblNodeCountSuccessPct = (intNodeCountProcessed - intNodeCountFailed) / intNodeCountProcessed * 100

            strLogMessage = "Error, unable to verify database on " & intNodeCountFailed.ToString & " node"
            If intNodeCountFailed > 1 Then strLogMessage &= "s"
            strLogMessage &= " (" & dblNodeCountSuccessPct.ToString("0") & "% succeeded)"

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, strLogMessage)

            If dblNodeCountSuccessPct < MINIMUM_NODE_SUCCESS_PCT Then
                m_message = "Unable to copy the database file one or more nodes; "
                If intNodeCountNotEnoughFreeSpace > 0 Then
                    m_message = "not enough space on the disk"
                Else
                    m_message = "see " & m_MgrName & " manager log for details"
                End If

                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Aborting since did not succeed on at least " & MINIMUM_NODE_SUCCESS_PCT.ToString & "% of the nodes")
                Return False
            Else
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Warning, will continue analysis using the remaining nodes")

                ' Decrement intNodeCountProcessed by intNodeCountFailed so the stats in the next If / EndIf block are valid
                intNodeCountProcessed -= intNodeCountFailed
            End If
        End If

        If m_DebugLevel >= 1 Then
            If intNodeCountFileAlreadyExists = 0 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Copied database to " & intNodeCountProcessed.ToString & " nodes")
            Else
                strLogMessage = "Verified database exists on " & intNodeCountProcessed.ToString & " nodes"

                If intNodeCountProcessed - intNodeCountFileAlreadyExists > 0 Then
                    strLogMessage &= " (newly copied to " & (intNodeCountProcessed - intNodeCountFileAlreadyExists).ToString & " nodes)"
                End If

                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, strLogMessage)
            End If
        End If

        'Database file has been distributed, so return happy
        Return True

    End Function

    ''' <summary>
    ''' Reads the list of nodes from the hosts config file
    ''' </summary>
    ''' <param name="HostFilePath">Name of hosts file on cluster head node</param>
    ''' <returns>returns a string collection containing IP addresses for each node</returns>
    ''' <remarks></remarks>
    Private Function GetHostList(ByVal HostFilePath As String) As List(Of String)

        Dim lstNodes As New List(Of String)
        Dim InpLine As String
        Dim LineFields() As String
        Dim Separators() As String = {" "}

        Try
            Using srHostFile As StreamReader = New StreamReader(New FileStream(HostFilePath, FileMode.Open, FileAccess.Read, FileShare.Read))

                Do While Not srHostFile.EndOfStream
                    'Read the line from the file and check to see if it contains a node IP address. 
                    ' If it does, add the IP address to the collection of addresses
                    InpLine = srHostFile.ReadLine

                    'Verify the line isn't a comment line
                    If Not String.IsNullOrWhiteSpace(InpLine) AndAlso Not InpLine.Contains("#") Then
                        'Parse the node name and add it to the collection
                        LineFields = InpLine.Split(Separators, StringSplitOptions.RemoveEmptyEntries)
                        If LineFields.Length >= 1 Then
                            If Not lstNodes.Contains(LineFields(0)) Then
                                lstNodes.Add(LineFields(0))
                            End If
                        End If
                    End If

                Loop

            End Using

        Catch Err As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error reading cluster config file '" & HostFilePath & "': " & Err.Message)
            Return Nothing
        End Try

        'Return the list of nodes, if any
        Return lstNodes

    End Function

    Private Function VerifyFilesMatchSizeAndDate(ByVal FileA As String, ByVal FileB As String) As Boolean

        Const DETAILED_LOG_THRESHOLD As Integer = 3

        Dim blnFilesMatch As Boolean
        Dim ioFileA As FileInfo
        Dim ioFileB As FileInfo
        Dim dblSecondDiff As Double

        blnFilesMatch = False
        If File.Exists(FileA) AndAlso File.Exists(FileB) Then
            ' Files both exist
            ioFileA = New FileInfo(FileA)
            ioFileB = New FileInfo(FileB)

            If m_DebugLevel > DETAILED_LOG_THRESHOLD Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Comparing files: " & ioFileA.FullName & " vs. " & ioFileB.FullName)
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, " ... file sizes: " & ioFileA.Length.ToString & " vs. " & ioFileB.Length.ToString)
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, " ... file dates: " & ioFileA.LastWriteTimeUtc.ToString & " vs. " & ioFileB.LastWriteTimeUtc.ToString)
            End If

            If ioFileA.Length = ioFileB.Length Then
                ' Sizes match
                If ioFileA.LastWriteTimeUtc = ioFileB.LastWriteTimeUtc Then
                    ' Dates match
                    If m_DebugLevel > DETAILED_LOG_THRESHOLD Then
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, " ... sizes match and dates match exactly")
                    End If

                    blnFilesMatch = True
                Else
                    ' Dates don't match, are they off by one hour?
                    dblSecondDiff = Math.Abs(ioFileA.LastWriteTimeUtc.Subtract(ioFileB.LastWriteTimeUtc).TotalSeconds)

                    If dblSecondDiff <= 2 Then
                        ' File times differ by less than 2 seconds; count this as the same

                        If m_DebugLevel > DETAILED_LOG_THRESHOLD Then
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, " ... sizes match and dates match within 2 seconds (" & dblSecondDiff.ToString("0.0") & " seconds apart)")
                        End If

                        blnFilesMatch = True
                    ElseIf dblSecondDiff >= 3598 And dblSecondDiff <= 3602 Then
                        ' File times are an hour apart (give or take 2 seconds); count this as the same

                        If m_DebugLevel > DETAILED_LOG_THRESHOLD Then
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, " ... sizes match and dates match within 1 hour (" & dblSecondDiff.ToString("0.0") & " seconds apart)")
                        End If

                        blnFilesMatch = True
                    Else
                        If m_DebugLevel >= DETAILED_LOG_THRESHOLD Then
                            If m_DebugLevel = DETAILED_LOG_THRESHOLD Then
                                ' This message didn't get logged above; log it now.
                                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Comparing files: " & ioFileA.FullName & " vs. " & ioFileB.FullName)
                            End If
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, " ... sizes match but times do not match within 2 seconds or 1 hour (" & dblSecondDiff.ToString("0.0") & " seconds apart)")
                        End If

                    End If
                End If
            End If
        End If

        Return blnFilesMatch

    End Function

    ''' <summary>
    ''' Verifies specified database is present on the node. If present, compares date and size. If not
    '''	present, copies database from master
    ''' </summary>
    ''' <param name="OrgDBFilePath">Full path to the source file</param>
    ''' <param name="DestPath">Fasta storage location on cluster node</param>
    ''' <param name="blnFileAlreadyExists">Output parameter: true if the file already exists</param>
    ''' <param name="blnNotEnoughFreeSpace">Output parameter: true if the target node does not have enough space for the file</param>
    ''' <returns>TRUE for success; FALSE for failure</returns>
    ''' <remarks>Assumes DestPath is URL containing IP address of node and destination share name</remarks>
    Private Function VerifyRemoteDatabase(ByVal OrgDBFilePath As String, ByVal DestPath As String, ByRef blnFileAlreadyExists As Boolean, ByRef blnNotEnoughFreeSpace As Boolean) As Boolean

        Dim CopyNeeded As Boolean

        blnFileAlreadyExists = False
        blnNotEnoughFreeSpace = False

        If m_DebugLevel > 3 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Verifying database " & DestPath)
        End If

        Dim DestFile As String = Path.Combine(DestPath, Path.GetFileName(OrgDBFilePath))
        Try
            If File.Exists(DestFile) Then
                'File was found on node, compare file size and date (allowing for a 1 hour difference in case of daylight savings)
                If VerifyFilesMatchSizeAndDate(OrgDBFilePath, DestFile) Then
                    blnFileAlreadyExists = True
                    CopyNeeded = False
                Else
                    CopyNeeded = True
                End If
            Else
                'File wasn't on node, we'll have to copy it
                CopyNeeded = True
            End If

            'Does the file need to be copied to the node?
            If CopyNeeded Then
                'Copy the file
                If m_DebugLevel > 3 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Copying database file " & DestFile)
                End If
                File.Copy(OrgDBFilePath, DestFile, True)
                'Now everything is in its proper place, so return
                Return True
            Else
                'File existed and was current, so everybody's happy
                If m_DebugLevel >= 3 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Database file at " & DestPath & " matches the source file's date and time; will not re-copy")
                End If
                Return True
            End If
        Catch Err As Exception
            'Something bad happened
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error copying database file to " & DestFile & ": " & Err.Message)
            If Err.Message.Contains("not enough space") Then
                blnNotEnoughFreeSpace = True
            End If
            Return False
        End Try

    End Function
#End Region

End Class
