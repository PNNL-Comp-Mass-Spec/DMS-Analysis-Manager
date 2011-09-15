'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2006, Battelle Memorial Institute
' Created 06/07/06
'
' Last modified 09/18/2008
' Last modified 06/15/2009 JDS - Added logging using log4net
'*********************************************************************************************************

Imports System.IO
Imports System.Collections.Specialized
Imports AnalysisManagerBase

Public Class clsAnalysisResourcesSeq
	Inherits clsAnalysisResources

	'*********************************************************************************************************
	'Subclass for Sequest-specific tasks:
	'	1) Distributes OrgDB files to cluster nodes if running on a cluster
	'	2) Uses ParamFileGenerator to create Sequest param file from database instead of copying it
	'	3) Retrieves zipped DTA files, unzips, and un-concatenates them
	'*********************************************************************************************************

#Region "Methods"

    Protected Sub ArchiveSequestParamFile()

        Dim strSrcFilePath As String = ""
        Dim strTargetFolderPath As String = ""

        Try
            strSrcFilePath = System.IO.Path.Combine(m_mgrParams.GetParam("workdir"), m_jobParams.GetParam("ParmFileName"))
            strTargetFolderPath = m_jobParams.GetParam("ParmFileStoragePath")

            If m_DebugLevel >= 3 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Verifying that the Sequest parameter file " & m_jobParams.GetParam("ParmFileName") & " exists in " & strTargetFolderPath)
            End If

            ArchiveSequestParamFile(strSrcFilePath, strTargetFolderPath)

        Catch ex As Exception
            If strSrcFilePath Is Nothing Then strSrcFilePath = "??"
            If strTargetFolderPath Is Nothing Then strTargetFolderPath = "??"

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error archiving param file to ParmFileStoragePath: " & strSrcFilePath & " --> " & strTargetFolderPath & ex.Message)
        End Try

    End Sub

    Public Sub ArchiveSequestParamFile(ByVal strSrcFilePath As String, ByVal strTargetFolderPath As String)

        Dim blnNeedToArchiveFile As Boolean
        Dim strTargetFilePath As String
        Dim strLineIgnoreRegExList() As String

        Dim strNewNameBase As String
        Dim strNewName As String
        Dim strNewPath As String

        Dim intRevisionNumber As Integer

        Dim fiArchivedFile As System.IO.FileInfo

        ReDim strLineIgnoreRegExList(0)
        strLineIgnoreRegExList(0) = "mass_type_parent *=.*"

        blnNeedToArchiveFile = False

        strTargetFilePath = System.IO.Path.Combine(strTargetFolderPath, System.IO.Path.GetFileName(strSrcFilePath))

        If Not System.IO.File.Exists(strTargetFilePath) Then
            If m_DebugLevel >= 1 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Sequest parameter file not found in archive folder; copying to " & strTargetFilePath)
            End If

            blnNeedToArchiveFile = True
        Else

            ' Read the files line-by-line and compare
            ' Since the first 2 lines of a Sequest parameter file don't matter, and since the 3rd line can vary from computer to computer, we start the comparison at the 4th line

            If Not TextFilesMatch(strSrcFilePath, strTargetFilePath, 4, 0, True, strLineIgnoreRegExList) Then

                If m_DebugLevel >= 1 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Sequest parameter file in archive folder doesn't match parameter file for current job; renaming old file and copying new file to " & strTargetFilePath)
                End If

                ' Files don't match; rename the old file
                fiArchivedFile = New System.IO.FileInfo(strTargetFilePath)

                strNewNameBase = System.IO.Path.GetFileNameWithoutExtension(strTargetFilePath) & "_" & fiArchivedFile.LastWriteTime.ToString("yyyy-MM-dd")
                strNewName = strNewNameBase & System.IO.Path.GetExtension(strTargetFilePath)

                ' See if the renamed file exists; if it does, we'll have to tweak the name
                intRevisionNumber = 1
                Do
                    strNewPath = System.IO.Path.Combine(strTargetFolderPath, strNewName)
                    If Not System.IO.File.Exists(strNewPath) Then
                        Exit Do
                    End If

                    intRevisionNumber += 1
                    strNewName = strNewNameBase & "_v" & intRevisionNumber.ToString & System.IO.Path.GetExtension(strTargetFilePath)
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

            System.IO.File.Copy(strSrcFilePath, strTargetFilePath, True)
        End If

    End Sub

    Protected Sub CheckForExistingOutFiles()

        ' Note that this sub is similar to CheckForExistingOutFiles() in clsAnalysisToolRunnerSeqBase
        '  but this sub only looks for the .out files; it doesn't move them.

        Const RESUME_FILE_NAME As String = "Resume.txt"

        Dim strResultFolderName As String
        Dim strFailedResultsFolderPath As String
        Dim ioSourceFolder As System.IO.DirectoryInfo
        Dim ioFileList() As System.IO.FileInfo

        Dim intIndex As Integer
        Dim intValidOutFiles As Integer

        Try
            strResultFolderName = m_jobParams.GetParam("OutputFolderName")
            strFailedResultsFolderPath = m_mgrParams.GetParam("FailedResultsFolderPath")

            If strResultFolderName Is Nothing OrElse strResultFolderName.Length = 0 Then
                ' Results folder name is not defined
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "m_ResFolderName is empty; this is unexpected")
                Exit Try
            End If

            strFailedResultsFolderPath = System.IO.Path.Combine(strFailedResultsFolderPath, strResultFolderName)

            If m_DebugLevel >= 4 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Looking for existing .Out files at " & strFailedResultsFolderPath)
            End If

            ioSourceFolder = New System.IO.DirectoryInfo(strFailedResultsFolderPath)
            If ioSourceFolder.Exists Then
                If m_DebugLevel >= 1 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Archived results folder found for job " & m_jobParams.GetParam("Job") & "; checking for a file named " & RESUME_FILE_NAME)
                End If

                If ioSourceFolder.GetFiles(RESUME_FILE_NAME).Length > 0 Then
                    ' Yes, folder contains a file named Resume.txt
                    ' Are there any non-empty .Out files?

                    ioFileList = ioSourceFolder.GetFiles("*.out")
                    intValidOutFiles = 0

                    For intIndex = 0 To ioFileList.Length - 1
                        If ioFileList(intIndex).Length > 0 Then
                            intValidOutFiles += 1
                        End If
                    Next intIndex

                    If intValidOutFiles = 0 Then
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Warning: did not find any existing .Out files in the archived results folder (" & strFailedResultsFolderPath & ")")
                    Else
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Found " & intValidOutFiles & " .Out files in " & strFailedResultsFolderPath & "; these will be moved to the Work Directory prior to starting Sequest")
                    End If

                End If

            End If

        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error in CheckForExistingOutFiles: " & ex.Message)
        End Try

    End Sub

    ''' <summary>
    ''' Compares two files line-by-line.  If intComparisonStartLine is > 0, then ignores differences up until the given line number.  If 
    ''' </summary>
    ''' <param name="strFile1">First file</param>
    ''' <param name="strFile2">Second file</param>
    ''' <param name="intComparisonStartLine">Line at which to start the comparison; if 0 or 1, then compares all lines</param>
    ''' <param name="intComparisonEndLine">Line at which to end the comparison; if 0, then compares all the way to the end</param>
    ''' <param name="blnIgnoreWhitespace">If true, then removes white space from the beginning and end of each line before compaing</param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Protected Function TextFilesMatch(ByVal strFile1 As String, ByVal strFile2 As String, _
                                      ByVal intComparisonStartLine As Integer, ByVal intComparisonEndLine As Integer, _
                                      ByVal blnIgnoreWhitespace As Boolean) As Boolean

        Return TextFilesMatch(strFile1, strFile2, intComparisonStartLine, intComparisonEndLine, blnIgnoreWhitespace, Nothing)

    End Function

    ''' <summary>
    ''' Compares two files line-by-line.  If intComparisonStartLine is > 0, then ignores differences up until the given line number.  If 
    ''' </summary>
    ''' <param name="strFile1">First file</param>
    ''' <param name="strFile2">Second file</param>
    ''' <param name="intComparisonStartLine">Line at which to start the comparison; if 0 or 1, then compares all lines</param>
    ''' <param name="intComparisonEndLine">Line at which to end the comparison; if 0, then compares all the way to the end</param>
    ''' <param name="blnIgnoreWhitespace">If true, then removes white space from the beginning and end of each line before compaing</param>
    ''' <param name="strLineIgnoreRegExList">List of RegEx match specs that indicate lines to ignore</param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Protected Function TextFilesMatch(ByVal strFile1 As String, ByVal strFile2 As String, _
                                      ByVal intComparisonStartLine As Integer, ByVal intComparisonEndLine As Integer, _
                                      ByVal blnIgnoreWhitespace As Boolean, _
                                      ByRef strLineIgnoreRegExList() As String) As Boolean

        Dim srFile1 As System.IO.StreamReader
        Dim srFile2 As System.IO.StreamReader

        Dim strLineIn1 As String
        Dim strLineIn2 As String

        Dim intIndex As Integer

        Dim chWhiteSpaceChars() As Char
        Dim blnFilesMatch As Boolean
        Dim intLineNumber As Integer = 0

        Dim intLineIgnoreListCount As Integer
        Dim reLineIgnoreList() As System.Text.RegularExpressions.Regex

        ReDim chWhiteSpaceChars(1)
        chWhiteSpaceChars(0) = ControlChars.Tab
        chWhiteSpaceChars(1) = " "c

        blnFilesMatch = True

        Try
            intLineIgnoreListCount = 0
            If Not strLineIgnoreRegExList Is Nothing AndAlso strLineIgnoreRegExList.Length > 0 Then
                ReDim reLineIgnoreList(strLineIgnoreRegExList.Length - 1)

                For intIndex = 0 To strLineIgnoreRegExList.Length - 1
                    If Not strLineIgnoreRegExList(intIndex) Is Nothing AndAlso strLineIgnoreRegExList(intIndex).Length > 0 Then
                        reLineIgnoreList(intLineIgnoreListCount) = New System.Text.RegularExpressions.Regex(strLineIgnoreRegExList(intIndex), System.Text.RegularExpressions.RegexOptions.Compiled Or System.Text.RegularExpressions.RegexOptions.IgnoreCase)
                        intLineIgnoreListCount += 1
                    End If
                Next
            Else
                ReDim reLineIgnoreList(0)
            End If

            srFile1 = New System.IO.StreamReader(New System.IO.FileStream(strFile1, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.Read))
            srFile2 = New System.IO.StreamReader(New System.IO.FileStream(strFile2, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.Read))

            Do While srFile1.Peek >= 0
                strLineIn1 = srFile1.ReadLine
                intLineNumber += 1

                If intComparisonEndLine > 0 AndAlso intLineNumber > intComparisonEndLine Then
                    ' No need to compare further; files match up to this point
                    Exit Do
                End If

                If srFile2.Peek >= 0 Then
                    strLineIn2 = srFile2.ReadLine

                    If intLineNumber >= intComparisonStartLine Then
                        If blnIgnoreWhitespace Then
                            strLineIn1 = strLineIn1.Trim(chWhiteSpaceChars)
                            strLineIn2 = strLineIn2.Trim(chWhiteSpaceChars)
                        End If

                        If strLineIn1 <> strLineIn2 Then
                            ' Lines don't match; are we ignoring both of them?
                            If TextFilesMatchIgnoreLine(strLineIn1, intLineIgnoreListCount, reLineIgnoreList) AndAlso _
                               TextFilesMatchIgnoreLine(strLineIn2, intLineIgnoreListCount, reLineIgnoreList) Then
                                ' Ignoring both lines
                            Else
                                blnFilesMatch = False
                                Exit Do
                            End If
                        End If
                    End If

                Else
                    ' File1 has more lines than file2
                    If blnIgnoreWhitespace Then
                        ' Ignoring whitespace
                        ' If file1 only has blank lines from here on out, then the files match; otherwise, they don't
                        ' See if the remaining lines are blank
                        Do
                            If strLineIn1.Length <> 0 Then
                                If Not TextFilesMatchIgnoreLine(strLineIn1, intLineIgnoreListCount, reLineIgnoreList) Then
                                    blnFilesMatch = False
                                    Exit Do
                                End If
                            End If

                            If srFile1.Peek >= 0 Then
                                strLineIn1 = srFile1.ReadLine
                                strLineIn1 = strLineIn1.Trim(chWhiteSpaceChars)
                            Else
                                Exit Do
                            End If
                        Loop

                    Else
                        ' Not ignoring whitespace; files don't match
                        blnFilesMatch = False
                    End If

                    Exit Do
                End If
            Loop

            If srFile2.Peek >= 0 Then
                ' File2 has more lines than file1
                If blnIgnoreWhitespace Then
                    ' Ignoring whitespace
                    ' If file2 only has blank lines from here on out, then the files match; otherwise, they don't
                    ' See if the remaining lines are blank
                    Do
                        strLineIn2 = srFile2.ReadLine
                        strLineIn2 = strLineIn2.Trim(chWhiteSpaceChars)

                        If strLineIn2.Length <> 0 Then
                            If Not TextFilesMatchIgnoreLine(strLineIn2, intLineIgnoreListCount, reLineIgnoreList) Then
                                blnFilesMatch = False
                                Exit Do
                            End If
                        End If
                    Loop While srFile2.Peek >= 0

                Else
                    ' Not ignoring whitespace; files don't match
                    blnFilesMatch = False
                End If
            End If


            srFile1.Close()
            srFile2.Close()

        Catch ex As Exception
            ' Error occurred
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error in TextFilesMatch" & ex.Message)
            blnFilesMatch = False
        End Try

        Return blnFilesMatch

    End Function

    Protected Function TextFilesMatchIgnoreLine(ByVal strText As String, ByVal intLineIgnoreListCount As Integer, ByRef reLineIgnoreList() As System.Text.RegularExpressions.Regex) As Boolean

        Dim intIndex As Integer
        Dim blnIgnoreLine As Boolean = False

        If Not reLineIgnoreList Is Nothing Then
            For intIndex = 0 To intLineIgnoreListCount - 1
                If Not reLineIgnoreList(intIndex) Is Nothing Then
                    If reLineIgnoreList(intIndex).Match(strText).Success Then
                        ' Line matches; ignore it
                        blnIgnoreLine = True
                        Exit For
                    End If
                End If
            Next
        End If

        Return blnIgnoreLine

    End Function

    ''' <summary>
    ''' Retrieves files necessary for performance of Sequest analysis
    ''' </summary>
    ''' <returns>IJobParams.CloseOutType indicating success or failure</returns>
    ''' <remarks></remarks>
    Public Overrides Function GetResources() As AnalysisManagerBase.IJobParams.CloseOutType

        Dim LocOrgDBFolder As String

        'Clear out list of files to delete or keep when packaging the results
        clsGlobal.ResetFilesToDeleteOrKeep()

        ' Look for existing .Out files in the Failed Results Folder; 
        ' if any are found, we'll post an entry to the log
        CheckForExistingOutFiles()

        'Retrieve Fasta file (we'll distribute it to the cluster nodes later in this function)
        LocOrgDBFolder = m_mgrParams.GetParam("orgdbdir")
        If Not RetrieveOrgDB(LocOrgDBFolder) Then Return IJobParams.CloseOutType.CLOSEOUT_FAILED

        'Retrieve param file
        If Not RetrieveGeneratedParamFile( _
         m_jobParams.GetParam("ParmFileName"), _
         m_jobParams.GetParam("ParmFileStoragePath"), _
         m_mgrParams.GetParam("workdir")) _
        Then Return IJobParams.CloseOutType.CLOSEOUT_FAILED

        ' Make sure the Sequest parameter file is present in the parameter file storage path
        ArchiveSequestParamFile()

        'Retrieve unzipped dta files
        If Not RetrieveDtaFiles(True) Then
            'Errors were reported in function call, so just return
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        ' If running on a cluster, then distribute the database file across the nodes
        ' We do this after we have successfully retrieved the DTA files and unzipped them
        If CBool(m_mgrParams.GetParam("cluster")) Then
            'Check the cluster nodes, updating local database copies as necessary
            If Not VerifyDatabase(m_jobParams.GetParam("generatedFastaName"), LocOrgDBFolder) Then
                'Errors were reported in function call, so just return
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

        End If

        'Add all the extensions of the files to delete after run
        clsGlobal.m_FilesToDeleteExt.Add("_dta.zip") 'Zipped DTA
        clsGlobal.m_FilesToDeleteExt.Add("_dta.txt") 'Unzipped, concatenated DTA
        clsGlobal.m_FilesToDeleteExt.Add(".dta")  'DTA files

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

        Dim HostFile As String = m_mgrParams.GetParam("hostsfilelocation")
        Dim Nodes As StringCollection
        Dim NodeDbLoc As String = m_mgrParams.GetParam("nodedblocation")

        Dim strLogMessage As String

        Dim blnFileAlreadyExists As Boolean
        Dim intNodeCountProcessed As Integer
        Dim intNodeCountFailed As Integer
        Dim intNodeCountFileAlreadyExists As Integer

        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Copying database to nodes: " & System.IO.Path.GetFileName(OrgDBName))

        'Get the list of nodes from the hosts file
        Nodes = GetHostList(HostFile)
        If Nodes Is Nothing Then
            If HostFile Is Nothing Then HostFile = ""
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Unable to determine node names from host file: " & HostFile)
            Return False
        End If


        ' Define the path to the database on the head node
        Dim OrgDBFilePath As String = System.IO.Path.Combine(OrgDBPath, OrgDBName)
        If Not File.Exists(OrgDBFilePath) Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Database file can't be found on master")
            Return False
        End If

        ' For each node, verify specified database file is present and matches file on host
        ' Allow up to 25% of the nodes to fail (they should just get skipped when the Sequest search occurs)

        blnFileAlreadyExists = False
        intNodeCountProcessed = 0
        intNodeCountFailed = 0
        intNodeCountFileAlreadyExists = 0

        For Each NodeName As String In Nodes
            If Not VerifyRemoteDatabase(OrgDBFilePath, "\\" & NodeName & "\" & NodeDbLoc, blnFileAlreadyExists) Then
                intNodeCountFailed += 1
                blnFileAlreadyExists = True
            End If

            intNodeCountProcessed += 1
            If blnFileAlreadyExists Then intNodeCountFileAlreadyExists += 1
        Next

        If intNodeCountProcessed = 0 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "The Nodes collection is empty; unable to continue")
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
    ''' <param name="HostFileNameLoc">Name of hosts file on cluster head node</param>
    ''' <returns>returns a string collection containing IP addresses for each node</returns>
    ''' <remarks></remarks>
    Private Function GetHostList(ByVal HostFileNameLoc As String) As StringCollection

        Dim NodeColl As New StringCollection
        Dim InpLine As String
        Dim LineFields() As String
        Dim Separators() As String = {" "}

        Try
            Dim HostFile As StreamReader = File.OpenText(HostFileNameLoc)
            InpLine = HostFile.ReadLine
            While Not InpLine Is Nothing

                'Read the line from the file and check to see if it contains a node IP address. If it does, add
                '	the IP address to the collection of addresses
                If InpLine.IndexOf("#") < 0 Then        'Verify the line isn't a comment line
                    'Parse the node name and add it to the collection
                    LineFields = InpLine.Split(Separators, StringSplitOptions.RemoveEmptyEntries)
                    If LineFields.GetLength(0) >= 1 Then
                        If NodeColl Is Nothing Then NodeColl = New StringCollection
                        NodeColl.Add(LineFields(0))
                    End If
                End If
                InpLine = HostFile.ReadLine
            End While
            HostFile.Close()
        Catch Err As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error reading cluster config file '" & HostFileNameLoc & "': " & Err.Message)
            Return Nothing
        End Try

        'Return the list of nodes, if any
        If NodeColl.Count < 1 Then
            Return Nothing
        Else
            Return NodeColl
        End If

    End Function

    Private Function VerifyFilesMatchSizeAndDate(ByVal FileA As String, ByVal FileB As String) As Boolean

        Const DETAILED_LOG_THRESHOLD As Integer = 3

        Dim blnFilesMatch As Boolean
        Dim ioFileA As System.IO.FileInfo
        Dim ioFileB As System.IO.FileInfo
        Dim dblSecondDiff As Double

        blnFilesMatch = False
        If System.IO.File.Exists(FileA) AndAlso System.IO.File.Exists(FileB) Then
            ' Files both exist
            ioFileA = New System.IO.FileInfo(FileA)
            ioFileB = New System.IO.FileInfo(FileB)

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
    ''' <returns>TRUE for success; FALSE for failure</returns>
    ''' <remarks>Assumes DestPath is URL containing IP address of node and destination share name</remarks>
    Private Function VerifyRemoteDatabase(ByVal OrgDBFilePath As String, ByVal DestPath As String, ByRef blnFileAlreadyExists As Boolean) As Boolean

        Dim CopyNeeded As Boolean = False

        blnFileAlreadyExists = False

        If m_DebugLevel > 3 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Verifying database " & DestPath)
        End If

        Dim DestFile As String = Path.Combine(DestPath, System.IO.Path.GetFileName(OrgDBFilePath))
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
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error copying database file to " & DestFile & "; " & Err.Message)
            Return False
        End Try

    End Function
#End Region

End Class
