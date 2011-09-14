'*********************************************************************************************************
' Written by John Sandoval for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2009, Battelle Memorial Institute
' Created 01/29/2009
'
' Last modified 06/15/2009 JDS - Added logging using log4net
'*********************************************************************************************************

Option Strict On

Imports AnalysisManagerBase
Imports PRISM.Files
Imports AnalysisManagerBase.clsGlobal

Public Class clsAnalysisToolRunnerInspResultsAssembly
    Inherits clsAnalysisToolRunnerBase

    '*********************************************************************************************************
    'Class for running Inspect Results Assembler
    '*********************************************************************************************************

#Region "Constants and Enums"
    Private Const PVALUE_MINLENGTH5_SCRIPT As String = "PValue_MinLength5.py"

    Private Const ORIGINAL_INSPECT_FILE_SUFFIX As String = "_inspect.txt"
    Private Const FIRST_HITS_INSPECT_FILE_SUFFIX As String = "_inspect_fht.txt"
    Private Const FILTERED_INSPECT_FILE_SUFFIX As String = "_inspect_filtered.txt"

    'Used for result file type
    Enum ResultFileType
        INSPECT_RESULT = 0
        INSPECT_ERROR = 1
        INSPECT_SEARCH = 2
        INSPECT_CONSOLE = 3
    End Enum

    ' Note: if you add/remove any steps, then update PERCENT_COMPLETE_LEVEL_COUNT and update the population of mPercentCompleteStartLevels()
    Enum eInspectResultsProcessingSteps
        Starting = 0
        AssembleResults = 1
        RunpValue = 2
        ZipInspectResults = 3
        CreatePeptideToProteinMapping = 4
    End Enum

#End Region

#Region "Structures"
    Protected Structure udtModInfoType
        Public ModName As String
        Public ModMass As String           ' Storing as a string since reading from a text file and writing to a text file
        Public Residues As String
    End Structure
#End Region

#Region "Module Variables"
    Public Const INSPECT_INPUT_PARAMS_FILENAME As String = "inspect_input.txt"

    Protected mInspectResultsFileName As String

    Protected mInspectSearchLogFilePath As String = "InspectSearchLog.txt"      ' This value gets updated in function RunInSpecT

    Private WithEvents mPeptideToProteinMapper As PeptideToProteinMapEngine.clsPeptideToProteinMapEngine

    ' mPercentCompleteStartLevels is an array that lists the percent complete value to report 
    '  at the start of each of the various processing steps performed in this procedure
    ' The percent complete values range from 0 to 100
    Const PERCENT_COMPLETE_LEVEL_COUNT As Integer = 5
    Protected mPercentCompleteStartLevels() As Single

#End Region

#Region "Methods"
    ''' <summary>
    ''' Constructor
    ''' </summary>
    ''' <remarks>Initializes classwide variables</remarks>
    Public Sub New()
        InitializeVariables()
    End Sub

    ''' <summary>
    ''' Runs InSpecT tool
    ''' </summary>
    ''' <returns>CloseOutType enum indicating success or failure</returns>
    ''' <remarks></remarks>
    Public Overrides Function RunTool() As IJobParams.CloseOutType

        Dim numClonedSteps As String
        Dim intNumResultFiles As Integer

        Dim isParallelized As Boolean = False

        Dim Result As IJobParams.CloseOutType
        Dim eReturnCode As IJobParams.CloseOutType

        Dim blnProcessingError As Boolean

        ' We no longer need to index the .Fasta file (since we're no longer using PValue.py with the -a switch or Summary.py
        ''Dim objIndexedDBCreator As New clsCreateInspectIndexedDB

        Try
            If m_DebugLevel > 4 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerInspResultsAssembly.RunTool(): Enter")
            End If

            'Call base class for initial setup
            MyBase.RunTool()

            ' Store the AnalysisManager version info in the database
            StoreToolVersionInfo()

            'Determine if this is a parallelized job
            numClonedSteps = m_jobParams.GetParam("NumberOfClonedSteps")
            If [String].IsNullOrEmpty(numClonedSteps) Then
                ' This is not a parallelized job; no need to assemble the results

                ' FilterInspectResultsByFirstHits will create file _inspect_fht.txt
                Result = FilterInspectResultsByFirstHits()

                ' FilterInspectResultsByPValue will create file _inspect_filtered.txt
                Result = FilterInspectResultsByPValue()
                If Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                    blnProcessingError = True
                End If
                isParallelized = False
            Else
                ' This is a parallelized job; need to re-assemble the results
                intNumResultFiles = CInt(numClonedSteps)

                If m_DebugLevel >= 1 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Assembling parallelized inspect files; file count = " & intNumResultFiles.ToString)
                End If

                ' AssembleResults will create _inspect.txt, _inspect_fht.txt, and _inspect_filtered.txt
                Result = AssembleResults(intNumResultFiles)

                If Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                    blnProcessingError = True
                End If
                isParallelized = True
            End If

            If Not blnProcessingError Then
                ' Rename and zip up files _inspect_filtered.txt and _inspect.txt
                Result = ZipInspectResults()
                If Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                    blnProcessingError = True
                End If
            End If

            If Not blnProcessingError Then
                ' Create the Peptide to Protein map file
                Result = CreatePeptideToProteinMapping()
                If Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS And Result <> IJobParams.CloseOutType.CLOSEOUT_NO_DATA Then
                    blnProcessingError = True
                End If
            End If

            UpdateStatusRunning(100)

            'Stop the job timer
            m_StopTime = System.DateTime.Now

            If blnProcessingError Then
                ' Something went wrong
                ' In order to help diagnose things, we will move whatever files were created into the Result folder, 
                '  archive it using CopyFailedResultsToArchiveFolder, then return IJobParams.CloseOutType.CLOSEOUT_FAILED
                eReturnCode = IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            'Add the current job data to the summary file
            If Not UpdateSummaryFile() Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Error creating summary file, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step"))
            End If

            'Make sure objects are released
            System.Threading.Thread.Sleep(2000)        '2 second delay
            GC.Collect()
            GC.WaitForPendingFinalizers()

            Result = MakeResultsFolder()
            If Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                'MakeResultsFolder handles posting to local log, so set database error message and exit
                m_message = "Error making results folder"
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            Result = MoveResultFiles()
            If Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                'MoveResultFiles moves the Result files to the Result folder
                m_message = "Error moving files into results folder"
                eReturnCode = IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            If blnProcessingError Or eReturnCode = IJobParams.CloseOutType.CLOSEOUT_FAILED Then
                ' Try to save whatever files were moved into the results folder
                Dim objAnalysisResults As clsAnalysisResults = New clsAnalysisResults(m_mgrParams, m_jobParams)
                objAnalysisResults.CopyFailedResultsToArchiveFolder(System.IO.Path.Combine(m_WorkDir, m_ResFolderName))

                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            Result = CopyResultsFolderToServer()
            If Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                'TODO: What do we do here?
                Return Result
            End If

            'If parallelized, then remove multiple Result files from server
            If isParallelized Then
                If Not clsGlobal.RemoveNonResultServerFiles(m_DebugLevel) Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Error deleting non Result files from directory on server, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step"))
                    Return IJobParams.CloseOutType.CLOSEOUT_FAILED
                End If
            End If

        Catch ex As Exception
            Dim Msg As String
            Msg = "clsMSGFToolRunner.RunTool(); Exception during Inspect Results Assembly: " & _
                ex.Message & "; " & clsGlobal.GetExceptionStackTrace(ex)
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
            m_message = AnalysisManagerBase.clsGlobal.AppendToComment(m_message, "Exception during Inspect Results Assembly")
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS 'No failures so everything must have succeeded

    End Function

    ''' <summary>
    ''' Initializes class
    ''' </summary>
    ''' <param name="mgrParams">Object containing manager parameters</param>
    ''' <param name="jobParams">Object containing job parameters</param>
    ''' <param name="StatusTools">Object for updating status file as job progresses</param>
    ''' <remarks></remarks>
    Public Overrides Sub Setup(ByVal mgrParams As IMgrParams, ByVal jobParams As IJobParams, _
      ByVal StatusTools As IStatusFile)

        MyBase.Setup(mgrParams, jobParams, StatusTools)

        If m_DebugLevel > 3 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerInspResultsAssembly.Setup()")
        End If

        mInspectResultsFileName = m_Dataset & ORIGINAL_INSPECT_FILE_SUFFIX

    End Sub

    Private Function AssembleResults(ByVal intNumResultFiles As Integer) As IJobParams.CloseOutType
        Dim result As IJobParams.CloseOutType
        Dim strFileName As String = ""

        Try
            If m_DebugLevel >= 3 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Assembling parallelized inspect result files")
            End If

            UpdateStatusRunning(mPercentCompleteStartLevels(eInspectResultsProcessingSteps.AssembleResults))

            ' Combine the individual _xx_inspect.txt files to create the single _inspect.txt file
            result = AssembleFiles(mInspectResultsFileName, ResultFileType.INSPECT_RESULT, intNumResultFiles)
            If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                Return result
            End If

            If m_DebugLevel >= 3 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Assembling parallelized inspect error files")
            End If

            strFileName = m_Dataset & "_error.txt"
            result = AssembleFiles(strFileName, ResultFileType.INSPECT_ERROR, intNumResultFiles)
            If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                Return result
            End If
            clsGlobal.m_ExceptionFiles.Add(strFileName)


            If m_DebugLevel >= 3 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Assembling parallelized inspect search log files")
            End If

            strFileName = "InspectSearchLog.txt"
            result = AssembleFiles(strFileName, ResultFileType.INSPECT_SEARCH, intNumResultFiles)
            If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                Return result
            End If
            clsGlobal.m_ExceptionFiles.Add(strFileName)


            If m_DebugLevel >= 3 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Assembling parallelized inspect console output files")
            End If

            strFileName = "InspectConsoleOutput.txt"
            result = AssembleFiles(strFileName, ResultFileType.INSPECT_CONSOLE, intNumResultFiles)
            If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                Return result
            End If
            clsGlobal.m_ExceptionFiles.Add(strFileName)


            ' FilterInspectResultsByFirstHits will create file _inspect_fht.txt
            result = FilterInspectResultsByFirstHits()

            ' Rescore the assembled inspect results using PValue_MinLength5.py (which is similar to PValue.py but retains peptides of length 5 or greater)
            ' This will create files _inspect_fht.txt and _inspect_filtered.txt
            result = RescoreAssembledInspectResults()
            If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                Return result
            End If

        Catch ex As Exception
            m_message = "Error in InspectResultsAssembly->AssembleResults"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    ''' <summary>
    ''' Assemble the result files
    ''' </summary>
    ''' <returns>CloseOutType enum indicating success or failure</returns>
    ''' <remarks></remarks>

    Private Function AssembleFiles(ByVal strCombinedFileName As String, _
                                   ByVal resFileType As ResultFileType, _
                                   ByVal intNumResultFiles As Integer) As IJobParams.CloseOutType

        Dim tr As System.IO.StreamReader = Nothing
        Dim tw As System.IO.StreamWriter

        Dim s As String
        Dim DatasetName As String
        Dim fileNameCounter As Integer
        Dim InspectResultsFile As String = ""
        Dim intLinesRead As Integer

        Dim blnFilesContainHeaderLine As Boolean
        Dim blnHeaderLineWritten As Boolean
        Dim blnAddSegmentNumberToEachLine As Boolean
        Dim blnAddBlankLineBetweenFiles As Boolean

        Dim intTabIndex As Integer
        Dim intSlashIndex As Integer

        Try
            DatasetName = m_Dataset

            tw = CreateNewExportFile(System.IO.Path.Combine(m_WorkDir, strCombinedFileName))
            If tw Is Nothing Then
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            For fileNameCounter = 1 To intNumResultFiles
                Select Case resFileType
                    Case ResultFileType.INSPECT_RESULT
                        InspectResultsFile = DatasetName & "_" & fileNameCounter & ORIGINAL_INSPECT_FILE_SUFFIX
                        blnFilesContainHeaderLine = True
                        blnAddSegmentNumberToEachLine = False
                        blnAddBlankLineBetweenFiles = False

                    Case ResultFileType.INSPECT_ERROR
                        InspectResultsFile = DatasetName & "_" & fileNameCounter & "_error.txt"
                        blnFilesContainHeaderLine = False
                        blnAddSegmentNumberToEachLine = True
                        blnAddBlankLineBetweenFiles = False

                    Case ResultFileType.INSPECT_SEARCH
                        InspectResultsFile = "InspectSearchLog_" & fileNameCounter & ".txt"
                        blnFilesContainHeaderLine = True
                        blnAddSegmentNumberToEachLine = True
                        blnAddBlankLineBetweenFiles = False

                    Case ResultFileType.INSPECT_CONSOLE
                        InspectResultsFile = "InspectConsoleOutput_" & fileNameCounter & ".txt"
                        blnFilesContainHeaderLine = False
                        blnAddSegmentNumberToEachLine = False
                        blnAddBlankLineBetweenFiles = True

                    Case Else
                        ' Unknown ResultFileType
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerInspResultsAssembly->AssembleFiles: Unknown Inspect Result File Type: " & resFileType.ToString)
                        Exit For
                End Select

                If System.IO.File.Exists(System.IO.Path.Combine(m_WorkDir, InspectResultsFile)) Then
                    intLinesRead = 0

                    tr = New System.IO.StreamReader(New System.IO.FileStream(System.IO.Path.Combine(m_WorkDir, InspectResultsFile), System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read))
                    s = tr.ReadLine

                    Do While s IsNot Nothing
                        intLinesRead += 1
                        If intLinesRead = 1 Then

                            If blnFilesContainHeaderLine Then
                                ' Handle the header line
                                If Not blnHeaderLineWritten Then
                                    If blnAddSegmentNumberToEachLine Then
                                        s = "Segment" & ControlChars.Tab & s
                                    End If
                                    tw.WriteLine(s)
                                End If
                            Else
                                If blnAddSegmentNumberToEachLine Then
                                    If Not blnHeaderLineWritten Then
                                        tw.WriteLine("Segment" & ControlChars.Tab & "Message")
                                    End If
                                    tw.WriteLine(fileNameCounter.ToString & ControlChars.Tab & s)
                                Else
                                    tw.WriteLine(s)
                                End If
                            End If
                            blnHeaderLineWritten = True

                        Else
                            If resFileType = ResultFileType.INSPECT_RESULT Then
                                ' Parse each line of the Inspect Results files to remove the folder path information from the first column
                                Try
                                    intTabIndex = s.IndexOf(ControlChars.Tab)
                                    If intTabIndex > 0 Then
                                        ' Note: .LastIndexOf will start at index intTabIndex and search backword until the first match is found (this is a bit counter-intuitive, but that's what it does)
                                        intSlashIndex = s.LastIndexOf(System.IO.Path.DirectorySeparatorChar, intTabIndex)
                                        If intSlashIndex > 0 Then
                                            s = s.Substring(intSlashIndex + 1)
                                        End If
                                    End If
                                Catch ex As Exception
                                    ' Ignore errors here
                                End Try
                            End If

                            If blnAddSegmentNumberToEachLine Then
                                tw.WriteLine(fileNameCounter.ToString & ControlChars.Tab & s)
                            Else
                                tw.WriteLine(s)
                            End If
                        End If

                        ' Read the next line
                        s = tr.ReadLine
                    Loop
                    tr.Close()

                    If blnAddBlankLineBetweenFiles Then
                        Console.WriteLine()
                    End If
                End If
            Next

            'close the main result file
            tw.Close()

        Catch ex As Exception
            m_message = "Error in InspectResultsAssembly->AssembleFiles"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    Private Function CreateNewExportFile(ByVal exportFilePath As String) As System.IO.StreamWriter
        Dim ef As System.IO.StreamWriter

        If System.IO.File.Exists(exportFilePath) Then
            'post error to log
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerInspResultsAssembly->createNewExportFile: Export file already exists (" & exportFilePath & "); this is unexpected")
            Return Nothing
        End If

        ef = New System.IO.StreamWriter(New System.IO.FileStream(exportFilePath, System.IO.FileMode.Create, System.IO.FileAccess.Write, System.IO.FileShare.Read))
        Return ef

    End Function

    Private Function CreatePeptideToProteinMapping() As IJobParams.CloseOutType

        Dim OrgDbDir As String = m_mgrParams.GetParam("orgdbdir")

        ' Note that job parameter "generatedFastaName" gets defined by clsAnalysisResources.RetrieveOrgDB
        Dim dbFilename As String = System.IO.Path.Combine(OrgDbDir, m_jobParams.GetParam("generatedFastaName"))
        Dim strInputFilePath As String

        Dim blnIgnorePeptideToProteinMapperErrors As Boolean
        Dim blnSuccess As Boolean

        UpdateStatusRunning(mPercentCompleteStartLevels(eInspectResultsProcessingSteps.CreatePeptideToProteinMapping))

        strInputFilePath = System.IO.Path.Combine(m_WorkDir, mInspectResultsFileName)

        Try
            ' Validate that the input file has at least one entry; if not, then no point in continuing
            Dim srInFile As System.IO.StreamReader
            Dim strLineIn As String
            Dim intLinesRead As Integer

            srInFile = New System.IO.StreamReader(New System.IO.FileStream(strInputFilePath, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.Read))

            intLinesRead = 0
            Do While srInFile.Peek >= 0 AndAlso intLinesRead < 10
                strLineIn = srInFile.ReadLine()
                If Not String.IsNullOrEmpty(strLineIn) Then
                    intLinesRead += 1
                End If
            Loop

            If intLinesRead <= 1 Then
                ' File is empty or only contains a header line
                m_message = "No results above threshold"
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "No results above threshold; filtered inspect results file is empty")
                Return IJobParams.CloseOutType.CLOSEOUT_NO_DATA
            End If

        Catch ex As Exception

            m_message = "Error validating Inspect results file contents in InspectResultsAssembly->CreatePeptideToProteinMapping"

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ", job " & _
                m_JobNum & "; " & clsGlobal.GetExceptionStackTrace(ex))
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED

        End Try

        Try
            If m_DebugLevel >= 1 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Creating peptide to protein map file")
            End If

            blnIgnorePeptideToProteinMapperErrors = AnalysisManagerBase.clsGlobal.CBoolSafe(m_jobParams.GetParam("IgnorePeptideToProteinMapError"))

            mPeptideToProteinMapper = New PeptideToProteinMapEngine.clsPeptideToProteinMapEngine

            With mPeptideToProteinMapper
                .DeleteInspectTempFiles = True
                .IgnoreILDifferences = False
                .InspectParameterFilePath = System.IO.Path.Combine(m_WorkDir, INSPECT_INPUT_PARAMS_FILENAME)

                If m_DebugLevel > 2 Then
                    .LogMessagesToFile = True
                    .LogFolderPath = m_WorkDir
                Else
                    .LogMessagesToFile = False
                End If

                .MatchPeptidePrefixAndSuffixToProtein = False
                .OutputProteinSequence = False
                .PeptideInputFileFormat = PeptideToProteinMapEngine.clsPeptideToProteinMapEngine.ePeptideInputFileFormatConstants.InspectResultsFile
                .PeptideFileSkipFirstLine = False
                .ProteinInputFilePath = System.IO.Path.Combine(OrgDbDir, dbFilename)
                .SaveProteinToPeptideMappingFile = True
                .SearchAllProteinsForPeptideSequence = True
                .SearchAllProteinsSkipCoverageComputationSteps = True
                .ShowMessages = False
            End With

            blnSuccess = mPeptideToProteinMapper.ProcessFile(strInputFilePath, m_WorkDir, String.Empty, True)

            mPeptideToProteinMapper.CloseLogFileNow()

            If blnSuccess Then
                If m_DebugLevel >= 2 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Peptide to protein mapping complete")
                End If
            Else
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error running clsPeptideToProteinMapEngine: " & mPeptideToProteinMapper.GetErrorMessage())

                If blnIgnorePeptideToProteinMapperErrors Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Ignoring protein mapping error since 'IgnorePeptideToProteinMapError' = True")
                    Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
                Else
                    Return IJobParams.CloseOutType.CLOSEOUT_FAILED
                End If
            End If

        Catch ex As Exception

            m_message = "Error in InspectResultsAssembly->CreatePeptideToProteinMapping"

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerInspResultsAssembly.CreatePeptideToProteinMapping, Error running clsPeptideToProteinMapEngine, job " & _
                m_JobNum & "; " & clsGlobal.GetExceptionStackTrace(ex))

            If blnIgnorePeptideToProteinMapperErrors Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Ignoring protein mapping error since 'IgnorePeptideToProteinMapError' = True")
                Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
            Else
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If
        End Try

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    ''' <summary>
    ''' Reads the modification information defined in strInspectParameterFilePath, storing it in udtModList
    ''' </summary>
    ''' <param name="strInspectParameterFilePath"></param>
    ''' <param name="udtModList"></param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Private Function ExtractModInfoFromInspectParamFile(ByVal strInspectParameterFilePath As String, ByRef udtModList() As udtModInfoType) As Boolean

        Dim srInFile As System.IO.StreamReader

        Dim strLineIn As String
        Dim strSplitLine As String()

        Dim intModCount As Integer

        Try
            ' Initialize udtModList
            intModCount = 0
            ReDim udtModList(-1)

            If m_DebugLevel > 4 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerInspResultsAssembly.ExtractModInfoFromInspectParamFile(): Reading " & strInspectParameterFilePath)
            End If

            ' Read the contents of strProteinToPeptideMappingFilePath
            srInFile = New System.IO.StreamReader((New System.IO.FileStream(strInspectParameterFilePath, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read)))

            Do While srInFile.Peek <> -1
                strLineIn = srInFile.ReadLine

                strLineIn = strLineIn.Trim

                If strLineIn.Length > 0 Then

                    If strLineIn.Chars(0) = "#"c Then
                        ' Comment line; skip it
                    ElseIf strLineIn.ToLower.StartsWith("mod") Then
                        ' Modification definition line

                        ' Split the line on commas
                        strSplitLine = strLineIn.Split(","c)

                        If strSplitLine.Length >= 5 AndAlso strSplitLine(0).ToLower.Trim = "mod" Then
                            If udtModList.Length = 0 Then
                                ReDim udtModList(0)
                            ElseIf intModCount >= udtModList.Length Then
                                ReDim Preserve udtModList(udtModList.Length * 2 - 1)
                            End If

                            With udtModList(intModCount)
                                .ModName = strSplitLine(4)
                                .ModMass = strSplitLine(1)
                                .Residues = strSplitLine(2)
                            End With

                            intModCount += 1
                        End If
                    End If
                End If
            Loop

            ' Shrink udtModList to the appropriate length
            ReDim Preserve udtModList(intModCount - 1)

            Console.WriteLine()

        Catch ex As Exception
            m_message = "Error in InspectResultsAssembly->ExtractModInfoFromInspectParamFile"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message)
            Return False
        Finally
            If Not srInFile Is Nothing Then
                srInFile.Close()
            End If
        End Try

        Return True

    End Function

    ''' <summary>
    ''' Use PValue_MinLength5.py to only retain the top hit for each scan (no p-value filtering is actually applied)
    ''' </summary>
    ''' <returns>CloseOutType enum indicating success or failure</returns>
    ''' <remarks></remarks>
    Private Function FilterInspectResultsByFirstHits() As IJobParams.CloseOutType

        Dim eResult As IJobParams.CloseOutType

        Dim strInspectResultsFilePath As String = System.IO.Path.Combine(m_WorkDir, mInspectResultsFileName)
        Dim strFilteredFilePath As String = System.IO.Path.Combine(m_WorkDir, m_Dataset & FIRST_HITS_INSPECT_FILE_SUFFIX)

        UpdateStatusRunning(mPercentCompleteStartLevels(eInspectResultsProcessingSteps.RunpValue))

        ' Note that RunPvalue() will log any errors that occur
        eResult = RunpValue(strInspectResultsFilePath, strFilteredFilePath, False, True)

        Return eResult

    End Function

    ''' <summary>
    ''' Filters the inspect results using PValue_MinLength5.py"
    ''' </summary>
    ''' <returns>CloseOutType enum indicating success or failure</returns>
    ''' <remarks></remarks>
    Private Function FilterInspectResultsByPValue() As IJobParams.CloseOutType

        Dim eResult As IJobParams.CloseOutType

        Dim strInspectResultsFilePath As String = System.IO.Path.Combine(m_WorkDir, mInspectResultsFileName)
        Dim strFilteredFilePath As String = System.IO.Path.Combine(m_WorkDir, m_Dataset & FILTERED_INSPECT_FILE_SUFFIX)

        UpdateStatusRunning(mPercentCompleteStartLevels(eInspectResultsProcessingSteps.RunpValue))

        ' Note that RunPvalue() will log any errors that occur
        eResult = RunpValue(strInspectResultsFilePath, strFilteredFilePath, True, False)

        Return eResult

    End Function

    Protected Sub InitializeVariables()

        ' Define the percent complete values to use for the start of each processing step

        ReDim mPercentCompleteStartLevels(PERCENT_COMPLETE_LEVEL_COUNT)

        mPercentCompleteStartLevels(eInspectResultsProcessingSteps.Starting) = 0
        mPercentCompleteStartLevels(eInspectResultsProcessingSteps.AssembleResults) = 5
        mPercentCompleteStartLevels(eInspectResultsProcessingSteps.RunpValue) = 10
        mPercentCompleteStartLevels(eInspectResultsProcessingSteps.ZipInspectResults) = 65
        mPercentCompleteStartLevels(eInspectResultsProcessingSteps.CreatePeptideToProteinMapping) = 66
        mPercentCompleteStartLevels(PERCENT_COMPLETE_LEVEL_COUNT) = 100

    End Sub

    Private Function RenameAndZipInspectFile(ByVal strSourceFilePath As String, _
                                             ByVal strZipFilePath As String, _
                                             ByVal blnDeleteSourceAfterZip As Boolean) As Boolean

        Dim fiFileInfo As System.IO.FileInfo
        Dim strTargetFilePath As String
        Dim blnSuccess As Boolean

        ' Zip up file specified by strSourceFilePath
        ' Rename to _inspect.txt before zipping
        fiFileInfo = New System.IO.FileInfo(strSourceFilePath)

        If Not fiFileInfo.Exists Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Inspect results file not found; nothing to zip: " & fiFileInfo.FullName)
            Return False
        End If

        strTargetFilePath = System.IO.Path.Combine(m_WorkDir, mInspectResultsFileName)
        If m_DebugLevel >= 3 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Renaming " & fiFileInfo.FullName & " to " & strTargetFilePath)
        End If

        fiFileInfo.MoveTo(strTargetFilePath)
        fiFileInfo.Refresh()

        blnSuccess = MyBase.ZipFile(fiFileInfo.FullName, blnDeleteSourceAfterZip, strZipFilePath)

        clsGlobal.m_ExceptionFiles.Add(System.IO.Path.GetFileName(strZipFilePath))

        Return blnSuccess

    End Function

    ''' <summary>
    ''' Uses PValue_MinLength5.py to recompute the p-value and FScore values for Inspect results computed in parallel then reassembled
    ''' In addition, filters the data on p-value of 0.1 or smaller
    ''' </summary>
    ''' <returns>CloseOutType enum indicating success or failure</returns>
    ''' <remarks></remarks>
    Private Function RescoreAssembledInspectResults() As IJobParams.CloseOutType

        Dim eResult As IJobParams.CloseOutType

        Dim strInspectResultsFilePath As String = System.IO.Path.Combine(m_WorkDir, mInspectResultsFileName)
        Dim strFilteredFilePath As String = System.IO.Path.Combine(m_WorkDir, m_Dataset & FILTERED_INSPECT_FILE_SUFFIX)

        UpdateStatusRunning(mPercentCompleteStartLevels(eInspectResultsProcessingSteps.RunpValue))

        ' Note that RunPvalue() will log any errors that occur
        eResult = RunpValue(strInspectResultsFilePath, strFilteredFilePath, True, False)

        Try
            ' Make sure the filtered inspect results file is not zero-length
            ' Also, log some stats on the size of the filtered file vs. the original one

            Dim fiOriginalFile As System.IO.FileInfo
            Dim fiRescoredFile As System.IO.FileInfo

            fiRescoredFile = New System.IO.FileInfo(strFilteredFilePath)
            fiOriginalFile = New System.IO.FileInfo(strInspectResultsFilePath)

            If Not fiRescoredFile.Exists Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Rescored Inspect Results file not found: " & fiRescoredFile.FullName)
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            If fiOriginalFile.Length = 0 Then
                ' Assembled inspect results file is 0-bytes; this is unexpected
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Assembled Inspect Results file is 0 bytes: " & fiOriginalFile.FullName)
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            If m_DebugLevel >= 1 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Rescored Inspect results file created; size is " & (fiRescoredFile.Length / CDbl(fiOriginalFile.Length) * 100).ToString("0.0") & "% of the original (" & fiRescoredFile.Length & " bytes vs. " & fiOriginalFile.Length & " bytes in original)")
            End If

        Catch ex As Exception
            m_message = "Error in InspectResultsAssembly->RescoreAssembledInspectResults"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    Private Function RunpValue(ByVal strInspectResultsInputFilePath As String, _
                               ByVal strOutputFilePath As String, _
                               ByVal blnCreateImageFiles As Boolean, _
                               ByVal blnTopHitOnly As Boolean) As IJobParams.CloseOutType

        Dim CmdRunner As clsRunDosProgram
        Dim CmdStr As String

        Dim InspectDir As String = m_mgrParams.GetParam("inspectdir")
        Dim pvalDistributionFilename As String = System.IO.Path.Combine(m_WorkDir, m_Dataset & "_PValueDistribution.txt")

        ' The following code is only required if you use the -a and -d switches
        ''Dim orgDbDir As String = m_mgrParams.GetParam("orgdbdir")
        ''Dim fastaFilename As String = System.IO.Path.Combine(orgDbDir, m_jobParams.GetParam("generatedFastaName"))
        ''Dim dbFilename As String = fastaFilename.Replace("fasta", "trie")

        Dim pythonProgLoc As String = m_mgrParams.GetParam("pythonprogloc")
        Dim pthresh As String = ""

        Dim blnShuffledDBUsed As Boolean

        ' Check whether a shuffled DB was created prior to running Inspect
        blnShuffledDBUsed = ValidateShuffledDBInUse(strInspectResultsInputFilePath)

        ' Lookup the p-value to filter on
        pthresh = AnalysisManagerBase.clsGlobal.GetJobParameter(m_jobParams, "InspectPvalueThreshold", "0.1")

        CmdRunner = New clsRunDosProgram(InspectDir)

        If m_DebugLevel > 4 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerInspResultsAssembly.RunpValue(): Enter")
        End If

        ' verify that python program file exists
        Dim progLoc As String = pythonProgLoc
        If Not System.IO.File.Exists(progLoc) Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Cannot find python.exe program file: " & progLoc)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        ' verify that PValue python script exists
        Dim pvalueScriptPath As String = System.IO.Path.Combine(InspectDir, PVALUE_MINLENGTH5_SCRIPT)
        If Not System.IO.File.Exists(pvalueScriptPath) Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Cannot find PValue script: " & pvalueScriptPath)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        ' Possibly required: Update the PTMods.txt file in InspectDir to contain the modification details, as defined in inspect_input.txt
        UpdatePTModsFile(InspectDir, System.IO.Path.Combine(m_WorkDir, "inspect_input.txt"))

        'Set up and execute a program runner to run PVALUE_MINLENGTH5_SCRIPT.py
        ' Note that PVALUE_MINLENGTH5_SCRIPT.py is nearly identical to PValue.py but it retains peptides with 5 amino acids (default is 7)
        ' -r is the input file
        ' -w is the output file
        ' -s saves the p-value distribution to a text file
        ' -H means to not remove entries mapped to shuffled proteins (created by shuffleDB.py); shuffled protein names start with XXX; we use this option when creating the First Hits file so that we retain the top hit, even if it is from a shuffled protein
        ' -p 0.1 will filter out results with p-value <= 0.1 (this threshold was suggested by Sam Payne)
        ' -i means to create a PValue distribution image file (.PNG)
        ' -S 0.5 means that 50% of the proteins in the DB come from shuffled proteins

        ' Other parameters not used:
        ' -1 means to only retain the top match for each scan
        ' -x means to retain "bad" matches (those with poor delta-score values, a p-value below the threshold, or an MQScore below -1)
        ' -a means to perform protein selection (sort of like protein prophet, but not very good, according to Sam Payne)
        ' -d .trie file to use (only used if -a is enabled)

        CmdStr = " " & pvalueScriptPath & _
                 " -r " & strInspectResultsInputFilePath & _
                 " -w " & strOutputFilePath & _
                 " -s " & pvalDistributionFilename

        If blnCreateImageFiles Then
            CmdStr &= " -i"
        End If

        If blnTopHitOnly Then
            CmdStr &= " -H -1 -p 1"
        Else
            CmdStr &= " -p " & pthresh
        End If

        If blnShuffledDBUsed Then
            CmdStr &= " -S 0.5"
        End If

        ' The following could be used to enable protein selection
        ' That would require that the database file be present, and this can take quite a bit longer
        ''CmdStr &= " -a -d " & dbFilename


        If m_DebugLevel >= 1 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, progLoc & " " & CmdStr)
        End If

        With CmdRunner
            .CreateNoWindow = True
            .CacheStandardOutput = True
            .EchoOutputToConsole = True

            .WriteConsoleOutputToFile = True
            .ConsoleOutputFilePath = System.IO.Path.Combine(m_WorkDir, "PValue_ConsoleOutput.txt")
        End With

        If Not CmdRunner.RunProgram(progLoc, CmdStr, "PValue", False) Then
            ' Error running program; the error should have already been logged
        End If

        If CmdRunner.ExitCode <> 0 Then
            ' Note: Log the non-zero exit code as an error, but return CLOSEOUT_SUCCESS anyway
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, System.IO.Path.GetFileName(pvalueScriptPath) & " returned a non-zero exit code: " & CmdRunner.ExitCode.ToString)
        End If

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    ''' <summary>
    ''' Stores the tool version info in the database
    ''' </summary>
    ''' <remarks></remarks>
    Protected Function StoreToolVersionInfo() As Boolean

        Dim strToolVersionInfo As String = String.Empty
        Dim ioAppFileInfo As System.IO.FileInfo = New System.IO.FileInfo(System.Reflection.Assembly.GetExecutingAssembly().Location)

        If m_DebugLevel >= 2 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info")
        End If

        ' Lookup the version of the Analysis Manager
        Try
            Dim oAssemblyName As System.Reflection.AssemblyName
            oAssemblyName = System.Reflection.Assembly.Load("AnalysisManagerInspResultsAssemblyPlugIn").GetName

            Dim strNameAndVersion As String
            strNameAndVersion = oAssemblyName.Name & ", Version=" & oAssemblyName.Version.ToString()
            strToolVersionInfo = clsGlobal.AppendToComment(strToolVersionInfo, strNameAndVersion)

        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception determining Assembly info for AnalysisManagerInspResultsAssemblyPlugIn: " & ex.Message)
        End Try

        ' Store version information for the PeptideToProteinMapEngine and its associated DLLs
        MyBase.StoreToolVersionInfoOneFile(strToolVersionInfo, System.IO.Path.Combine(ioAppFileInfo.DirectoryName, "PeptideToProteinMapEngine.dll"))
        MyBase.StoreToolVersionInfoOneFile(strToolVersionInfo, System.IO.Path.Combine(ioAppFileInfo.DirectoryName, "ProteinFileReader.dll"))
        MyBase.StoreToolVersionInfoOneFile(strToolVersionInfo, System.IO.Path.Combine(ioAppFileInfo.DirectoryName, "System.Data.SQLite.dll"))
        MyBase.StoreToolVersionInfoOneFile(strToolVersionInfo, System.IO.Path.Combine(ioAppFileInfo.DirectoryName, "ProteinCoverageSummarizer.dll"))

        ' Store the path to important DLLs in ioToolFiles
        Dim ioToolFiles As New System.Collections.Generic.List(Of System.IO.FileInfo)
        ioToolFiles.Add(New System.IO.FileInfo(System.IO.Path.Combine(ioAppFileInfo.DirectoryName, "AnalysisManagerInspResultsAssemblyPlugIn.dll")))
        ioToolFiles.Add(New System.IO.FileInfo(System.IO.Path.Combine(ioAppFileInfo.DirectoryName, "PeptideToProteinMapEngine.dll")))
        ioToolFiles.Add(New System.IO.FileInfo(System.IO.Path.Combine(ioAppFileInfo.DirectoryName, "ProteinFileReader.dll")))
        ' Skip System.Data.SQLite.dll; we don't need to track the file date
        ioToolFiles.Add(New System.IO.FileInfo(System.IO.Path.Combine(ioAppFileInfo.DirectoryName, "ProteinCoverageSummarizer.dll")))

        Try
            Return MyBase.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles)
        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion: " & ex.Message)
            Return False
        End Try

    End Function

    ''' <summary>
    ''' Assures that the PTMods.txt file in strInspectDir contains the modification info defined in strInspectInputFilePath
    ''' Note: We run the risk that two InspectResultsAssembly tasks will run simultaneously and will both try to update PTMods.txt
    ''' </summary>
    ''' <param name="strInspectDir"></param>
    ''' <param name="strInspectParameterFilePath"></param>
    ''' <remarks></remarks>
    Private Function UpdatePTModsFile(ByVal strInspectDir As String, ByVal strInspectParameterFilePath As String) As Boolean

        Dim srInFile As System.IO.StreamReader
        Dim swOutFile As System.IO.StreamWriter

        Dim intIndex As Integer

        Dim strPTModsFilePath As String
        Dim strPTModsFilePathOld As String
        Dim strPTModsFilePathNew As String

        Dim strLineIn As String
        Dim strSplitLine() As String
        Dim strModName As String

        Dim udtModList() As udtModInfoType
        ReDim udtModList(-1)

        Dim blnModProcessed() As Boolean

        Dim blnMatchFound As Boolean
        Dim blnPrevLineWasBlank As Boolean
        Dim blnDifferenceFound As Boolean

        Try
            If m_DebugLevel > 4 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerInspResultsAssembly.UpdatePTModsFile(): Enter ")
            End If

            ' Read the mods defined in strInspectInputFilePath
            If ExtractModInfoFromInspectParamFile(strInspectParameterFilePath, udtModList) Then

                If udtModList.Length > 0 Then

                    ' Initialize blnModProcessed()
                    ReDim blnModProcessed(udtModList.Length - 1)

                    ' Read PTMods.txt to look for the mods in udtModList
                    ' While reading, will create a new file with any required updates
                    ' In case two managers are doing this simultaneously, we'll put a unique string in strPTModsFilePathNew

                    strPTModsFilePath = System.IO.Path.Combine(strInspectDir, "PTMods.txt")
                    strPTModsFilePathNew = strPTModsFilePath & ".Job" & m_JobNum & ".tmp"

                    If m_DebugLevel > 4 Then
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerInspResultsAssembly.UpdatePTModsFile(): Open " & strPTModsFilePath)
                    End If
                    srInFile = New System.IO.StreamReader(New System.IO.FileStream(strPTModsFilePath, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read))

                    If m_DebugLevel > 4 Then
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerInspResultsAssembly.UpdatePTModsFile(): Create " & strPTModsFilePathNew)
                    End If
                    swOutFile = New System.IO.StreamWriter(New System.IO.FileStream(strPTModsFilePathNew, System.IO.FileMode.Create, System.IO.FileAccess.Write, System.IO.FileShare.Read))

                    blnDifferenceFound = False
                    Do While srInFile.Peek <> -1
                        strLineIn = srInFile.ReadLine
                        strLineIn = strLineIn.Trim

                        If strLineIn.Length > 0 Then

                            If strLineIn.Chars(0) = "#"c Then
                                ' Comment line; skip it
                            Else
                                ' Split the line on tabs
                                strSplitLine = strLineIn.Split(ControlChars.Tab)

                                If strSplitLine.Length >= 3 Then
                                    strModName = strSplitLine(0).ToLower

                                    blnMatchFound = False
                                    For intIndex = 0 To udtModList.Length - 1
                                        If udtModList(intIndex).ModName.ToLower = strModName Then
                                            ' Match found
                                            blnMatchFound = True
                                            Exit For
                                        End If
                                    Next

                                    If blnMatchFound Then
                                        If blnModProcessed(intIndex) Then
                                            ' This mod was already processed; don't write the line out again
                                            strLineIn = String.Empty
                                        Else
                                            With udtModList(intIndex)
                                                ' First time we've seen this mod; make sure the mod mass and residues are correct
                                                If strSplitLine(1) <> .ModMass OrElse strSplitLine(2) <> .Residues Then
                                                    ' Mis-match; update the line
                                                    strLineIn = .ModName & ControlChars.Tab & .ModMass & ControlChars.Tab & .Residues

                                                    If m_DebugLevel > 4 Then
                                                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerInspResultsAssembly.UpdatePTModsFile(): Mod def in PTMods.txt doesn't match required mod def; updating to: " & strLineIn)
                                                    End If

                                                    blnDifferenceFound = True
                                                End If
                                            End With
                                            blnModProcessed(intIndex) = True
                                        End If
                                    End If
                                End If
                            End If
                        End If

                        If blnPrevLineWasBlank AndAlso strLineIn.Length = 0 Then
                            ' Don't write out two blank lines in a row; skip this line
                        Else
                            swOutFile.WriteLine(strLineIn)

                            If strLineIn.Length = 0 Then
                                blnPrevLineWasBlank = True
                            Else
                                blnPrevLineWasBlank = False
                            End If
                        End If

                    Loop

                    ' Close the input file
                    srInFile.Close()

                    ' Look for any unprocessed mods
                    For intIndex = 0 To udtModList.Length - 1
                        If Not blnModProcessed(intIndex) Then
                            With udtModList(intIndex)
                                strLineIn = .ModName & ControlChars.Tab & .ModMass & ControlChars.Tab & .Residues
                            End With
                            swOutFile.WriteLine(strLineIn)

                            blnDifferenceFound = True
                        End If
                    Next

                    ' Close the output file
                    swOutFile.Close()

                    If blnDifferenceFound Then
                        ' Wait 500 msec, then replace PTMods.txt with strPTModsFilePathNew
                        System.Threading.Thread.Sleep(500)

                        Try
                            strPTModsFilePathOld = strPTModsFilePath & ".old"
                            If System.IO.File.Exists(strPTModsFilePathOld) Then
                                System.IO.File.Delete(strPTModsFilePathOld)
                            End If

                            System.IO.File.Move(strPTModsFilePath, strPTModsFilePathOld)
                            System.IO.File.Move(strPTModsFilePathNew, strPTModsFilePath)
                        Catch ex As Exception
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerInspResultsAssembly.UpdatePTModsFile, Error swapping in the new PTMods.txt file : " & m_JobNum & "; " & ex.Message)
                            Return False
                        End Try
                    Else
                        ' No difference was found; delete the .tmp file
                        System.Threading.Thread.Sleep(500)
                        Try
                            System.IO.File.Delete(strPTModsFilePathNew)
                        Catch ex As Exception
                            ' Ignore errors here
                        End Try
                    End If

                End If
            End If

        Catch ex As Exception
            m_message = "Error in InspectResultsAssembly->UpdatePTModsFile"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message)
            Return False
        End Try

        Return True

    End Function

    Private Sub UpdateStatusRunning(ByVal sngPercentComplete As Single)
        m_progress = sngPercentComplete
        m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.RUNNING_TOOL, sngPercentComplete, 0, "", "", "", False)
    End Sub

    Private Function ValidateShuffledDBInUse(ByVal strInspectResultsPath As String) As Boolean
        Dim srInspectResults As System.IO.StreamReader
        Dim intLinesRead As Integer

        Dim strLineIn As String = String.Empty
        Dim strSplitLine() As String

        Dim intDecoyProteinCount As Integer
        Dim intNormalProteinCount As Integer

        Dim blnShuffledDBUsed As Boolean

        Dim chSepChars() As Char = New Char() {ControlChars.Tab}

        blnShuffledDBUsed = AnalysisManagerBase.clsGlobal.CBoolSafe(m_jobParams.GetParam("InspectUsesShuffledDB"))

        If blnShuffledDBUsed Then
            ' Open the _inspect.txt file and make sure proteins exist that start with XXX
            ' If not, change blnShuffledDBUsed back to false

            Try
                srInspectResults = New System.IO.StreamReader(New System.IO.FileStream(strInspectResultsPath, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.Read))

                intLinesRead = 0
                intDecoyProteinCount = 0
                intNormalProteinCount = 0

                Do While srInspectResults.Peek >= 0
                    strLineIn = srInspectResults.ReadLine
                    intLinesRead += 1

                    If Not strLineIn Is Nothing Then
                        ' Protein info should be stored in the fourth column (index=3)
                        strSplitLine = strLineIn.Split(chSepChars, 5)

                        If intLinesRead = 1 Then
                            ' Verify that strSplitLine(3) is "Protein"
                            If Not strSplitLine(3).ToLower.StartsWith("protein") Then
                                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "The fourth column in the Inspect results file does not start with 'Protein'; this is unexpected")
                            End If
                        Else
                            If strSplitLine(3).StartsWith("XXX") Then
                                intDecoyProteinCount += 1
                            Else
                                intNormalProteinCount += 1
                            End If
                        End If
                    End If

                    If intDecoyProteinCount > 10 Then Exit Do
                Loop

                srInspectResults.Close()

                If intDecoyProteinCount = 0 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "The job has 'InspectUsesShuffledDB' set to True in the Settings file, but none of the proteins in the result file starts with XXX.  Will assume the fasta file did NOT have shuffled proteins, and will thus NOT use '-S 0.5' when calling " & PVALUE_MINLENGTH5_SCRIPT)
                    blnShuffledDBUsed = False
                End If

            Catch ex As Exception
                m_message = "Error in InspectResultsAssembly->strInspectResultsPath"
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message)
            End Try

        End If

        Return blnShuffledDBUsed

    End Function


    ''' <summary>
    ''' Stores the _inspect.txt file in _inspect_all.zip
    ''' Stores the _inspect_fht.txt file in _inspect_fht.zip (but renames it to _inspect.txt before storing)
    ''' Stores the _inspect_filtered.txt file in _inspect.zip (but renames it to _inspect.txt before storing)
    ''' </summary>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Private Function ZipInspectResults() As IJobParams.CloseOutType

        Dim blnSuccess As Boolean

        Try
            UpdateStatusRunning(mPercentCompleteStartLevels(eInspectResultsProcessingSteps.ZipInspectResults))

            ' Zip up the _inspect.txt file into _inspect_all.zip
            ' Rename to _inspect.txt before zipping
            ' Delete the _inspect.txt file after zipping
            blnSuccess = RenameAndZipInspectFile(System.IO.Path.Combine(m_WorkDir, m_Dataset & ORIGINAL_INSPECT_FILE_SUFFIX), _
                                                 System.IO.Path.Combine(m_WorkDir, m_Dataset & "_inspect_all.zip"), _
                                                 True)

            If Not blnSuccess Then
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If


            ' Zip up the _inspect_fht.txt file into _inspect_fht.zip
            ' Rename to _inspect.txt before zipping
            ' Delete the _inspect.txt file after zipping
            blnSuccess = RenameAndZipInspectFile(System.IO.Path.Combine(m_WorkDir, m_Dataset & FIRST_HITS_INSPECT_FILE_SUFFIX), _
                                                 System.IO.Path.Combine(m_WorkDir, m_Dataset & "_inspect_fht.zip"), _
                                                 True)

            If Not blnSuccess Then
                ' Ignore errors creating the _fht.zip file
            End If


            ' Zip up the _inspect_filtered.txt file into _inspect.zip
            ' Rename to _inspect.txt before zipping
            ' Do not delete the _inspect.txt file after zipping since it is used in function CreatePeptideToProteinMapping
            blnSuccess = RenameAndZipInspectFile(System.IO.Path.Combine(m_WorkDir, m_Dataset & FILTERED_INSPECT_FILE_SUFFIX), _
                                     System.IO.Path.Combine(m_WorkDir, m_Dataset & "_inspect.zip"), _
                                     False)

            If Not blnSuccess Then
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            ' Add the _inspect.txt file to .FilesToDelete since we only want to keep the Zipped version
            clsGlobal.FilesToDelete.Add(mInspectResultsFileName)

        Catch ex As Exception
            m_message = "Error in InspectResultsAssembly->ZipInspectResults"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

#End Region

#Region "Event Handlers"
    Private Sub mPeptideToProteinMapper_ProgressChanged(ByVal taskDescription As String, ByVal percentComplete As Single) Handles mPeptideToProteinMapper.ProgressChanged

        ' Note that percentComplete is a value between 0 and 100

        Const STATUS_UPDATE_INTERVAL_SECONDS As Integer = 5
        Const MAPPER_PROGRESS_LOG_INTERVAL_SECONDS As Integer = 120

        Static dtLastStatusUpdate As System.DateTime
        Static dtLastLogTime As System.DateTime

        Dim sngStartPercent As Single = mPercentCompleteStartLevels(eInspectResultsProcessingSteps.CreatePeptideToProteinMapping)
        Dim sngEndPercent As Single = mPercentCompleteStartLevels(eInspectResultsProcessingSteps.CreatePeptideToProteinMapping + 1)
        Dim sngPercentCompleteEffective As Single

        sngPercentCompleteEffective = sngStartPercent + CSng(percentComplete / 100.0 * (sngEndPercent - sngStartPercent))

        If System.DateTime.Now.Subtract(dtLastStatusUpdate).TotalSeconds >= STATUS_UPDATE_INTERVAL_SECONDS Then
            dtLastStatusUpdate = System.DateTime.Now

            ' Synchronize the stored Debug level with the value stored in the database
            Const MGR_SETTINGS_UPDATE_INTERVAL_SECONDS As Integer = 300
            AnalysisManagerBase.clsAnalysisToolRunnerBase.GetCurrentMgrSettingsFromDB(MGR_SETTINGS_UPDATE_INTERVAL_SECONDS, m_mgrParams, m_DebugLevel)

            UpdateStatusRunning(sngPercentCompleteEffective)
        End If

        If m_DebugLevel >= 3 Then
            If System.DateTime.Now.Subtract(dtLastLogTime).TotalSeconds >= MAPPER_PROGRESS_LOG_INTERVAL_SECONDS Then
                dtLastLogTime = System.DateTime.Now
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Mapping peptides to proteins: " & percentComplete.ToString("0.0") & "% complete")
            End If
        End If

    End Sub
#End Region

End Class
