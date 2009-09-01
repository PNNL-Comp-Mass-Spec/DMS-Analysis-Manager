Option Strict On

'*********************************************************************************************************
' Written by John Sandoval for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2009, Battelle Memorial Institute
' Created 01/29/2009
'
' Last modified 06/15/2009 JDS - Added logging using log4net
'*********************************************************************************************************

imports AnalysisManagerBase
Imports PRISM.Files
Imports PRISM.Files.clsFileTools
Imports AnalysisManagerBase.clsGlobal
Imports System.io
Imports System.Text.RegularExpressions
Imports System.Collections.Generic

Public Class clsAnalysisToolRunnerInspResultsAssembly
    Inherits clsAnalysisToolRunnerBase

    '*********************************************************************************************************
    'Class for running Inspect Results Assembler
    '*********************************************************************************************************

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
    Protected r_FileSeparator As Regex
    Protected r_DTAFirstLine As Regex
    Protected m_textCattedFile As String

    Protected mInspectSearchLogFilePath As String = "InspectSearchLog.txt"      ' This value gets updated in function RunInSpecT

    Private WithEvents mPeptideToProteinMapper As PeptideToProteinMapEngine.clsPeptideToProteinMapEngine

    ' mPercentCompleteStartLevels is an array that lists the percent complete value to report 
    '  at the start of each of the various processing steps performed in this procedure
    ' The percent complete values range from 0 to 100
    Const PERCENT_COMPLETE_LEVEL_COUNT As Integer = 7
    Protected mPercentCompleteStartLevels() As Single

#End Region

#Region "Enums"
    'Used for result file type
    Enum ResultFileType
        INSPECT_RESULT = 0
        INSPECT_ERROR = 1
        INSPECT_SEARCH = 2
    End Enum

    ' Note: if you add/remove any steps, then update PERCENT_COMPLETE_LEVEL_COUNT and update the population of mPercentCompleteStartLevels()
    Enum eInspectResultsProcessingSteps
        Starting = 0
        CreateIndexedDbFiles = 1
        AssembleResults = 2
        CreatePeptideToProteinMapping = 3
        RunpValue = 4
        RunSummary = 5
        ZipMainOutputFile = 6
    End Enum

#End Region

#Region "Methods"
    ''' <summary>
    ''' Constructor
    ''' </summary>
    ''' <remarks>Initializes classwide variables</remarks>
    Public Sub New()
        InitializeVariables()
    End Sub

    Protected Sub InitializeVariables()

        ' Define the percent complete values to use for the start of each processing step

        ReDim mPercentCompleteStartLevels(PERCENT_COMPLETE_LEVEL_COUNT)

        mPercentCompleteStartLevels(eInspectResultsProcessingSteps.Starting) = 0
        mPercentCompleteStartLevels(eInspectResultsProcessingSteps.CreateIndexedDbFiles) = 1
        mPercentCompleteStartLevels(eInspectResultsProcessingSteps.AssembleResults) = 5
        mPercentCompleteStartLevels(eInspectResultsProcessingSteps.CreatePeptideToProteinMapping) = 10
        mPercentCompleteStartLevels(eInspectResultsProcessingSteps.RunpValue) = 90
        mPercentCompleteStartLevels(eInspectResultsProcessingSteps.RunSummary) = 95
        mPercentCompleteStartLevels(eInspectResultsProcessingSteps.ZipMainOutputFile) = 98
        mPercentCompleteStartLevels(PERCENT_COMPLETE_LEVEL_COUNT) = 100

    End Sub

    ''' <summary>
    ''' Initializes class
    ''' </summary>
    ''' <param name="mgrParams">Object containing manager parameters</param>
    ''' <param name="jobParams">Object containing job parameters</param>
    ''' <param name="logger">Logging object</param>
    ''' <param name="StatusTools">Object for updating status file as job progresses</param>
    ''' <remarks></remarks>
    Public Overrides Sub Setup(ByVal mgrParams As IMgrParams, ByVal jobParams As IJobParams, _
      ByVal StatusTools As IStatusFile)

        MyBase.Setup(mgrParams, jobParams, StatusTools)

        If m_DebugLevel > 3 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerInspResultsAssembly.Setup()")
        End If
    End Sub
    ''' <summary>
    ''' Runs InSpecT tool
    ''' </summary>
    ''' <returns>CloseOutType enum indicating success or failure</returns>
    ''' <remarks></remarks>
    Public Overrides Function RunTool() As IJobParams.CloseOutType
        Dim result As IJobParams.CloseOutType
        Dim numClonedSteps As String
        Dim intNumResultFiles As Integer

        Dim isParallelized As Boolean = False
        Dim objIndexedDBCreator As New clsCreateInspectIndexedDB

        Try
            If m_DebugLevel > 4 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerInspResultsAssembly.RunTool(): Enter")
            End If

            'Start the job timer
            m_StartTime = System.DateTime.Now

            UpdateStatusRunning(mPercentCompleteStartLevels(eInspectResultsProcessingSteps.CreateIndexedDbFiles))

            result = objIndexedDBCreator.CreateIndexedDbFiles(m_mgrParams, m_jobParams, m_DebugLevel, m_JobNum)
            If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                Return result
            End If

            'Determine if this is a parallelized job
            numClonedSteps = m_jobParams.GetParam("NumberOfClonedSteps")
            mInspectResultsFileName = m_jobParams.GetParam("datasetNum") & "_inspect.txt"
            If Not [String].IsNullOrEmpty(numClonedSteps) Then
                intNumResultFiles = CInt(numClonedSteps)

                If m_DebugLevel >= 2 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Assembling parallelized inspect files; file count = " & intNumResultFiles.ToString)
                End If

                UpdateStatusRunning(mPercentCompleteStartLevels(eInspectResultsProcessingSteps.AssembleResults))

                ' This is a parallelized job; need to re-assemble the results
                result = AssembleResults(intNumResultFiles)
                If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                    Return result
                End If
                isParallelized = True
            End If

            UpdateStatusRunning(mPercentCompleteStartLevels(eInspectResultsProcessingSteps.CreatePeptideToProteinMapping))

            ' Create the Peptide to Protein map file
            If CreatePeptideToProteinMapping() <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN, "Error creating peptide to protein Mapping, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step"))
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            UpdateStatusRunning(mPercentCompleteStartLevels(eInspectResultsProcessingSteps.RunpValue))

            If RunpValue() <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN, "Error running PValue script, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step"))
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            UpdateStatusRunning(mPercentCompleteStartLevels(eInspectResultsProcessingSteps.RunSummary))

            result = RunSummary()
            If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN, "Error running summary script, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step"))
                Return result
            End If

            'Zip the result file if parallelized
            If isParallelized Then
                UpdateStatusRunning(mPercentCompleteStartLevels(eInspectResultsProcessingSteps.ZipMainOutputFile))

                If ZipMainOutputFile(System.IO.Path.Combine(m_WorkDir, mInspectResultsFileName)) <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN, "Error occurred zipping the result file, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step"))
                    Return IJobParams.CloseOutType.CLOSEOUT_FAILED
                End If
            End If

            UpdateStatusRunning(100)

            'Stop the job timer
            m_StopTime = System.DateTime.Now

            'Add the current job data to the summary file
            If Not UpdateSummaryFile() Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN, "Error creating summary file, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step"))
            End If

            'Make sure objects are released
            System.Threading.Thread.Sleep(2000)        '2 second delay
            GC.Collect()
            GC.WaitForPendingFinalizers()

            result = MakeResultsFolder()
            If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN, "Error creating the result folder, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step"))
                Return result
            End If

            result = MoveResultFiles()
            If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN, "Error moving result files to local result directory, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step"))
                Return result
            End If

            result = CopyResultsFolderToServer()
            If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN, "Error copying results to result directory on server, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step"))
                Return result
            End If

            'If parallelized, then remove multiple result files from server
            If isParallelized Then
                If Not clsGlobal.RemoveNonResultServerFiles(m_DebugLevel) Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN, "Error deleting non result files from directory on server, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step"))
                    Return IJobParams.CloseOutType.CLOSEOUT_FAILED
                End If
            End If

            If Not clsGlobal.RemoveNonResultFiles(m_WorkDir, m_DebugLevel) Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN, "Error deleting non result files from local directory, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step"))
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

        Catch ex As Exception
            m_message = "Error in InspectResultsAssembly->RunTool: " & ex.Message
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS 'No failures so everything must have succeeded

    End Function

    Private Sub UpdateStatusRunning(ByVal sngPercentComplete As Single)
        m_progress = sngPercentComplete
        m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.RUNNING_TOOL, sngPercentComplete, 0, "", "", "", False)
    End Sub

    Private Function AssembleResults(ByVal intNumResultFiles As Integer) As IJobParams.CloseOutType
        Dim result As IJobParams.CloseOutType
        Dim InspectErrorResultFile As String = ""
        Dim InspectSearchLogFile As String = ""

        Try

            If m_DebugLevel >= 3 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Assembling parallelized inspect result files")
            End If

            result = AssembleFiles(mInspectResultsFileName, ResultFileType.INSPECT_RESULT, intNumResultFiles)
            If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                Return result
            End If
            ' Note: do not add mInspectResultsFileName to clsGlobal.m_ExceptionFiles since we will be zipping the file and we thus do not want to include it in the results folder

            ' Now that the combined Inspect Results file has been made, we need to rescore it using PValue_MinLength5.py (which is similar to PValue.py but retains peptides of length 5 or greater)
            RescoreAssembledInspectResults()

            If m_DebugLevel >= 3 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Assembling parallelized inspect error files")
            End If

            InspectErrorResultFile = m_jobParams.GetParam("datasetNum") & "_error.txt"
            result = AssembleFiles(InspectErrorResultFile, ResultFileType.INSPECT_ERROR, intNumResultFiles)
            If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                Return result
            End If
            clsGlobal.m_ExceptionFiles.Add(InspectErrorResultFile)

            If m_DebugLevel >= 3 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Assembling parallelized inspect search log files")
            End If

            InspectSearchLogFile = "InspectSearchLog.txt"
            result = AssembleFiles(InspectSearchLogFile, ResultFileType.INSPECT_SEARCH, intNumResultFiles)
            If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                Return result
            End If
            clsGlobal.m_ExceptionFiles.Add(InspectSearchLogFile)

        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerInspResultsAssembly.AssembleResults, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step") & ": " & ex.Message)
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
        Dim blnAddSegmentNumberToEachLine As Boolean = False

        Dim intTabIndex As Integer
        Dim intSlashIndex As Integer

        Try
            DatasetName = m_jobParams.GetParam("datasetNum")

            tw = createNewExportFile(System.IO.Path.Combine(m_WorkDir, strCombinedFileName))
            If tw Is Nothing Then
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            For fileNameCounter = 1 To intNumResultFiles
                Select Case resFileType
                    Case ResultFileType.INSPECT_RESULT
                        InspectResultsFile = DatasetName & "_" & fileNameCounter & "_inspect.txt"
                        blnFilesContainHeaderLine = True
                        blnAddSegmentNumberToEachLine = False

                    Case ResultFileType.INSPECT_ERROR
                        InspectResultsFile = DatasetName & "_" & fileNameCounter & "_error.txt"
                        blnFilesContainHeaderLine = False
                        blnAddSegmentNumberToEachLine = True

                    Case ResultFileType.INSPECT_SEARCH
                        InspectResultsFile = "InspectSearchLog_" & fileNameCounter & ".txt"
                        blnFilesContainHeaderLine = True
                        blnAddSegmentNumberToEachLine = True

                    Case Else
                        ' Unknown ResultFileType
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerInspResultsAssembly->AssembleFiles: Unknown Inspect Result File Type: " & resFileType.ToString)
                        Exit For
                End Select

                If File.Exists(System.IO.Path.Combine(m_WorkDir, InspectResultsFile)) Then
                    intLinesRead = 0

                    tr = New System.IO.StreamReader(System.IO.Path.Combine(m_WorkDir, InspectResultsFile))
                    s = tr.ReadLine

                    Do While s IsNot Nothing
                        intLinesRead += 1
                        If intLinesRead = 1 Then

                            If blnFilesContainHeaderLine Then
                                If Not blnHeaderLineWritten Then
                                    If blnAddSegmentNumberToEachLine Then
                                        s = "Segment" & ControlChars.Tab & s
                                    End If
                                    tw.WriteLine(s)
                                End If
                            Else
                                If Not blnHeaderLineWritten Then
                                    tw.WriteLine("Segment" & ControlChars.Tab & "Message")
                                End If
                                If blnAddSegmentNumberToEachLine Then
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
                End If
            Next

            'close the main result file
            tw.Close()

        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "clsAnalysisToolRunnerInspResultsAssembly.AssembleFiles, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step") & ": " & ex.Message)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    Private Function createNewExportFile(ByVal exportFileName As String) As System.IO.StreamWriter
        Dim ef As System.IO.StreamWriter

        If System.IO.File.Exists(exportFileName) Then
            'post error to log
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerInspResultsAssembly->createNewExportFile: Export file already exists (" & exportFileName & "); this is unexpected")
            Return Nothing
        End If

        ef = New System.IO.StreamWriter(exportFileName, False)
        Return ef

    End Function

    Private Function CreatePeptideToProteinMapping() As IJobParams.CloseOutType
        Dim OrgDbDir As String = m_mgrParams.GetParam("orgdbdir")
        Dim dbFilename As String = Path.Combine(OrgDbDir, m_jobParams.GetParam("generatedFastaName"))

        Dim blnIgnorePeptideToProteinMapperErrors As Boolean
        Dim blnSuccess As Boolean

        Try
            If m_DebugLevel >= 2 Then
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

            blnSuccess = mPeptideToProteinMapper.ProcessFile(Path.Combine(m_WorkDir, mInspectResultsFileName), m_WorkDir, String.Empty, True)

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
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerInspResultsAssembly.CreatePeptideToProteinMapping, Error running clsPeptideToProteinMapEngine: " & _
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
    ''' Uses PValue_MinLength5.py to recompute the p-value and FScore values for Inspect results computed in parallel then reassembled
    ''' </summary>
    ''' <returns>CloseOutType enum indicating success or failure</returns>
    ''' <remarks></remarks>
    Private Function RescoreAssembledInspectResults() As IJobParams.CloseOutType
        Const PVALUE_MINLENGTH5_SCRIPT As String = "PValue_MinLength5.py"

        Dim CmdRunner As clsRunDosProgram
        Dim CmdStr As String

        Dim InspectDir As String = m_mgrParams.GetParam("inspectdir")
        Dim strInspectResultsFilePath As String = Path.Combine(m_WorkDir, m_jobParams.GetParam("datasetNum") & "_inspect.txt")
        Dim strRescoredFilePath As String = Path.Combine(m_WorkDir, m_jobParams.GetParam("datasetNum") & "_inspect_Rescored.txt")
        Dim pythonProgLoc As String = m_mgrParams.GetParam("pythonprogloc")

        CmdRunner = New clsRunDosProgram(InspectDir)

        If m_DebugLevel > 4 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerInspResultsAssembly.RescoreAssembledInspectResults(): Enter")
        End If

        ' verify that python program file exists
        Dim progLoc As String = pythonProgLoc
        If Not File.Exists(progLoc) Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Cannot find python.exe program file: " & progLoc)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        ' verify that PValue python script exists
        Dim pvalueScriptPath As String = System.IO.Path.Combine(InspectDir, PVALUE_MINLENGTH5_SCRIPT)
        If Not File.Exists(pvalueScriptPath) Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Cannot find PValue script: " & pvalueScriptPath)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        'Set up and execute a program runner to run PVALUE_MINLENGTH5_SCRIPT.py
        ' Note that PVALUE_MINLENGTH5_SCRIPT.py is nearly identical to PValue.py but it retains peptides with 5 amino acids (default is 7)
        ' Note that -H means to not remove entries mapped to shuffled proteins (whose names start with XXX)
        ' -p 1 will essentially mean to not remove any peptides by p-value
        ' -x means to retain "bad" matches (those with poor delta-score values, a p-value below the threshold, or and MQScore below -1)

        CmdStr = " " & pvalueScriptPath & " -r " & strInspectResultsFilePath & " -w " & strRescoredFilePath & " -H -p 1 -x"
        If m_DebugLevel >= 1 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, progLoc & CmdStr)
        End If

        If Not CmdRunner.RunProgram(progLoc, CmdStr, "PValue", False) Then
            ' Error running program; the error should have already been logged
        End If

        If CmdRunner.ExitCode <> 0 Then
            ' Note: Log the non-zero exit code as an error, but continue processing anyway
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, System.IO.Path.GetFileName(pvalueScriptPath) & " returned a non-zero exit code: " & CmdRunner.ExitCode.ToString)
        End If

        Try
            ' Now replace the original inspect results file with the rescored one
            ' However, if the rescored file is less than 90% of the size of the original one, then we will retain the original one (for debugging purposes)
            Dim fiRescoredFile As System.IO.FileInfo
            Dim fiTargetFile As System.IO.FileInfo
            Dim strTargetFilePath As String
            Dim strOriginalInspectFilePath As String

            fiRescoredFile = New System.IO.FileInfo(strRescoredFilePath)
            fiTargetFile = New System.IO.FileInfo(strInspectResultsFilePath)

            If Not fiRescoredFile.Exists Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Rescored Inspect Results file not found: " & fiRescoredFile.FullName)
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            If fiTargetFile.Length = 0 Then
                ' Assembled inspect results file is 0-bytes; this is unexpected
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Assembled Inspect Results file is 0 bytes: " & fiTargetFile.FullName)
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            If m_DebugLevel >= 1 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Rescored Inspect results file created; size is " & (fiRescoredFile.Length / CDbl(fiTargetFile.Length) * 100).ToString("0.0") & "% of the original (" & fiRescoredFile.Length & " bytes vs. " & fiTargetFile.Length & " bytes in original)")
            End If

            strTargetFilePath = fiTargetFile.FullName
            If fiRescoredFile.Length < 0.9 * fiTargetFile.Length Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Rescored Inspect results file is more than 10% smaller than the original results file; renaming the original file to _inspect_original.txt")

                ' Keep the original Inspect results file since the new file is less than 90% the size
                strOriginalInspectFilePath = System.IO.Path.GetFileNameWithoutExtension(fiTargetFile.Name) & "_original.txt"
                fiTargetFile.MoveTo(System.IO.Path.Combine(fiTargetFile.DirectoryName, strOriginalInspectFilePath))
                clsGlobal.m_ExceptionFiles.Add(System.IO.Path.GetFileName(strOriginalInspectFilePath))
            End If

            If System.IO.File.Exists(strTargetFilePath) Then
                If m_DebugLevel >= 3 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Deleting " & strTargetFilePath)
                End If

                System.IO.File.Delete(strTargetFilePath)

                ' Wait 1 second
                System.Threading.Thread.Sleep(1000)
            End If

            ' Now rename the rescored file 
            If m_DebugLevel >= 3 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Renaming " & fiRescoredFile.FullName & " to " & strTargetFilePath)
            End If

            fiRescoredFile.MoveTo(strTargetFilePath)

        Catch ex As Exception
            m_message = "Error in InspectResultsAssembly->RescoreAssembledInspectResults: " & ex.Message
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    ''' <summary>
    ''' Run pValue.py program
    ''' </summary>
    ''' <returns>CloseOutType enum indicating success or failure</returns>
    ''' <remarks></remarks>
    Private Function RunpValue() As IJobParams.CloseOutType
        Const PVALUE_SCRIPT As String = "PValue.py"

        Dim CmdRunner As clsRunDosProgram
        Dim CmdStr As String

        Dim InspectDir As String = m_mgrParams.GetParam("inspectdir")
        Dim strInspectResultsFilePath As String = Path.Combine(m_WorkDir, m_jobParams.GetParam("datasetNum") & "_inspect.txt")
        Dim pvalDistributionFilename As String = Path.Combine(m_WorkDir, m_jobParams.GetParam("datasetNum") & "_PValueDistribution.txt")
        Dim strFilteredGroupedFilePath As String = Path.Combine(m_WorkDir, m_jobParams.GetParam("datasetNum") & "_inspect_FilteredGrouped.txt")
        Dim orgDbDir As String = m_mgrParams.GetParam("orgdbdir")
        Dim fastaFilename As String = Path.Combine(orgDbDir, m_jobParams.GetParam("generatedFastaName"))
        Dim dbFilename As String = fastaFilename.Replace("fasta", "trie")
        Dim pythonProgLoc As String = m_mgrParams.GetParam("pythonprogloc")
        Dim pthresh As String = getPthresh()

        CmdRunner = New clsRunDosProgram(InspectDir)

        If m_DebugLevel > 4 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerInspResultsAssembly.RunpValue(): Enter")
        End If

        ' verify that python program file exists
        Dim progLoc As String = pythonProgLoc
        If Not File.Exists(progLoc) Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Cannot find python.exe program file: " & progLoc)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        ' verify that PValue python script exists
        Dim pvalueScriptPath As String = System.IO.Path.Combine(InspectDir, PVALUE_SCRIPT)
        If Not File.Exists(pvalueScriptPath) Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Cannot find PValue script: " & pvalueScriptPath)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        ' Possibly required: Update the PTMods.txt file in InspectDir to contain the modification details, as defined in inspect_input.txt
        UpdatePTModsFile(InspectDir, System.IO.Path.Combine(m_WorkDir, "inspect_input.txt"))

        'Set up and execute a program runner to run PValue.py
        CmdStr = " " & pvalueScriptPath & " -r " & strInspectResultsFilePath & " -s " & pvalDistributionFilename & " -w " & strFilteredGroupedFilePath & " -p " & pthresh & " -i -a -d " & dbFilename
        If m_DebugLevel >= 1 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, progLoc & CmdStr)
        End If

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
    ''' Run -p threshold value
    ''' </summary>
    ''' <returns>Value as a string or empty string means failure</returns>
    ''' <remarks></remarks>
    Private Function getPthresh() As String
        Dim defPvalThresh As String = "0.1"
        Dim tmpPvalThresh As String = ""
        tmpPvalThresh = m_mgrParams.GetParam("InspectPvalueThreshold")
        If tmpPvalThresh <> "" Then
            Return tmpPvalThresh 'return pValueThreshold value in settings file
        Else
            Return defPvalThresh 'if not found, return default of 0.1
        End If

    End Function

    ''' <summary>
    ''' Assures that the PTMods.txt file in strInspectDir contains the modification info defined in strInspectInputFilePath
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

                    ' Read PTMods.txt to make look for the mods in udtModList
                    ' While reading, will create a new file with any required updates

                    strPTModsFilePath = System.IO.Path.Combine(strInspectDir, "PTMods.txt")
                    strPTModsFilePathNew = strPTModsFilePath & ".tmp"

                    If m_DebugLevel > 4 Then
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerInspResultsAssembly.UpdatePTModsFile(): Open " & strPTModsFilePath)
                    End If
                    srInFile = New System.IO.StreamReader(New System.IO.FileStream(strPTModsFilePath, FileMode.Open, FileAccess.Read, FileShare.Read))

                    If m_DebugLevel > 4 Then
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerInspResultsAssembly.UpdatePTModsFile(): Create " & strPTModsFilePathNew)
                    End If
                    swOutFile = New System.IO.StreamWriter(New System.IO.FileStream(strPTModsFilePathNew, FileMode.Create, FileAccess.Write, FileShare.Read))

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
                        ' Wait 2 seconds, then replace PTMods.txt with strPTModsFilePathNew
                        System.Threading.Thread.Sleep(2000)

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
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerInspResultsAssembly.UpdatePTModsFile, Error creating the new PTMods.txt file : " & m_JobNum & "; " & ex.Message)
            Return False
        End Try

        Return True

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
            srInFile = New System.IO.StreamReader((New System.IO.FileStream(strInspectParameterFilePath, FileMode.Open, FileAccess.Read, FileShare.Read)))

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
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerInspResultsAssembly.ExtractModInfoFromInspectParamFile, Error reading the Inspect parameter file (" & System.IO.Path.GetFileName(strInspectParameterFilePath) & "); " & ex.Message)
            Return False
        Finally
            If Not srInFile Is Nothing Then
                srInFile.Close()
            End If
        End Try

        Return True

    End Function

    ''' <summary>
    ''' Run Summary program
    ''' </summary>
    ''' <returns>CloseOutType enum indicating success or failure</returns>
    ''' <remarks></remarks>
    Private Function RunSummary() As IJobParams.CloseOutType
        Const SUMMARY_SCRIPT As String = "Summary.py"

        Dim CmdRunner As clsRunDosProgram
        Dim CmdStr As String

        Dim InspectDir As String = m_mgrParams.GetParam("inspectdir")
        Dim filteredHtmlFilename As String = Path.Combine(m_WorkDir, m_jobParams.GetParam("datasetNum") & "_inpsect_FilteredGrouped.html")
        Dim filteredGroupFilename As String = Path.Combine(m_WorkDir, m_jobParams.GetParam("datasetNum") & "_inspect_FilteredGrouped.txt")
        Dim orgDbDir As String = m_mgrParams.GetParam("orgdbdir")
        Dim fastaFilename As String = Path.Combine(orgDbDir, m_jobParams.GetParam("generatedFastaName"))
        Dim dbFilename As String = fastaFilename.Replace("fasta", "trie")
        Dim pythonProgLoc As String = m_mgrParams.GetParam("pythonprogloc")
        Dim pthresh As String = getPthresh()

        CmdRunner = New clsRunDosProgram(InspectDir)

        If m_DebugLevel > 4 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerInspResultsAssembly.RunSummary(): Enter")
        End If

        ' verify that python program file exists
        Dim progLoc As String = pythonProgLoc
        If Not File.Exists(progLoc) Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Cannot find python.exe program file: " & progLoc)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        ' verify that Summary python script exists
        Dim SummaryScriptPath As String = System.IO.Path.Combine(InspectDir, SUMMARY_SCRIPT)
        If Not File.Exists(SummaryScriptPath) Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Cannot find Summary script: " & SummaryScriptPath)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        'Set up and execute a program runner to run Summary.py
        CmdStr = " " & SummaryScriptPath & " -r " & filteredGroupFilename & " -d " & dbFilename & " -p " & pthresh & " -w " & filteredHtmlFilename
        If m_DebugLevel >= 1 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, progLoc & CmdStr)
        End If

        If Not CmdRunner.RunProgram(progLoc, CmdStr, "Summary", False) Then
            ' Error running program; the error should have already been logged
        End If

        If CmdRunner.ExitCode <> 0 Then
            ' Note: Log the non-zero exit code as an error, but return CLOSEOUT_SUCCESS anyway
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, System.IO.Path.GetFileName(SummaryScriptPath) & " returned a non-zero exit code: " & CmdRunner.ExitCode.ToString)
        End If

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    ''' <summary>
    ''' Zips the concatenated result file
    ''' </summary>
    ''' <returns>CloseOutType enum indicating success or failure</returns>
    ''' <remarks></remarks>
    Private Function ZipMainOutputFile(ByVal filename As String) As IJobParams.CloseOutType
        Dim ZipFileName As String

        Try
            Dim Zipper As New ZipTools(m_WorkDir, m_mgrParams.GetParam("zipprogram"))
            ZipFileName = Path.Combine(m_WorkDir, Path.GetFileNameWithoutExtension(filename)) & ".zip"
            If Not Zipper.MakeZipFile("-fast", ZipFileName, Path.GetFileName(filename)) Then
                Dim Msg As String = "Error zipping output files, job " & m_JobNum
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
                m_message = AppendToComment(m_message, "Error zipping output files")
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If
            clsGlobal.m_ExceptionFiles.Add(Path.GetFileName(ZipFileName))

        Catch ex As Exception
            Dim Msg As String = "clsAnalysisToolRunnerInspResultsAssembly.ZipMainOutputFile, Exception zipping output files, job " & m_JobNum & ": " & ex.Message
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
            m_message = AppendToComment(m_message, "Error zipping output files")
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        'Delete the dta result file
        Try
            File.SetAttributes(filename, File.GetAttributes(filename) And (Not FileAttributes.ReadOnly))
            File.Delete(filename)
        Catch Err As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerInspResultsAssembly.ZipMainOutputFile, Error deleting _inspect.txt file, job " & m_JobNum & Err.Message)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

#End Region

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

End Class
