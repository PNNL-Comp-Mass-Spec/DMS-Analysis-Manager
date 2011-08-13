'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Created 07/29/2011
'
'*********************************************************************************************************

Option Strict On

imports AnalysisManagerBase
'Imports PRISM.Files
'Imports AnalysisManagerBase.clsGlobal

Public Class clsAnalysisToolRunnerMSGFDB
    Inherits clsAnalysisToolRunnerBase

	'*********************************************************************************************************
	'Class for running MSGFDB analysis
	'*********************************************************************************************************

#Region "Module Variables"
    Protected Const MSGFDB_CONSOLE_OUTPUT As String = "MSGFDB_ConsoleOutput.txt"
    Protected Const MSGFDB_JAR_NAME As String = "MSGFDB.jar"

    Protected Const PROGRESS_PCT_MSGFDB_STARTING As Single = 1
    Protected Const PROGRESS_PCT_MSGFDB_LOADING_PROTEINS As Single = 3
    Protected Const PROGRESS_PCT_MSGFDB_PREPROCESSING_SPECTRA As Single = 5
    Protected Const PROGRESS_PCT_MSGFDB_SEARCHING_DATABASE As Single = 10
    Protected Const PROGRESS_PCT_MSGFDB_COMPUTING_SPECTRAL_PROBABILITIES = 50
    Protected Const PROGRESS_PCT_MSGFDB_COMPUTING_EFDRS As Single = 92
    Protected Const PROGRESS_PCT_MSGFDB_WRITING_RESULTS As Single = 93
    Protected Const PROGRESS_PCT_MSGFDB_MAPPING_PEPTIDES_TO_PROTEINS As Single = 94
    Protected Const PROGRESS_PCT_MSGFDB_COMPLETE As Single = 99

    Protected WithEvents CmdRunner As clsRunDosProgram

    Private WithEvents mPeptideToProteinMapper As PeptideToProteinMapEngine.clsPeptideToProteinMapEngine

#End Region

#Region "Methods"
	''' <summary>
	''' Runs MSGFDB tool
	''' </summary>
	''' <returns>CloseOutType enum indicating success or failure</returns>
	''' <remarks></remarks>
    Public Overrides Function RunTool() As IJobParams.CloseOutType
        Dim CmdStr As String
        Dim intJavaMemorySize As Integer
        Dim strMSGFCmdLineOptions As String

        Dim result As IJobParams.CloseOutType
        Dim eReturnCode As IJobParams.CloseOutType
        Dim blnProcessingError As Boolean = False

        Dim blnSuccess As Boolean
        Dim MSGFDBProgLoc As String

        Dim OrgDbDir As String
        Dim strFASTAFilePath As String
        Dim FastaFileSizeKB As Single

        Dim strParameterFilePath As String
        Dim ResultsFileName As String

        Dim objIndexedDBCreator As New clsCreateMSGFDBSuffixArrayFiles

        Try
            'Call base class for initial setup
            If Not MyBase.RunTool = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            If m_DebugLevel > 4 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerMSGFDB.RunTool(): Enter")
            End If

            ' Verify that program files exist

            ' JavaProgLoc will typically be "C:\Program Files\Java\jre6\bin\Java.exe"
            ' Note that we need to run MSGF with a 64-bit version of Java since it prefers to use 2 or more GB of ram
            Dim JavaProgLoc As String = m_mgrParams.GetParam("JavaLoc")
            If Not System.IO.File.Exists(JavaProgLoc) Then
                If JavaProgLoc.Length = 0 Then JavaProgLoc = "Parameter 'JavaLoc' not defined for this manager"
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Cannot find Java: " & JavaProgLoc)
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            ' Determine the path to the MSGFDB program
            MSGFDBProgLoc = DetermineProgramLocation("MSGFDB", "MSGFDBprogloc", MSGFDB_JAR_NAME)

            If String.IsNullOrWhiteSpace(MSGFDBProgLoc) Then
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            ' Store the MSGFDB version info in the database
            StoreToolVersionInfo(MSGFDBProgLoc)

            ' Make sure the _DTA.txt file is valid
            If Not ValidateCDTAFile() Then
                Return IJobParams.CloseOutType.CLOSEOUT_NO_DTA_FILES
            End If


            ' Define the path to the fasta file
            OrgDbDir = m_mgrParams.GetParam("orgdbdir")
            strFASTAFilePath = System.IO.Path.Combine(OrgDbDir, m_jobParams.GetParam("generatedFastaName"))

            Dim fiFastaFile As System.IO.FileInfo
            fiFastaFile = New System.IO.FileInfo(strFASTAFilePath)

            If Not fiFastaFile.Exists Then
                ' Fasta file not found
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Fasta file not found: " & fiFastaFile.FullName)
                Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
            End If

            FastaFileSizeKB = CSng(fiFastaFile.Length / 1024.0)

            If m_DebugLevel >= 3 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Indexing Fasta file to create Suffix Array files")
            End If

            ' Index the fasta file to create the Suffix Array files
            result = objIndexedDBCreator.CreateSuffixArrayFiles(m_WorkDir, m_DebugLevel, m_JobNum, JavaProgLoc, MSGFDBProgLoc, fiFastaFile.FullName)
            If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                ' Error message has already been logged
                Return result
            End If

            strParameterFilePath = System.IO.Path.Combine(m_WorkDir, m_jobParams.GetParam("parmFileName"))

            If Not System.IO.File.Exists(strParameterFilePath) Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Parameter file not found")
                Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
            End If

            ' Read the MSGFDB Parameter File
            strMSGFCmdLineOptions = ParseMSGFDBParameterFile(strParameterFilePath, FastaFileSizeKB)
            ResultsFileName = m_Dataset & "_msgfdb.txt"

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Running MSGFDB")

            ' If an MSGFDB analysis crashes with an "out-of-memory" error, then we need to reserve more memory for Java 
            ' Customize this on a per-job basis using the MSGFDBJavaMemorySize setting in the settings file 
            ' (job 611216 succeeded with a value of 5000)
            intJavaMemorySize = clsGlobal.GetJobParameter(m_jobParams, "MSGFDBJavaMemorySize", 2000)
            If intJavaMemorySize < 512 Then intJavaMemorySize = 512

            If IsMatch(System.Environment.MachineName, "monroe2") Then intJavaMemorySize = 1024

            'Set up and execute a program runner to run MSGFDB
            CmdStr = " -Xmx" & intJavaMemorySize.ToString & "M -jar " & MSGFDBProgLoc


            ' Define the input file, output file, and fasta file
            CmdStr &= " -s " & m_Dataset & "_dta.txt"
            CmdStr &= " -o " & ResultsFileName
            CmdStr &= " -d " & fiFastaFile.FullName

            ' Append the remaining options loaded from the parameter file
            CmdStr &= " " & strMSGFCmdLineOptions

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, JavaProgLoc & " " & CmdStr)

            CmdRunner = New clsRunDosProgram(m_WorkDir)

            With CmdRunner
                .CreateNoWindow = True
                .CacheStandardOutput = True
                .EchoOutputToConsole = True

                .WriteConsoleOutputToFile = True
                .ConsoleOutputFilePath = System.IO.Path.Combine(m_WorkDir, MSGFDB_CONSOLE_OUTPUT)
            End With

            m_progress = PROGRESS_PCT_MSGFDB_STARTING

            blnSuccess = CmdRunner.RunProgram(JavaProgLoc, CmdStr, "MSGFDB", True)

            If Not blnSuccess Then
                Dim Msg As String
                Msg = "Error running MSGFDB"
                m_message = AnalysisManagerBase.clsGlobal.AppendToComment(m_message, Msg)

                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg & ", job " & m_JobNum)

                If CmdRunner.ExitCode <> 0 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "MSGFDB returned a non-zero exit code: " & CmdRunner.ExitCode.ToString)
                Else
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Call to MSGFDB failed (but exit code is 0)")
                End If

                blnProcessingError = True

            Else
                m_progress = PROGRESS_PCT_MSGFDB_COMPLETE
                m_StatusTools.UpdateAndWrite(m_progress)
                If m_DebugLevel >= 3 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "MSGFDB Search Complete")
                End If
            End If

            If Not blnProcessingError Then
                'Zip the output file
                result = ZipMSGFDBResults(ResultsFileName)
                If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                    blnProcessingError = True
                End If
            End If

            If Not blnProcessingError Then
                ' Create the Peptide to Protein map file
                result = CreatePeptideToProteinMapping(ResultsFileName)
                If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS And result <> IJobParams.CloseOutType.CLOSEOUT_NO_DATA Then
                    blnProcessingError = True
                End If
            End If


            'Stop the job timer
            m_StopTime = System.DateTime.Now

            If blnProcessingError Then
                ' Something went wrong
                ' In order to help diagnose things, we will move whatever files were created into the result folder, 
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

            If blnProcessingError Or result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                ' Move the source files and any results to the Failed Job folder
                ' Useful for debugging MSGFDB problems
                CopyFailedResultsToArchiveFolder()
                Return result
            End If

            result = MakeResultsFolder()
            If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                'MakeResultsFolder handles posting to local log, so set database error message and exit
                m_message = "Error making results folder"
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            result = MoveResultFiles()
            If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                'MoveResultFiles moves the result files to the result folder
                m_message = "Error moving files into results folder"
                eReturnCode = IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            result = CopyResultsFolderToServer()
            If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                ' Note that CopyResultsFolderToServer should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
                Return result
            End If

        Catch ex As Exception
            m_message = "Error in InspectPlugin->RunTool: " & ex.Message
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS 'No failures so everything must have succeeded

    End Function

    Protected Sub CopyFailedResultsToArchiveFolder()

        Dim result As IJobParams.CloseOutType

        Dim strFailedResultsFolderPath As String = m_mgrParams.GetParam("FailedResultsFolderPath")
        If String.IsNullOrWhiteSpace(strFailedResultsFolderPath) Then strFailedResultsFolderPath = "??Not Defined??"

        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Processing interrupted; copying results to archive folder: " & strFailedResultsFolderPath)

        ' Bump up the debug level if less than 2
        If m_DebugLevel < 2 Then m_DebugLevel = 2

        ' Try to save whatever files are in the work directory (however, delete the _DTA.txt and _DTA.zip files first)
        Dim strFolderPathToArchive As String
        strFolderPathToArchive = String.Copy(m_WorkDir)

        Try
            System.IO.File.Delete(System.IO.Path.Combine(m_WorkDir, m_Dataset & "_dta.zip"))
            System.IO.File.Delete(System.IO.Path.Combine(m_WorkDir, m_Dataset & "_dta.txt"))
        Catch ex As Exception
            ' Ignore errors here
        End Try

        ' Make the results folder
        result = MakeResultsFolder()
        If result = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            ' Move the result files into the result folder
            result = MoveResultFiles()
            If result = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                ' Move was a success; update strFolderPathToArchive
                strFolderPathToArchive = System.IO.Path.Combine(m_WorkDir, m_ResFolderName)
            End If
        End If

        ' Copy the results folder to the Archive folder
        Dim objAnalysisResults As clsAnalysisResults = New clsAnalysisResults(m_mgrParams, m_jobParams)
        objAnalysisResults.CopyFailedResultsToArchiveFolder(strFolderPathToArchive)


    End Sub

    Private Function CreatePeptideToProteinMapping(ResultsFileName As String) As IJobParams.CloseOutType

        Dim OrgDbDir As String = m_mgrParams.GetParam("orgdbdir")

        ' Note that job parameter "generatedFastaName" gets defined by clsAnalysisResources.RetrieveOrgDB
        Dim dbFilename As String = System.IO.Path.Combine(OrgDbDir, m_jobParams.GetParam("generatedFastaName"))
        Dim strInputFilePath As String

        Dim blnIgnorePeptideToProteinMapperErrors As Boolean
        Dim blnSuccess As Boolean

        UpdateStatusRunning(PROGRESS_PCT_MSGFDB_MAPPING_PEPTIDES_TO_PROTEINS)

        strInputFilePath = System.IO.Path.Combine(m_WorkDir, ResultsFileName)

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
                clsGlobal.m_Completions_Msg = "No results above threshold"
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "No results above threshold; MSGF-DB results file is empty")
                Return IJobParams.CloseOutType.CLOSEOUT_NO_DATA
            End If

        Catch ex As Exception

            m_message = "Error validating MSGF-DB results file contents in CreatePeptideToProteinMapping"

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
                .DeleteTempFiles = True
                .IgnoreILDifferences = False
                .InspectParameterFilePath = String.Empty

                If m_DebugLevel > 2 Then
                    .LogMessagesToFile = True
                    .LogFolderPath = m_WorkDir
                Else
                    .LogMessagesToFile = False
                End If

                .MatchPeptidePrefixAndSuffixToProtein = False
                .OutputProteinSequence = False
                .PeptideInputFileFormat = PeptideToProteinMapEngine.clsPeptideToProteinMapEngine.ePeptideInputFileFormatConstants.MSGFDBResultsFile
                .PeptideFileSkipFirstLine = False
                .ProteinDataRemoveSymbolCharacters = True
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

            m_message = "Error in CreatePeptideToProteinMapping"

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "CreatePeptideToProteinMapping, Error running clsPeptideToProteinMapEngine, job " & _
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

    Protected Function GetMSFGDBParameterNames() As System.Collections.Generic.Dictionary(Of String, String)
        Dim dctParamNames As System.Collections.Generic.Dictionary(Of String, String)
        dctParamNames = New System.Collections.Generic.Dictionary(Of String, String)(25)

        dctParamNames.Add("PMTolerance", "t")
        dctParamNames.Add("TDA", "tda")
        dctParamNames.Add("FragmentationMethodID", "m")
        dctParamNames.Add("InstrumentID", "inst")
        dctParamNames.Add("EnzymeID", "e")
        dctParamNames.Add("C13", "c13")
        dctParamNames.Add("NNET", "nnet")
        dctParamNames.Add("minLength", "minLength")
        dctParamNames.Add("maxLength", "maxLength")
        dctParamNames.Add("minCharge", "minCharge")
        dctParamNames.Add("maxCharge", "maxCharge")
        dctParamNames.Add("NumMatchesPerSpec", "n")

        ' The following are special cases; 
        ' do not add to dctParamNames
        '   uniformAAProb
        '   NumThreads
        '   NumMods
        '   StaticMod
        '   DynamicMod

        Return dctParamNames
    End Function

    ''' <summary>
    ''' Compare to strings (not case sensitive)
    ''' </summary>
    ''' <param name="strText1"></param>
    ''' <param name="strText2"></param>
    ''' <returns>True if they match; false if not</returns>
    ''' <remarks></remarks>
    Protected Function IsMatch(ByVal strText1 As String, ByVal strText2 As String) As Boolean
        If String.Compare(strText1, strText2, True) = 0 Then
            Return True
        Else
            Return False
        End If
    End Function



    ''' <summary>
    ''' Parse the MSGFDB console output file to determine the MSGFDB version and to track the search progress
    ''' </summary>
    ''' <param name="strConsoleOutputFilePath"></param>
    ''' <remarks></remarks>
    Private Sub ParseConsoleOutputFile(ByVal strConsoleOutputFilePath As String)

        ' Example Console output:
        '
        ' Using 2 threads.
        ' Suffix array loading... 1.355 sec
        ' Spectrum 0-17169 (total: 17170)
        ' pool-1-thread-2: Preprocessing spectra... 111.000 sec
        ' pool-1-thread-1: Preprocessing spectra... 152.000 sec
        ' pool-1-thread-1: Database search... 93.000 sec
        ' pool-1-thread-2: Database search... 159.000 sec
        ' pool-1-thread-1: Computing spectral probabilities... 146.000 sec
        ' pool-1-thread-1: Generating results... 0.000 sec
        ' pool-1-thread-2: Computing spectral probabilities... 303.000 sec
        ' pool-1-thread-2: Generating results... 0.000 sec
        ' Computing EFDRs... 0.0070 sec
        ' Writing results... 0.662 sec
        ' Time: 939.998 sec


        Static reExtractThreadCount As System.Text.RegularExpressions.Regex = New System.Text.RegularExpressions.Regex("Using (\d+) thread", _
                                                                                                          Text.RegularExpressions.RegexOptions.Compiled Or _
                                                                                                          Text.RegularExpressions.RegexOptions.IgnoreCase)

        Static reExtractThreadNum As System.Text.RegularExpressions.Regex = New System.Text.RegularExpressions.Regex("thread-(\d+)", _
                                                                                                                  Text.RegularExpressions.RegexOptions.Compiled Or _
                                                                                                                  Text.RegularExpressions.RegexOptions.IgnoreCase)
        Try

            If Not System.IO.File.Exists(strConsoleOutputFilePath) Then
                If m_DebugLevel >= 4 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Console output file not found: " & strConsoleOutputFilePath)
                End If

                Exit Sub
            End If

            If m_DebugLevel >= 4 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Parsing file " & strConsoleOutputFilePath)
            End If


            Dim srInFile As System.IO.StreamReader
            Dim strLineIn As String
            Dim intLinesRead As Integer

            Dim oThreadsComputingSpecProbs As System.Collections.Generic.List(Of Short) = New System.Collections.Generic.List(Of Short)
            Dim oMatch As System.Text.RegularExpressions.Match
            Dim intThreadCount As Short = 0

            srInFile = New System.IO.StreamReader(New System.IO.FileStream(strConsoleOutputFilePath, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.ReadWrite))

            intLinesRead = 0
            Do While srInFile.Peek() >= 0
                strLineIn = srInFile.ReadLine()
                intLinesRead += 1

                If Not String.IsNullOrWhiteSpace(strLineIn) Then


                    ' Update progress if the line starts with one of the expected phrases
                    If strLineIn.StartsWith("Using") Then

                        ' Extract out the thread number
                        oMatch = reExtractThreadCount.Match(strLineIn)

                        If oMatch.Success Then
                            Short.TryParse(oMatch.Groups(1).Value, intThreadCount)
                        End If

                    ElseIf strLineIn.StartsWith("Suffix array loading") Then
                        If m_progress < PROGRESS_PCT_MSGFDB_LOADING_PROTEINS Then
                            m_progress = PROGRESS_PCT_MSGFDB_LOADING_PROTEINS
                        End If

                    ElseIf strLineIn.Contains("Preprocessing spectra") Then
                        If m_progress < PROGRESS_PCT_MSGFDB_PREPROCESSING_SPECTRA Then
                            m_progress = PROGRESS_PCT_MSGFDB_PREPROCESSING_SPECTRA
                            If m_DebugLevel >= 3 Then
                                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, " ... " & m_progress.ToString("0") & "%: Preprocessing spectra")
                            End If
                        End If

                    ElseIf strLineIn.Contains("Database search") Then
                        If m_progress < PROGRESS_PCT_MSGFDB_SEARCHING_DATABASE Then
                            m_progress = PROGRESS_PCT_MSGFDB_SEARCHING_DATABASE
                            If m_DebugLevel >= 3 Then
                                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, " ... " & m_progress.ToString("0") & "%: Searching database")
                            End If
                        End If

                    ElseIf strLineIn.Contains("Computing spectral probabilities") Then
                        If m_progress < PROGRESS_PCT_MSGFDB_COMPUTING_SPECTRAL_PROBABILITIES Then
                            m_progress = PROGRESS_PCT_MSGFDB_COMPUTING_SPECTRAL_PROBABILITIES
                            If m_DebugLevel >= 3 Then
                                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, " ... " & m_progress.ToString("0") & "%: Computing spectral probabilities")
                            End If
                        End If

                        ' Extract out the thread number
                        oMatch = reExtractThreadNum.Match(strLineIn)

                        If oMatch.Success Then
                            Dim intThread As Short
                            If Short.TryParse(oMatch.Groups(1).Value, intThread) Then
                                If Not oThreadsComputingSpecProbs.Contains(intThread) Then
                                    oThreadsComputingSpecProbs.Add(intThread)
                                End If
                            End If
                        End If

                    ElseIf strLineIn.StartsWith("Computing EFDRs") Then
                        m_progress = PROGRESS_PCT_MSGFDB_COMPUTING_EFDRS

                    ElseIf strLineIn.StartsWith("Writing results") Then
                        m_progress = PROGRESS_PCT_MSGFDB_WRITING_RESULTS

                    End If
                End If
            Loop

            srInFile.Close()

            If m_progress >= PROGRESS_PCT_MSGFDB_COMPUTING_SPECTRAL_PROBABILITIES And _
                m_progress < PROGRESS_PCT_MSGFDB_COMPUTING_EFDRS Then

                Dim sngNewProgress As Single = PROGRESS_PCT_MSGFDB_COMPUTING_SPECTRAL_PROBABILITIES

                ' Incrementally adjust m_progress based on the number of items in oThreadsComputingSpecProbs
                If intThreadCount > 0 And oThreadsComputingSpecProbs.Count > 1 Then
                    Dim sngFraction As Single
                    Dim sngAddOn As Single

                    sngFraction = oThreadsComputingSpecProbs.Count / CSng(intThreadCount)
                    sngAddOn = (PROGRESS_PCT_MSGFDB_COMPUTING_EFDRS - PROGRESS_PCT_MSGFDB_COMPUTING_SPECTRAL_PROBABILITIES) * sngFraction

                    sngNewProgress += sngAddOn

                    If sngNewProgress > m_progress Then
                        m_progress = sngNewProgress
                        If m_DebugLevel >= 3 Then
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, " ... " & m_progress.ToString("0") & "%: Computing spectral probabilities")
                        End If
                    End If

                End If
            End If

        Catch ex As Exception
            ' Ignore errors here
            If m_DebugLevel >= 2 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error parsing console output file (" & strConsoleOutputFilePath & "): " & ex.Message)
            End If
        End Try

    End Sub

    ''' <summary>
    ''' Parses the static and dynamic modification information to create the MSGFDB Mods file
    ''' </summary>
    ''' <param name="strParameterFilePath">Full path to the MSGF parameter file; will create file MSGFDB_Mods.txt in the same folder</param>
    ''' <param name="sbOptions">String builder of command line arguments to pass to MSGFDB</param>
    ''' <param name="intNumMods">Max Number of Modifications per peptide</param>
    ''' <param name="lstStaticMods">List of Static Mods</param>
    ''' <param name="lstDynamicMods">List of Dynamic Mods</param>
    ''' <returns>True if success, false if an error</returns>
    ''' <remarks></remarks>
    Protected Function ParseMSGFDBModifications(ByVal strParameterFilePath As String, _
                                                ByRef sbOptions As System.Text.StringBuilder, _
                                                ByVal intNumMods As Integer, _
                                                ByRef lstStaticMods As System.Collections.Generic.List(Of String), _
                                                ByRef lstDynamicMods As System.Collections.Generic.List(Of String)) As Boolean

        Const MOD_FILE_NAME As String = "MSGFDB_Mods.txt"
        Dim blnSuccess As Boolean
        Dim strModFilePath As String
        Dim swModFile As System.IO.StreamWriter = Nothing

        Try
            Dim fiParameterFile As System.IO.FileInfo
            fiParameterFile = New System.IO.FileInfo(strParameterFilePath)

            strModFilePath = System.IO.Path.Combine(fiParameterFile.DirectoryName, MOD_FILE_NAME)

            sbOptions.Append(" -mod " & MOD_FILE_NAME)

            swModFile = New System.IO.StreamWriter(New System.IO.FileStream(strModFilePath, IO.FileMode.Create, IO.FileAccess.Write, IO.FileShare.Read))

            swModFile.WriteLine("# This file is used to specify modifications for MSGFDB")
            swModFile.WriteLine("")
            swModFile.WriteLine("# Max Number of Modifications per peptide")
            swModFile.WriteLine("# If this value is large, the search will be slow")
            swModFile.WriteLine("NumMods=" & intNumMods)

            swModFile.WriteLine("")
            swModFile.WriteLine("# Static mods")
            If lstStaticMods.Count = 0 Then
                swModFile.WriteLine("# None")
            Else
                For Each strStaticMod As String In lstStaticMods
                    Dim strModClean As String = String.Empty

                    If ParseMSGFValidateMod(strStaticMod, strModClean) Then
                        If strModClean.Contains(",opt,") Then
                            ' Auto-change this mod to a static (fixed) mod
                            strModClean.Replace(",opt,", ",fix,")
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Updated static mod to contain ',fix,' instead of ',opt,': " & strStaticMod)
                        End If
                        swModFile.WriteLine(strModClean)
                    Else
                        Return False
                    End If
                Next
            End If

            swModFile.WriteLine("")
            swModFile.WriteLine("# Dynamic mods")
            If lstDynamicMods.Count = 0 Then
                swModFile.WriteLine("# None")
            Else
                For Each strDynamicMod As String In lstDynamicMods
                    Dim strModClean As String = String.Empty

                    If ParseMSGFValidateMod(strDynamicMod, strModClean) Then
                        If strModClean.Contains(",fix,") Then
                            ' Auto-change this mod to a dynamic (optional) mod
                            strModClean.Replace(",fix,", ",opt,")
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Updated dynamic mod to contain ',opt,' instead of ',fix,': " & strDynamicMod)
                        End If
                        swModFile.WriteLine(strModClean)
                    Else
                        Return False
                    End If
                Next
            End If

            blnSuccess = True

        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception creating MSGFDB Mods file: " & ex.Message)
            blnSuccess = False
        Finally
            If Not swModFile Is Nothing Then
                swModFile.Close()
            End If
        End Try

        Return blnSuccess

    End Function

    ''' <summary>
    ''' Read the MSGFDB options file and convert the options to command line switches
    ''' </summary>
    ''' <param name="strParameterFilePath">Path to the MSGFDB Parameter File</param>
    ''' <param name="FastaFileSizeKB">Size of the .Fasta file, in KB</param>
    ''' <returns>Options string if success; empty string if an error</returns>
    ''' <remarks></remarks>
    Protected Function ParseMSGFDBParameterFile(ByVal strParameterFilePath As String, ByVal FastaFileSizeKB As Single) As String
        Const DEFINE_MAX_THREADS As Boolean = True
        Const SMALL_FASTA_FILE_THRESHOLD_KB As Integer = 20

        Dim sbOptions As System.Text.StringBuilder
        Dim srParamFile As System.IO.StreamReader
        Dim strLineIn As String

        Dim strKey As String
        Dim strValue As String
        Dim intValue As Integer

        Dim intParamFileThreadCount As Integer = 0
        Dim intDMSDefinedThreadCount As Integer = 0

        Dim dctParamNames As System.Collections.Generic.Dictionary(Of String, String)

        Dim intNumMods As Integer = 0
        Dim lstStaticMods As System.Collections.Generic.List(Of String) = New System.Collections.Generic.List(Of String)
        Dim lstDynamicMods As System.Collections.Generic.List(Of String) = New System.Collections.Generic.List(Of String)

        sbOptions = New System.Text.StringBuilder(500)


        Try

            ' Initialize the Param Name dictionary
            dctParamNames = GetMSFGDBParameterNames()

            srParamFile = New System.IO.StreamReader(New System.IO.FileStream(strParameterFilePath, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.Read))

            Do While srParamFile.Peek >= 0
                strLineIn = srParamFile.ReadLine()
                strKey = String.Empty
                strValue = String.Empty

                If Not String.IsNullOrWhiteSpace(strLineIn) Then
                    strLineIn = strLineIn.Trim()

                    If Not strLineIn.StartsWith("#") AndAlso strLineIn.Contains("="c) Then

                        Dim intCharIndex As Integer
                        intCharIndex = strLineIn.IndexOf("=")
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

                    ElseIf IsMatch(strKey, "uniformAAProb") Then
                        If String.IsNullOrWhiteSpace(strValue) OrElse IsMatch(strValue, "auto") Then
                            If FastaFileSizeKB < SMALL_FASTA_FILE_THRESHOLD_KB Then
                                sbOptions.Append(" -uniformAAProb 1")
                            Else
                                sbOptions.Append(" -uniformAAProb 0")
                            End If
                        Else
                            If Integer.TryParse(strValue, intValue) Then
                                sbOptions.Append(" -uniformAAProb " & intValue)
                            Else
                                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Invalid value for uniformAAProb in MSGFDB parameter file: " & strLineIn)
                            End If
                        End If

                    ElseIf DEFINE_MAX_THREADS AndAlso IsMatch(strKey, "NumThreads") Then
                        If String.IsNullOrWhiteSpace(strValue) OrElse IsMatch(strValue, "all") Then
                            ' Do not append -thread to the command line; MSGFDB will use all available cores by default
                        Else
                            If Integer.TryParse(strValue, intParamFileThreadCount) Then
                                ' intParamFileThreadCount now has the thread count
                            Else
                                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Invalid value for NumThreads in MSGFDB parameter file: " & strLineIn)
                            End If
                        End If


                    ElseIf IsMatch(strKey, "NumMods") Then
                        If Integer.TryParse(strValue, intValue) Then
                            intNumMods = intValue
                        Else
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Invalid value for NumMods in MSGFDB parameter file: " & strLineIn)
                        End If

                    ElseIf IsMatch(strKey, "StaticMod") Then
                        If Not IsMatch(strValue, "none") Then
                            lstStaticMods.Add(strValue)
                        End If

                    ElseIf IsMatch(strKey, "DynamicMod") Then
                        If Not IsMatch(strValue, "none") Then
                            lstDynamicMods.Add(strValue)
                        End If
                    End If

                End If
            Loop

            srParamFile.Close()

        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception reading MSGFDB parameter file: " & ex.Message)
            Return String.Empty
        End Try

        ' Define the thread count
        intDMSDefinedThreadCount = clsGlobal.GetJobParameter(m_jobParams, "MSGFDBThreads", 0)

        If intDMSDefinedThreadCount > 0 Then
            intParamFileThreadCount = intDMSDefinedThreadCount
        End If

        If intParamFileThreadCount > 0 Then
            sbOptions.Append(" -thread " & intParamFileThreadCount)
        End If

        ' Create the modification file and append the -mod switch
        If ParseMSGFDBModifications(strParameterFilePath, sbOptions, intNumMods, lstStaticMods, lstDynamicMods) Then
            Return sbOptions.ToString()
        Else
            Return String.Empty
        End If


    End Function

    ''' <summary>
    ''' Validates that the modification definition text
    ''' </summary>
    ''' <param name="strMod">Modification definition</param>
    ''' <param name="strModClean">Cleaned-up modification definition (output param)</param>
    ''' <returns>True if valid; false if invalid</returns>
    ''' <remarks>Valid modification definition contains 5 parts and doesn't contain any whitespace</remarks>
    Protected Function ParseMSGFValidateMod(ByVal strMod As String, ByRef strModClean As String) As Boolean

        Dim intPoundIndex As Integer
        Dim strSplitMod() As String

        Dim strComment As String = String.Empty

        strModClean = String.Empty

        intPoundIndex = strMod.IndexOf("#"c)
        If intPoundIndex > 0 Then
            strComment = strMod.Substring(intPoundIndex)
            strMod = strMod.Substring(0, intPoundIndex - 1).Trim
        End If

        strSplitMod = strMod.Split(","c)

        If strSplitMod.Length < 5 Then
            ' Invalid mod definition; must have 5 sections
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Invalid modification string; must have 5 sections: " & strMod)
            Return False
        End If

        ' Reconstruct the mod definition, making sure there is no whitespace
        strModClean = strSplitMod(0).Trim()
        For intIndex As Integer = 1 To strSplitMod.Length - 1
            strModClean &= "," & strSplitMod(intIndex).Trim()
        Next

        If Not String.IsNullOrWhiteSpace(strComment) Then
            strModClean &= "     " & strComment
        End If

        Return True

    End Function

    ''' <summary>
    ''' 
    ''' </summary>
    ''' <param name="sbOptions"></param>
    ''' <param name="strKeyName"></param>
    ''' <param name="strValue"></param>
    ''' <param name="strParameterName"></param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Protected Function ParseMSFDBParamLine(ByRef sbOptions As System.Text.StringBuilder, _
                                           ByVal strKeyName As String, _
                                           ByVal strValue As String, _
                                           ByVal strParameterName As String) As Boolean

        Dim strCommandLineSwitchName As String = strParameterName

        Return ParseMSFDBParamLine(sbOptions, strKeyName, strValue, strParameterName, strCommandLineSwitchName)

    End Function

    Protected Function ParseMSFDBParamLine(ByRef sbOptions As System.Text.StringBuilder, _
                                           ByVal strKeyName As String, _
                                           ByVal strValue As String, _
                                           ByVal strParameterName As String, _
                                           ByVal strCommandLineSwitchName As String) As Boolean

        If IsMatch(strKeyName, strParameterName) Then
            sbOptions.Append(" -" & strCommandLineSwitchName & " " & strValue)
            Return True
        Else
            Return False
        End If


    End Function

    ''' <summary>
    ''' Stores the tool version info in the database
    ''' </summary>
    ''' <remarks></remarks>
    Protected Function StoreToolVersionInfo(ByVal MSGFDBProgLoc As String) As Boolean

        Dim strToolVersionInfo As String = String.Empty
        Dim ioAppFileInfo As System.IO.FileInfo = New System.IO.FileInfo(System.Reflection.Assembly.GetExecutingAssembly().Location)

        If m_DebugLevel >= 2 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info")
        End If

        ' Store paths to key files in ioToolFiles
        Dim ioToolFiles As New System.Collections.Generic.List(Of System.IO.FileInfo)
        ioToolFiles.Add(New System.IO.FileInfo(MSGFDBProgLoc))

        Try
            Return MyBase.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles)
        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion: " & ex.Message)
            Return False
        End Try

    End Function

    Private Sub UpdateStatusRunning(ByVal sngPercentComplete As Single)
        m_progress = sngPercentComplete
        m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.RUNNING_TOOL, sngPercentComplete, 0, "", "", "", False)
    End Sub

    ''' <summary>
    ''' Make sure the _DTA.txt file exists and has at least one spectrum in it
    ''' </summary>
    ''' <returns>True if success; false if failure</returns>
    ''' <remarks></remarks>
    Protected Function ValidateCDTAFile() As Boolean
        Dim strInputFilePath As String
        Dim srReader As System.IO.StreamReader

        Dim blnDataFound As Boolean = False

        Try
            strInputFilePath = System.IO.Path.Combine(m_WorkDir, m_Dataset & "_dta.txt")

            If Not System.IO.File.Exists(strInputFilePath) Then
                m_message = "_DTA.txt file not found: " & strInputFilePath
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
                Return False
            End If

            srReader = New System.IO.StreamReader(New System.IO.FileStream(strInputFilePath, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.ReadWrite))

            Do While srReader.Peek >= 0
                If srReader.ReadLine.Trim.Length > 0 Then
                    blnDataFound = True
                    Exit Do
                End If
            Loop

            srReader.Close()

            If Not blnDataFound Then
                m_message = "The _DTA.txt file is empty"
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
            End If

        Catch ex As Exception
            m_message = "Exception in ValidateCDTAFile"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message)
            Return False
        End Try

        Return blnDataFound

    End Function

    ''' <summary>
    ''' Zips MSGFDB Output File
    ''' </summary>
    ''' <returns>CloseOutType enum indicating success or failure</returns>
    ''' <remarks></remarks>
    Private Function ZipMSGFDBResults(ResultsFileName As String) As IJobParams.CloseOutType
        Dim TmpFilePath As String

        Try

            TmpFilePath = System.IO.Path.Combine(m_WorkDir, ResultsFileName)
            If Not MyBase.ZipFile(TmpFilePath, False) Then
                Dim Msg As String = "Error zipping output files, job " & m_JobNum
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
                m_message = clsGlobal.AppendToComment(m_message, "Error zipping output files")
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            ' Add the _msgfdb.txt file to .FilesToDelete since we only want to keep the Zipped version
            clsGlobal.FilesToDelete.Add(ResultsFileName)

        Catch ex As Exception
            Dim Msg As String = "clsAnalysisToolRunnerMSGFDB.ZipMSGFDBResults, Exception zipping output files, job " & m_JobNum & ": " & ex.Message
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
            m_message = clsGlobal.AppendToComment(m_message, "Error zipping output files")
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

#End Region

#Region "Event Handlers"

    ''' <summary>
    ''' Event handler for CmdRunner.LoopWaiting event
    ''' </summary>
    ''' <remarks></remarks>
    Private Sub CmdRunner_LoopWaiting() Handles CmdRunner.LoopWaiting
        Static dtLastStatusUpdate As System.DateTime = System.DateTime.Now
        Static dtLastConsoleOutputParse As System.DateTime = System.DateTime.Now

        ' Synchronize the stored Debug level with the value stored in the database
        Const MGR_SETTINGS_UPDATE_INTERVAL_SECONDS As Integer = 300
        MyBase.GetCurrentMgrSettingsFromDB(MGR_SETTINGS_UPDATE_INTERVAL_SECONDS)

        'Update the status file (limit the updates to every 5 seconds)
        If System.DateTime.Now.Subtract(dtLastStatusUpdate).TotalSeconds >= 5 Then
            dtLastStatusUpdate = System.DateTime.Now
            UpdateStatusRunning(m_progress)
        End If

        If System.DateTime.Now.Subtract(dtLastConsoleOutputParse).TotalSeconds >= 15 Then
            dtLastConsoleOutputParse = System.DateTime.Now

            ParseConsoleOutputFile(System.IO.Path.Combine(m_WorkDir, MSGFDB_CONSOLE_OUTPUT))

        End If

    End Sub

    Private Sub mPeptideToProteinMapper_ProgressChanged(ByVal taskDescription As String, ByVal percentComplete As Single) Handles mPeptideToProteinMapper.ProgressChanged

        ' Note that percentComplete is a value between 0 and 100

        Const STATUS_UPDATE_INTERVAL_SECONDS As Integer = 5
        Const MAPPER_PROGRESS_LOG_INTERVAL_SECONDS As Integer = 120

        Static dtLastStatusUpdate As System.DateTime
        Static dtLastLogTime As System.DateTime

        Dim sngStartPercent As Single = PROGRESS_PCT_MSGFDB_MAPPING_PEPTIDES_TO_PROTEINS
        Dim sngEndPercent As Single = PROGRESS_PCT_MSGFDB_COMPLETE
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
