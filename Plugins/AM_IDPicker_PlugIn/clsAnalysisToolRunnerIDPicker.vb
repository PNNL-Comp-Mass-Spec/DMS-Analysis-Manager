Option Strict On

Imports AnalysisManagerBase

Imports System.Collections.Concurrent
Imports System.IO
Imports System.Runtime.InteropServices
Imports PHRPReader

Public Class clsAnalysisToolRunnerIDPicker
    Inherits clsAnalysisToolRunnerBase

    '*********************************************************************************************************
    'Class for running IDPicker
    '*********************************************************************************************************

#Region "Module Variables"

    Public Const ALWAYS_SKIP_IDPICKER = True

    Private Const PEPXML_CONSOLE_OUTPUT As String = "PepXML_ConsoleOutput.txt"

    Private Const IPD_Qonvert_CONSOLE_OUTPUT As String = "IDPicker_Qonvert_ConsoleOutput.txt"
    Private Const IPD_Assemble_CONSOLE_OUTPUT As String = "IDPicker_Assemble_ConsoleOutput.txt"
    Private Const IPD_Report_CONSOLE_OUTPUT As String = "IDPicker_Report_ConsoleOutput.txt"

    Private Const IDPicker_Qonvert As String = "idpQonvert.exe"
    Private Const IDPicker_Assemble As String = "idpAssemble.exe"
    Private Const IDPicker_Report As String = "idpReport.exe"
    Private Const IDPicker_GUI As String = "IdPickerGui.exe"

    Private Const ASSEMBLE_GROUPING_FILENAME As String = "Assemble.txt"
    Private Const ASSEMBLE_OUTPUT_FILENAME As String = "IDPicker_AssembledResults.xml"

    Private Const MSGFDB_DECOY_PROTEIN_PREFIX As String = "REV_"
    Private Const MSGFPLUS_DECOY_PROTEIN_PREFIX As String = "XXX_"

    Private Const PEPTIDE_LIST_TO_XML_EXE As String = "PeptideListToXML.exe"

    Private Const PROGRESS_PCT_IDPicker_STARTING As Single = 1
    Private Const PROGRESS_PCT_IDPicker_SEARCHING_FOR_FILES As Single = 5
    Private Const PROGRESS_PCT_IDPicker_CREATING_PEPXML_FILE As Single = 10
    Private Const PROGRESS_PCT_IDPicker_RUNNING_IDPQonvert As Single = 20
    Private Const PROGRESS_PCT_IDPicker_RUNNING_IDPAssemble As Single = 60
    Private Const PROGRESS_PCT_IDPicker_RUNNING_IDPReport As Single = 70
    Private Const PROGRESS_PCT_IDPicker_COMPLETE As Single = 95
    Private Const PROGRESS_PCT_COMPLETE As Single = 99

    Private mIDPickerProgramFolder As String = String.Empty
    Private mIDPickerParamFileNameLocal As String = String.Empty

    Private mPeptideListToXMLExePath As String = String.Empty
    Private mPepXMLFilePath As String = String.Empty
    Private mIdpXMLFilePath As String = String.Empty
    Private mIdpAssembleFilePath As String = String.Empty

    Private mIDPickerOptions As Dictionary(Of String, String)

    ' This variable holds the name of the program that is currently running via CmdRunner
    Private mCmdRunnerDescription As String = String.Empty

    ' This list tracks the error messages reported by CmdRunner
    Private mCmdRunnerErrors As ConcurrentBag(Of String)

    ' This list tracks error message text that we look for when considering whether or not to ignore an error message
    Private mCmdRunnerErrorsToIgnore As ConcurrentBag(Of String)

    ' This list tracks files that we want to include in the zipped up IDPicker report folder
    Private mFilenamesToAddToReportFolder As List(Of String)
    Private mBatchFilesMoved As Boolean

    Private WithEvents CmdRunner As clsRunDosProgram
#End Region

#Region "Structures"

#End Region

#Region "Methods"

    ''' <summary>
    ''' Runs PepXML converter and IDPicker tool
    ''' </summary>
    ''' <returns>CloseOutType enum indicating success or failure</returns>
    ''' <remarks></remarks>
    Public Overrides Function RunTool() As IJobParams.CloseOutType

        Dim OrgDbDir As String
        Dim strFASTAFilePath As String
        Dim strResultType As String
        Dim strSynFilePath As String
        Dim strErrorMessage As String = String.Empty

        Dim ePHRPResultType As clsPHRPReader.ePeptideHitResultType

        Dim result As IJobParams.CloseOutType

        ' As of January 21, 2015 we are now always skipping IDPicker (and thus simply creating the .pepXML file)
        Dim blnSkipIDPicker As Boolean = ALWAYS_SKIP_IDPICKER

        Dim blnProcessingError = False

        Dim blnSuccess As Boolean

        mIDPickerOptions = New Dictionary(Of String, String)(StringComparer.CurrentCultureIgnoreCase)
        mCmdRunnerErrors = New ConcurrentBag(Of String)
        mCmdRunnerErrorsToIgnore = New ConcurrentBag(Of String)
        mFilenamesToAddToReportFolder = New List(Of String)

        Try
            'Call base class for initial setup
            If Not MyBase.RunTool = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            If m_DebugLevel > 4 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerIDPicker.RunTool(): Enter")
            End If

            m_progress = PROGRESS_PCT_IDPicker_SEARCHING_FOR_FILES

            ' Determine the path to the IDPicker program (idpQonvert); folder will also contain idpAssemble.exe and idpReport.exe
            Dim progLocQonvert As String = String.Empty
            If Not blnSkipIDPicker Then
                progLocQonvert = DetermineProgramLocation("IDPicker", "IDPickerProgLoc", IDPicker_Qonvert)

                If String.IsNullOrWhiteSpace(progLocQonvert) Then
                    Return IJobParams.CloseOutType.CLOSEOUT_FAILED
                End If
            End If

            ' Determine the result type
            strResultType = m_jobParams.GetParam("ResultType")

            ePHRPResultType = clsPHRPReader.GetPeptideHitResultType(strResultType)
            If ePHRPResultType = clsPHRPReader.ePeptideHitResultType.Unknown Then
                m_message = "Invalid tool result type (not supported by IDPicker): " & strResultType
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            ' Define the path to the synopsis file
            strSynFilePath = Path.Combine(m_WorkDir, clsPHRPReader.GetPHRPSynopsisFileName(ePHRPResultType, m_Dataset))
            If Not File.Exists(strSynFilePath) Then
                Dim alternateFilePath = clsPHRPReader.AutoSwitchToLegacyMSGFDBIfRequired(strSynFilePath, "Dataset_msgfdb.txt")
                If File.Exists(alternateFilePath) Then
                    strSynFilePath = alternateFilePath
                End If
            End If

            If Not clsAnalysisResources.ValidateFileHasData(strSynFilePath, "Synopsis file", strErrorMessage) Then
                ' The synopsis file is empty
                m_message = strErrorMessage
                Return IJobParams.CloseOutType.CLOSEOUT_NO_DATA
            End If

            ' Define the path to the fasta file
            OrgDbDir = m_mgrParams.GetParam("orgdbdir")
            strFASTAFilePath = Path.Combine(OrgDbDir, m_jobParams.GetParam("PeptideSearch", "generatedFastaName"))

            Dim fiFastaFile = New FileInfo(strFASTAFilePath)

            If Not blnSkipIDPicker AndAlso Not fiFastaFile.Exists Then
                ' Fasta file not found
                m_message = "Fasta file not found: " & fiFastaFile.Name
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Fasta file not found: " & fiFastaFile.FullName)
                Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
            End If

            Dim blnSplitFasta = m_jobParams.GetJobParameter("SplitFasta", False)

            If Not blnSkipIDPicker AndAlso blnSplitFasta Then
                blnSkipIDPicker = True
                m_EvalMessage = "SplitFasta jobs typically have fasta files too large for IDPQonvert; skipping IDPicker"
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, m_EvalMessage)
            End If

            ' Store the version of IDPicker and PeptideListToXML in the database
            ' Alternatively, if blnSkipIDPicker is true, then just store the version of PeptideListToXML

            ' This function updates mPeptideListToXMLExePath and mIDPickerProgramFolder
            If Not StoreToolVersionInfo(progLocQonvert, blnSkipIDPicker) Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Aborting since StoreToolVersionInfo returned false")
                m_message = "Error determining IDPicker version"
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            ' Create the PepXML file
            blnSuccess = CreatePepXMLFile(fiFastaFile.FullName, strSynFilePath, ePHRPResultType)
            If Not blnSuccess Then
                If String.IsNullOrEmpty(m_message) Then
                    m_message = "Error creating PepXML file"
                End If
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error creating PepXML file for job " & m_JobNum)
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            If blnSkipIDPicker Then
                ' Don't keep this file since we're skipping IDPicker
                m_jobParams.AddResultFileToSkip("Tool_Version_Info_IDPicker.txt")

                Dim strParamFileNameLocal As String = m_jobParams.GetParam(clsAnalysisResourcesIDPicker.IDPICKER_PARAM_FILENAME_LOCAL)
                If String.IsNullOrEmpty(strParamFileNameLocal) Then
                    m_jobParams.AddResultFileToSkip(clsAnalysisResourcesIDPicker.DEFAULT_IDPICKER_PARAM_FILE_NAME)
                Else
                    m_jobParams.AddResultFileToSkip(strParamFileNameLocal)
                End If

            Else

                Dim blnCriticalError = False

                blnSuccess = RunIDPickerWrapper(ePHRPResultType, strSynFilePath, fiFastaFile.FullName, blnProcessingError, blnCriticalError)

                If blnCriticalError Then
                    Return IJobParams.CloseOutType.CLOSEOUT_FAILED
                End If

                If Not blnSuccess Then blnProcessingError = True

            End If

            If Not blnProcessingError Then
                ' Zip the PepXML file
                ZipPepXMLFile()
            End If

            m_jobParams.AddResultFileExtensionToSkip(".bat")

            m_progress = PROGRESS_PCT_COMPLETE

            'Stop the job timer
            m_StopTime = DateTime.UtcNow

            'Add the current job data to the summary file
            If Not UpdateSummaryFile() Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Error creating summary file, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step"))
            End If

            'Make sure objects are released
            Threading.Thread.Sleep(500)           ' 500 msec delay
            PRISM.Processes.clsProgRunner.GarbageCollectNow()

            If blnProcessingError Or result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                ' Something went wrong
                ' In order to help diagnose things, we will move whatever files were created into the result folder, 
                '  archive it using CopyFailedResultsToArchiveFolder, then return IJobParams.CloseOutType.CLOSEOUT_FAILED

                m_jobParams.RemoveResultFileToSkip(ASSEMBLE_GROUPING_FILENAME)
                m_jobParams.RemoveResultFileToSkip(ASSEMBLE_OUTPUT_FILENAME)

                CopyFailedResultsToArchiveFolder()
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            result = MakeResultsFolder()
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

            If Not blnSkipIDPicker Then
                result = MoveFilesIntoIDPickerSubfolder()
                If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                    ' Note that MoveResultFiles should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
                    m_message = "Error moving files into IDPicker subfolder"
                    Return IJobParams.CloseOutType.CLOSEOUT_FAILED
                End If
            End If

            result = CopyResultsFolderToServer()
            If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                ' Note that CopyResultsFolderToServer should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
                Return result
            End If

        Catch ex As Exception
            m_message = "Exception in IDPickerPlugin->RunTool"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS 'No failures so everything must have succeeded

    End Function

    Private Function RunIDPickerWrapper(
        ePHRPResultType As clsPHRPReader.ePeptideHitResultType,
        strSynFilePath As String,
        fastaFilePath As String,
        <Out> ByRef blnProcessingError As Boolean,
        <Out> ByRef blnCriticalError As Boolean) As Boolean

        Dim blnSuccess As Boolean
        blnProcessingError = False
        blnCriticalError = False

        ' Determine the prefix used by decoy proteins
        Dim strDecoyPrefix As String = String.Empty

        If ePHRPResultType = clsPHRPReader.ePeptideHitResultType.MSGFDB Then
            ' If we run MSGFDB with target/decoy mode and showDecoy=1, then the _syn.txt file will have decoy proteins that start with REV_ or XXX_
            ' Check for this
            blnSuccess = LookForDecoyProteinsInMSGFDBResults(strSynFilePath, ePHRPResultType, strDecoyPrefix)
            If Not blnSuccess Then
                If String.IsNullOrEmpty(m_message) Then
                    m_message = "Error looking for decoy proteins in the MSGFDB synopsis file"
                End If
                blnCriticalError = True
                Return False
            End If
        End If

        If String.IsNullOrEmpty(strDecoyPrefix) Then
            ' Look for decoy proteins in the Fasta file
            blnSuccess = DetermineDecoyProteinPrefix(fastaFilePath, strDecoyPrefix)
            If Not blnSuccess Then
                If String.IsNullOrEmpty(m_message) Then
                    m_message = "Error looking for decoy proteins in the Fasta file"
                End If
                blnCriticalError = True
                Return False
            End If
        End If

        If String.IsNullOrEmpty(strDecoyPrefix) Then
            m_EvalMessage = "No decoy proteins; skipping IDPicker"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, m_EvalMessage)
            Return True
        End If

        ' Load the IDPicker options
        blnSuccess = LoadIDPickerOptions()
        If Not blnSuccess Then
            blnProcessingError = True
            Return False
        End If

        ' Convert the search scores in the pepXML file to q-values
        blnSuccess = RunQonvert(fastaFilePath, strDecoyPrefix, ePHRPResultType)
        If Not blnSuccess Then
            blnProcessingError = True
            Return False
        End If

        ' Organizes the search results into a hierarchy
        blnSuccess = RunAssemble()
        If Not blnSuccess Then
            blnProcessingError = True
            Return False
        End If

        ' Apply parsimony in protein assembly and generate reports
        blnSuccess = RunReport()
        If Not blnSuccess Then
            blnProcessingError = True
            Return False
        Else
            Return True
        End If

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
    ''' Append a new command line argument (appends using ValueIfMissing if not defined in mIDPickerOptions)
    ''' </summary>
    ''' <param name="CmdArgs">Current arguments</param>
    ''' <param name="ArgumentName">Argument Name</param>
    ''' <param name="ValueIfMissing">Value to append if not defined in mIDPickerOptions</param>
    ''' <returns>The new argument list</returns>
    ''' <remarks></remarks>
    Private Function AppendArgument(CmdArgs As String, ArgumentName As String, ValueIfMissing As String) As String
        Const AppendIfMissing = True
        Return AppendArgument(CmdArgs, ArgumentName, ArgumentName, ValueIfMissing, AppendIfMissing)
    End Function


    ''' <summary>
    ''' Append a new command line argument (appends using ValueIfMissing if not defined in mIDPickerOptions)
    ''' </summary>
    ''' <param name="CmdArgs">Current arguments</param>
    ''' <param name="OptionName">Key name to lookup in mIDPickerOptions</param>
    ''' <param name="ArgumentName">Argument Name</param>
    ''' <param name="ValueIfMissing">Value to append if not defined in mIDPickerOptions</param>
    ''' <returns>The new argument list</returns>
    ''' <remarks></remarks>
    Private Function AppendArgument(CmdArgs As String, OptionName As String, ArgumentName As String, ValueIfMissing As String) As String
        Const AppendIfMissing = True
        Return AppendArgument(CmdArgs, OptionName, ArgumentName, ValueIfMissing, AppendIfMissing)
    End Function

    ''' <summary>
    ''' Append a new command line argument
    ''' </summary>
    ''' <param name="CmdArgs">Current arguments</param>
    ''' <param name="OptionName">Key name to lookup in mIDPickerOptions</param>
    ''' <param name="ArgumentName">Argument Name</param>
    ''' <param name="ValueIfMissing">Value to append if not defined in mIDPickerOptions</param>
    ''' <param name="AppendIfMissing">If True, then append the argument using ValueIfMissing if not found in mIDPickerOptions; if false, and not found, then does not append the argument</param>
    ''' <returns>The new argument list</returns>
    ''' <remarks></remarks>
    Private Function AppendArgument(CmdArgs As String, OptionName As String, ArgumentName As String, ValueIfMissing As String, AppendIfMissing As Boolean) As String
        Dim strValue As String = String.Empty
        Dim blnIsMissing As Boolean
        Dim blnAppendParam As Boolean

        If mIDPickerOptions.TryGetValue(OptionName, strValue) Then
            blnIsMissing = False
        Else
            blnIsMissing = True
            strValue = ValueIfMissing
        End If

        If blnIsMissing Then
            blnAppendParam = AppendIfMissing
        Else
            blnAppendParam = True
        End If

        If String.IsNullOrEmpty(CmdArgs) Then
            CmdArgs = String.Empty
        End If

        If blnAppendParam Then
            Return CmdArgs & " -" & ArgumentName & " " & PossiblyQuotePath(strValue)
        Else
            Return CmdArgs
        End If

    End Function

    Private Function CreateAssembleFile(strAssembleFilePath As String) As Boolean

        Dim strDatasetLabel As String

        Try
            ' Prepend strExperiment with PNNL/
            ' Also make sure it doesn't contain any spaces
            strDatasetLabel = "PNNL/" & m_Dataset.Replace(" ", "_")

            ' Create the Assemble.txt file
            Using swOutfile = New StreamWriter(New FileStream(strAssembleFilePath, FileMode.Create, FileAccess.Write, FileShare.Read))
                swOutfile.WriteLine(strDatasetLabel & " " & Path.GetFileName(mIdpXMLFilePath))
            End Using

        Catch ex As Exception
            m_message = "Exception in IDPickerPlugin->CreateAssembleFile"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex)
            Return False
        End Try

        Return True

    End Function

    Private Sub ClearConcurrentBag(ByRef oBag As ConcurrentBag(Of String))
        Dim item As String = String.Empty
        Do While Not oBag.IsEmpty
            oBag.TryTake(item)
        Loop
    End Sub

    ''' <summary>
    ''' Copies a file into folder strReportFolderPath then adds it to m_jobParams.AddResultFileToSkip
    ''' </summary>
    ''' <param name="strFileName"></param>
    ''' <param name="strReportFolderPath"></param>
    ''' <remarks></remarks>
    Private Sub CopyFileIntoReportFolder(strFileName As String, strReportFolderPath As String)
        Dim ioSourceFile As FileInfo

        Try
            ioSourceFile = New FileInfo(Path.Combine(m_WorkDir, strFileName))

            If ioSourceFile.Exists Then
                ioSourceFile.CopyTo(Path.Combine(strReportFolderPath, strFileName), True)
                m_jobParams.AddResultFileToSkip(strFileName)
            End If

        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Error copying ConsoleOutput file into the IDPicker Report folder: " & ex.Message)
        End Try

    End Sub

    Private Function CreatePepXMLFile(strFastaFilePath As String, strSynFilePath As String, ePHRPResultType As clsPHRPReader.ePeptideHitResultType) As Boolean

        Dim strParamFileName As String

        Dim iHitsPerSpectrum As Integer

        Dim CmdStr As String

        ' PepXML file creation should generally be done in less than 10 minutes
        ' However, for huge fasta files, conversion could take several hours
        Const intMaxRuntimeMinutes = 480

        Dim blnSuccess As Boolean

        Try

            'Set up and execute a program runner to run PeptideListToXML
            strParamFileName = m_jobParams.GetParam("ParmFileName")

            mPepXMLFilePath = Path.Combine(m_WorkDir, m_Dataset & ".pepXML")
            iHitsPerSpectrum = m_jobParams.GetJobParameter("PepXMLHitsPerSpectrum", 3)

            CmdStr = PossiblyQuotePath(strSynFilePath) & " /E:" & PossiblyQuotePath(strParamFileName) & " /F:" & PossiblyQuotePath(strFastaFilePath) & " /H:" & iHitsPerSpectrum

            If ePHRPResultType = clsPHRPReader.ePeptideHitResultType.MODa Or
               ePHRPResultType = clsPHRPReader.ePeptideHitResultType.MODPlus Then
                ' The SpecProb values listed in the _syn_MSGF.txt file are not true spectral probabilities
                ' Instead, they're just 1 - Probability  (where Probability is a value between 0 and 1 assigned by MODa)
                ' Therefore, don't include them in the PepXML file
                CmdStr &= " /NoMSGF"
            End If

            If m_jobParams.GetJobParameter("PepXMLNoScanStats", False) Then
                CmdStr &= " /NoScanStats"
            End If

            ClearConcurrentBag(mCmdRunnerErrorsToIgnore)

            m_progress = PROGRESS_PCT_IDPicker_CREATING_PEPXML_FILE

            blnSuccess = RunProgramWork("PeptideListToXML", mPeptideListToXMLExePath, CmdStr, PEPXML_CONSOLE_OUTPUT, False, intMaxRuntimeMinutes)

            If blnSuccess Then
                ' Make sure a .pepXML file was created
                If Not File.Exists(mPepXMLFilePath) Then
                    m_message = "Error creating PepXML file"
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ", job " & m_JobNum)
                    blnSuccess = False
                Else
                    m_jobParams.AddResultFileToSkip(PEPXML_CONSOLE_OUTPUT)
                End If
            End If

        Catch ex As Exception
            m_message = "Exception in IDPickerPlugin->CreatePepXMLFile"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex)
            Return False
        End Try

        Return blnSuccess

    End Function

    ''' <summary>
    ''' Determine the prefix used by to denote decoy (reversed) proteins
    ''' </summary>
    ''' <param name="strFastaFilePath"></param>
    ''' <param name="strDecoyPrefix"></param>
    ''' <returns>True if success; false if an error</returns>
    ''' <remarks></remarks>
    Private Function DetermineDecoyProteinPrefix(strFastaFilePath As String, ByRef strDecoyPrefix As String) As Boolean

        Dim lstReversedProteinPrefixes As SortedSet(Of String)
        Dim lstPrefixStats As Dictionary(Of String, Integer)

        Dim strProtein As String
        Dim strProteinPrefix As String
        Dim intCount As Integer

        Dim objFastaFileReader As ProteinFileReader.FastaFileReader

        strDecoyPrefix = String.Empty

        Try

            If m_DebugLevel >= 3 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Looking for decoy proteins in the fasta file")
            End If

            lstReversedProteinPrefixes = New SortedSet(Of String)

            lstReversedProteinPrefixes.Add("reversed_")                             ' MTS reversed proteins                 'reversed[_]%'
            lstReversedProteinPrefixes.Add("scrambled_")                            ' MTS scrambled proteins                'scrambled[_]%'
            lstReversedProteinPrefixes.Add("xxx.")                                  ' Inspect reversed/scrambled proteins   'xxx.%'
            lstReversedProteinPrefixes.Add(MSGFDB_DECOY_PROTEIN_PREFIX.ToLower())   ' MSGFDB reversed proteins              'rev[_]%'
            lstReversedProteinPrefixes.Add(MSGFPLUS_DECOY_PROTEIN_PREFIX.ToLower()) ' MSGF+ reversed proteins               'xxx[_]%'

            ' Note that X!Tandem decoy proteins end with ":reversed"
            ' IDPicker doesn't support decoy protein name suffixes, only prefixes

            lstPrefixStats = New Dictionary(Of String, Integer)

            objFastaFileReader = New ProteinFileReader.FastaFileReader

            If Not objFastaFileReader.OpenFile(strFastaFilePath) Then
                m_message = "Error reading fasta file with ProteinFileReader"
                Return False
            End If

            Do While objFastaFileReader.ReadNextProteinEntry()

                strProtein = objFastaFileReader.ProteinName

                For Each strPrefix In lstReversedProteinPrefixes
                    If strProtein.ToLower.StartsWith(strPrefix.ToLower) Then
                        strProteinPrefix = strProtein.Substring(0, strPrefix.Length)

                        If lstPrefixStats.TryGetValue(strProteinPrefix, intCount) Then
                            lstPrefixStats(strProteinPrefix) = intCount + 1
                        Else
                            lstPrefixStats.Add(strProteinPrefix, 1)
                        End If
                    End If
                Next
            Loop

            objFastaFileReader.CloseFile()

            If lstPrefixStats.Count = 1 Then
                strDecoyPrefix = lstPrefixStats.First.Key

            ElseIf lstPrefixStats.Count > 1 Then
                ' Find the prefix (key) in lstPrefixStats with the highest occurrence count
                Dim intMaxCount As Integer = -1
                For Each kvEntry In lstPrefixStats
                    If kvEntry.Value > intMaxCount Then
                        intMaxCount = kvEntry.Value
                        strDecoyPrefix = kvEntry.Key
                    End If
                Next
            End If

        Catch ex As Exception
            m_message = "Exception in IDPickerPlugin->DetermineDecoyProteinPrefix"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex)
            Return False
        End Try

        Return True

    End Function

    Private Function IgnoreError(strErrorMessage As String) As Boolean
        Dim blnIgnore = False

        For Each strIgnoreText As String In mCmdRunnerErrorsToIgnore
            If strErrorMessage.Contains(strIgnoreText) Then
                blnIgnore = True
                Exit For
            End If
        Next

        Return blnIgnore
    End Function

    Private Function LoadIDPickerOptions() As Boolean

        Try
            mIDPickerParamFileNameLocal = m_jobParams.GetParam(clsAnalysisResourcesIDPicker.IDPICKER_PARAM_FILENAME_LOCAL)
            If String.IsNullOrEmpty(mIDPickerParamFileNameLocal) Then
                m_message = "IDPicker parameter file not defined"
                Return False
            End If

            Dim strParameterFilePath = Path.Combine(m_WorkDir, mIDPickerParamFileNameLocal)

            Using srParamFile = New StreamReader(New FileStream(strParameterFilePath, FileMode.Open, FileAccess.Read, FileShare.Read))

                Do While Not srParamFile.EndOfStream
                    Dim strLineIn = srParamFile.ReadLine()

                    If String.IsNullOrWhiteSpace(strLineIn) Then
                        Continue Do
                    End If

                    strLineIn = strLineIn.Trim()

                    If strLineIn.StartsWith("#") OrElse Not strLineIn.Contains("="c) Then
                        Continue Do
                    End If

                    Dim strKey = String.Empty
                    Dim strValue = String.Empty

                    Dim intCharIndex = strLineIn.IndexOf("="c)
                    If intCharIndex > 0 Then
                        strKey = strLineIn.Substring(0, intCharIndex).Trim()
                        If intCharIndex < strLineIn.Length - 1 Then
                            strValue = strLineIn.Substring(intCharIndex + 1).Trim()
                        Else
                            strValue = String.Empty
                        End If
                    End If

                    intCharIndex = strValue.IndexOf("#"c)
                    If intCharIndex >= 0 Then
                        strValue = strValue.Substring(0, intCharIndex)
                    End If

                    If Not String.IsNullOrWhiteSpace(strKey) Then
                        If mIDPickerOptions.ContainsKey(strKey) Then
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Ignoring duplicate parameter file option '" & strKey & "' in file " & mIDPickerParamFileNameLocal)
                        Else
                            mIDPickerOptions.Add(strKey, strValue.Trim())
                        End If
                    End If

                Loop

            End Using

        Catch ex As Exception
            m_message = "Exception in IDPickerPlugin->LoadIDPickerOptions"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex)
            Return False
        End Try

        Return True

    End Function

    Private Function LookForDecoyProteinsInMSGFDBResults(strSynFilePath As String, eResultType As clsPHRPReader.ePeptideHitResultType, ByRef strDecoyPrefix As String) As Boolean

        Dim lstPrefixesToCheck As List(Of String)

        Try
            strDecoyPrefix = String.Empty
            lstPrefixesToCheck = New List(Of String)
            lstPrefixesToCheck.Add(MSGFDB_DECOY_PROTEIN_PREFIX.ToUpper())
            lstPrefixesToCheck.Add(MSGFPLUS_DECOY_PROTEIN_PREFIX.ToUpper())

            If m_DebugLevel >= 3 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Looking for decoy proteins in the MSGFDB synopsis file")
            End If

            Using oReader = New clsPHRPReader(strSynFilePath, eResultType, False, False, False)

                Do While oReader.MoveNext
                    For Each strPrefixToCheck As String In lstPrefixesToCheck
                        If oReader.CurrentPSM.ProteinFirst.ToUpper().StartsWith(strPrefixToCheck) Then
                            strDecoyPrefix = oReader.CurrentPSM.ProteinFirst.Substring(0, strPrefixToCheck.Length)

                            If m_DebugLevel >= 4 Then
                                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Decoy protein prefix found: " & strDecoyPrefix)
                            End If

                            Exit Do
                        End If
                    Next
                Loop
            End Using

        Catch ex As Exception
            m_message = "Exception in IDPickerPlugin->LookForDecoyProteinsInMSGFDBResults"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex)
            Return False
        End Try

        Return True

    End Function

    Private Function MoveFilesIntoIDPickerSubfolder() As IJobParams.CloseOutType

        Dim diSourceFolder As DirectoryInfo
        Dim diTargetFolder As DirectoryInfo

        Dim lstFileSpecs As List(Of String)
        Dim fiFilesToMove As List(Of FileInfo)

        Dim blnErrorEncountered As Boolean
        Dim ResFolderNamePath As String

        Try
            ResFolderNamePath = Path.Combine(m_WorkDir, m_ResFolderName)

            diSourceFolder = New DirectoryInfo(ResFolderNamePath)
            diTargetFolder = diSourceFolder.CreateSubdirectory("IDPicker")

            lstFileSpecs = New List(Of String)
            fiFilesToMove = New List(Of FileInfo)

            lstFileSpecs.Add("*.idpXML")
            lstFileSpecs.Add("IDPicker*.*")
            lstFileSpecs.Add("Tool_Version_Info_IDPicker.txt")
            lstFileSpecs.Add(mIDPickerParamFileNameLocal)

            If Not mBatchFilesMoved Then
                lstFileSpecs.Add("Run*.bat")
            End If

            For Each strFileSpec As String In lstFileSpecs
                fiFilesToMove.AddRange(diSourceFolder.GetFiles(strFileSpec))
            Next

            For Each fiFile As FileInfo In fiFilesToMove
                Dim intAttempts = 0
                Dim blnSuccess = False

                Do
                    Try
                        ' Note that the file may have been moved already; confirm that it still exists
                        fiFile.Refresh()
                        If fiFile.Exists Then
                            fiFile.MoveTo(Path.Combine(diTargetFolder.FullName, fiFile.Name))
                        End If
                        blnSuccess = True
                    Catch ex As Exception
                        intAttempts += 1
                        Threading.Thread.Sleep(2000)
                    End Try
                Loop While Not blnSuccess AndAlso intAttempts <= 3

                If Not blnSuccess Then
                    blnErrorEncountered = True
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Unable to move " & fiFile.Name & " into the IDPicker subfolder; tried " & (intAttempts - 1).ToString() & " times")
                End If
            Next

        Catch ex As Exception
            blnErrorEncountered = True
        End Try

        If blnErrorEncountered Then
            ' Try to save whatever files were moved into the results folder
            Dim objAnalysisResults = New clsAnalysisResults(m_mgrParams, m_jobParams)
            objAnalysisResults.CopyFailedResultsToArchiveFolder(Path.Combine(m_WorkDir, m_ResFolderName))

            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        Else
            Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
        End If

    End Function

    Private Sub ParseConsoleOutputFileForErrors(strConsoleOutputFilePath As String)

        Dim strLineIn As String
        Dim blnUnhandledException As Boolean
        Dim strExceptionText As String = String.Empty

        Try
            If File.Exists(strConsoleOutputFilePath) Then
                Using srInFile = New StreamReader(New FileStream(strConsoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))

                    While Not srInFile.EndOfStream
                        strLineIn = srInFile.ReadLine

                        If String.IsNullOrEmpty(strLineIn) Then Continue While

                        If blnUnhandledException Then
                            If String.IsNullOrEmpty(strExceptionText) Then
                                strExceptionText = String.Copy(strLineIn)
                            Else
                                strExceptionText = ";" & strLineIn
                            End If

                        ElseIf strLineIn.StartsWith("Error:") Then
                            If Not IgnoreError(strLineIn) Then
                                mCmdRunnerErrors.Add(strLineIn)
                            End If
                        ElseIf strLineIn.StartsWith("Unhandled Exception") Then
                            mCmdRunnerErrors.Add(strLineIn)
                            blnUnhandledException = True
                        End If

                    End While
                End Using

                If Not String.IsNullOrEmpty(strExceptionText) Then
                    mCmdRunnerErrors.Add(strExceptionText)
                End If
            End If

        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception in ParseConsoleOutputFileForErrors: " & ex.Message)
        End Try

    End Sub

    ''' <summary>
    ''' Run idpAssemble to organizes the search results into a hierarchy
    ''' </summary>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Private Function RunAssemble() As Boolean

        Dim strAssembleFilePath As String
        Dim progLoc As String
        Const intMaxRuntimeMinutes = 90

        Dim blnSuccess As Boolean

        ' Create the Assemble.txt file
        ' Since we're only processing one dataset, the file will only have one line
        strAssembleFilePath = Path.Combine(m_WorkDir, ASSEMBLE_GROUPING_FILENAME)

        blnSuccess = CreateAssembleFile(strAssembleFilePath)
        If Not blnSuccess Then
            If String.IsNullOrEmpty(m_message) Then
                m_message = "Error running idpAssemble"
            End If
            Return False
        End If

        ' Define the errors that we can ignore
        ClearConcurrentBag(mCmdRunnerErrorsToIgnore)
        mCmdRunnerErrorsToIgnore.Add("protein database filename should be the same in all input files")
        mCmdRunnerErrorsToIgnore.Add("Could not find the default configuration file")

        ' Define the path to the .Exe
        progLoc = Path.Combine(mIDPickerProgramFolder, IDPicker_Assemble)

        ' Build the command string, for example:
        '  Assemble.xml -MaxFDR 0.1 -b Assemble.txt
        Dim cmdStr As String

        cmdStr = ASSEMBLE_OUTPUT_FILENAME
        cmdStr = AppendArgument(cmdStr, "AssemblyMaxFDR", "MaxFDR", "0.1")
        cmdStr &= " -b Assemble.txt -dump"

        m_progress = PROGRESS_PCT_IDPicker_RUNNING_IDPAssemble

        blnSuccess = RunProgramWork("IDPAssemble", progLoc, cmdStr, IPD_Assemble_CONSOLE_OUTPUT, True, intMaxRuntimeMinutes)

        mIdpAssembleFilePath = Path.Combine(m_WorkDir, ASSEMBLE_OUTPUT_FILENAME)

        If blnSuccess Then

            ' Make sure the output file was created		
            If Not File.Exists(mIdpAssembleFilePath) Then
                m_message = "IDPicker Assemble results file not found"
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & " at " & mIdpAssembleFilePath)
                blnSuccess = False
            Else
                ' Do not keep the assemble input or output files
                m_jobParams.AddResultFileToSkip(ASSEMBLE_GROUPING_FILENAME)
                m_jobParams.AddResultFileToSkip(ASSEMBLE_OUTPUT_FILENAME)
            End If

        End If

        Return blnSuccess

    End Function

    ''' <summary>
    ''' Run idpQonvert to convert the search scores in the pepXML file to q-values
    ''' </summary>
    ''' <param name="strFASTAFilePath"></param>
    ''' <param name="strDecoyPrefix"></param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Private Function RunQonvert(strFASTAFilePath As String, strDecoyPrefix As String, ePHRPResultType As clsPHRPReader.ePeptideHitResultType) As Boolean

        Dim progLoc As String
        Dim CmdStr As String
        Const intMaxRuntimeMinutes = 90

        Dim blnSuccess As Boolean

        ' Define the errors that we can ignore
        ClearConcurrentBag(mCmdRunnerErrorsToIgnore)
        mCmdRunnerErrorsToIgnore.Add("could not find the default configuration file")
        mCmdRunnerErrorsToIgnore.Add("could not find the default residue masses file")

        ' Define the path to the .Exe
        progLoc = Path.Combine(mIDPickerProgramFolder, IDPicker_Qonvert)

        ' Possibly override some options
        If ePHRPResultType = clsPHRPReader.ePeptideHitResultType.MODa Or
           ePHRPResultType = clsPHRPReader.ePeptideHitResultType.MODPlus Then
            ' Higher MODa probability scores are better
            mIDPickerOptions("SearchScoreWeights") = "Probability 1"
            mIDPickerOptions("NormalizedSearchScores") = "Probability"
        End If

        ' Build the command string, for example:
        '   -MaxFDR 0.1 -ProteinDatabase c:\DMS_Temp_Org\ID_002339_125D2B84.fasta -SearchScoreWeights "msgfspecprob -1" -OptimizeScoreWeights 1 -NormalizedSearchScores msgfspecprob -DecoyPrefix Reversed_ -dump QC_Shew_11_06_pt5_3_13Feb12_Doc_11-12-07.pepXML
        CmdStr = String.Empty

        CmdStr = AppendArgument(CmdStr, "QonvertMaxFDR", "MaxFDR", "0.1")
        CmdStr &= " -ProteinDatabase " & PossiblyQuotePath(strFASTAFilePath)
        CmdStr = AppendArgument(CmdStr, "SearchScoreWeights", "msgfspecprob -1")
        CmdStr = AppendArgument(CmdStr, "OptimizeScoreWeights", "1")
        CmdStr = AppendArgument(CmdStr, "NormalizedSearchScores", "msgfspecprob")

        CmdStr &= " -DecoyPrefix " & PossiblyQuotePath(strDecoyPrefix)
        CmdStr &= " -dump"              ' This tells IDPQonvert to display the processing options that the program is using
        CmdStr &= " " & mPepXMLFilePath

        m_progress = PROGRESS_PCT_IDPicker_RUNNING_IDPQonvert

        blnSuccess = RunProgramWork("IDPQonvert", progLoc, CmdStr, IPD_Qonvert_CONSOLE_OUTPUT, True, intMaxRuntimeMinutes)

        mIdpXMLFilePath = Path.Combine(m_WorkDir, m_Dataset & ".idpXML")

        If blnSuccess Then

            ' Make sure the output file was created		
            If Not File.Exists(mIdpXMLFilePath) Then
                m_message = "IDPicker Qonvert results file not found"
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & " at " & mIdpXMLFilePath)
                blnSuccess = False
            End If
        End If

        Return blnSuccess

    End Function

    ''' <summary>
    ''' Run idpReport to apply parsimony in protein assembly and generate reports
    ''' </summary>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Private Function RunReport() As Boolean
        Dim strOutputFolderName As String
        Dim progLoc As String
        Const intMaxRuntimeMinutes = 60

        Dim blnSuccess As Boolean

        strOutputFolderName = "IDPicker"

        ' Define the errors that we can ignore
        ClearConcurrentBag(mCmdRunnerErrorsToIgnore)
        mCmdRunnerErrorsToIgnore.Add("protein database filename should be the same in all input files")
        mCmdRunnerErrorsToIgnore.Add("Could not find the default configuration file")

        ' Define the path to the .Exe
        progLoc = Path.Combine(mIDPickerProgramFolder, IDPicker_Report)

        ' Build the command string, for example:
        '  report Assemble.xml -MaxFDR 0.05 -MinDistinctPeptides 2 -MinAdditionalPeptides 2 -ModsAreDistinctByDefault true -MaxAmbiguousIds 2 -MinSpectraPerProtein 2 -OutputTextReport true
        Dim CmdStr As String

        CmdStr = strOutputFolderName & " " & mIdpAssembleFilePath
        CmdStr = AppendArgument(CmdStr, "ReportMaxFDR", "MaxFDR", "0.05")
        CmdStr = AppendArgument(CmdStr, "MinDistinctPeptides", "2")
        CmdStr = AppendArgument(CmdStr, "MinAdditionalPeptides", "2")
        CmdStr = AppendArgument(CmdStr, "ModsAreDistinctByDefault", "true")
        CmdStr = AppendArgument(CmdStr, "MaxAmbiguousIds", "2")
        CmdStr = AppendArgument(CmdStr, "MinSpectraPerProtein", "2")

        CmdStr &= " -OutputTextReport true -dump"

        m_progress = PROGRESS_PCT_IDPicker_RUNNING_IDPReport

        blnSuccess = RunProgramWork("IDPReport", progLoc, CmdStr, IPD_Report_CONSOLE_OUTPUT, True, intMaxRuntimeMinutes)

        If blnSuccess Then

            Dim diReportFolder As DirectoryInfo
            diReportFolder = New DirectoryInfo(Path.Combine(m_WorkDir, strOutputFolderName))

            ' Make sure the output folder was created
            If Not diReportFolder.Exists Then
                m_message = "IDPicker report folder file not found"
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & " at " & diReportFolder.FullName)
                blnSuccess = False
            End If

            If blnSuccess Then

                Dim blnTSVFileFound = False

                ' Move the .tsv files from the Report folder up one level
                For Each fiFile As FileInfo In diReportFolder.GetFiles("*.tsv")
                    fiFile.MoveTo(Path.Combine(m_WorkDir, fiFile.Name))
                    blnTSVFileFound = True
                Next

                If Not blnTSVFileFound Then
                    m_message = "IDPicker report folder does not contain any TSV files"
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & "; " & diReportFolder.FullName)
                    blnSuccess = False
                End If

            End If

            If blnSuccess Then
                ' Copy the ConsoleOutput and RunProgram batch files into the Report folder (and add them to the files to Skip)
                ' mFilenamesToAddToReportFolder will already contain the batch file names

                mFilenamesToAddToReportFolder.Add(IPD_Qonvert_CONSOLE_OUTPUT)
                mFilenamesToAddToReportFolder.Add(IPD_Assemble_CONSOLE_OUTPUT)
                mFilenamesToAddToReportFolder.Add(IPD_Report_CONSOLE_OUTPUT)

                For Each strFileName As String In mFilenamesToAddToReportFolder
                    CopyFileIntoReportFolder(strFileName, diReportFolder.FullName)
                Next

                mBatchFilesMoved = True

                ' Zip the report folder
                Dim strZippedResultsFilePath As String
                strZippedResultsFilePath = Path.Combine(m_WorkDir, "IDPicker_HTML_Results.zip")
                m_IonicZipTools.DebugLevel = m_DebugLevel
                blnSuccess = m_IonicZipTools.ZipDirectory(diReportFolder.FullName, strZippedResultsFilePath, True)

                If Not blnSuccess AndAlso m_IonicZipTools.Message.ToLower.Contains("OutOfMemoryException".ToLower) Then
                    m_NeedToAbortProcessing = True
                End If

            End If
        Else
            ' Check whether mCmdRunnerErrors contains a known error message
            For Each strError As String In mCmdRunnerErrors
                If strError.Contains("no spectra in workspace") Then
                    ' All of the proteins were filtered out; we'll treat this as a successful completion of IDPicker
                    m_message = String.Empty
                    m_EvalMessage = "IDPicker Report filtered out all of the proteins"
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, m_EvalMessage & "; this indicates there are not enough filter-passing peptides.")
                    blnSuccess = True
                    Exit For
                End If
            Next

        End If

        Return blnSuccess

    End Function

    ''' <summary>
    ''' 
    ''' </summary>
    ''' <param name="strProgramDescription"></param>
    ''' <param name="strExePath"></param>
    ''' <param name="CmdStr"></param>
    ''' <param name="strConsoleOutputFileName">If empty, then does not create a console output file</param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Private Function RunProgramWork(strProgramDescription As String, strExePath As String, CmdStr As String, strConsoleOutputFileName As String, blnCaptureConsoleOutputViaDosRedirection As Boolean, intMaxRuntimeMinutes As Integer) As Boolean

        Dim blnSuccess As Boolean

        If m_DebugLevel >= 1 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, strExePath & " " & CmdStr.TrimStart(" "c))
        End If

        mCmdRunnerDescription = String.Copy(strProgramDescription)
        ClearConcurrentBag(mCmdRunnerErrors)

        Dim cmdRunner = New clsRunDosProgram(m_WorkDir)
        RegisterEvents(cmdRunner)
        AddHandler cmdRunner.ErrorEvent, AddressOf CmdRunner_ConsoleErrorEvent
        AddHandler cmdRunner.LoopWaiting, AddressOf CmdRunner_LoopWaiting
        AddHandler cmdRunner.Timeout, AddressOf CmdRunner_Timeout

        If blnCaptureConsoleOutputViaDosRedirection Then
            ' Create a batch file to run the command
            ' Capture the console output (including output to the error stream) via redirection symbols: 
            '    strExePath CmdStr > ConsoleOutputFile.txt 2>&1

            Dim strExePathOriginal As String = String.Copy(strExePath)
            Dim CmdStrOriginal As String = String.Copy(CmdStr)

            strProgramDescription = strProgramDescription.Replace(" ", "_")

            Dim strBatchFileName As String = "Run_" & strProgramDescription & ".bat"
            mFilenamesToAddToReportFolder.Add(strBatchFileName)

            ' Update the Exe path to point to the RunProgram batch file; update CmdStr to be empty
            strExePath = Path.Combine(m_WorkDir, strBatchFileName)
            CmdStr = String.Empty

            If String.IsNullOrEmpty(strConsoleOutputFileName) Then
                strConsoleOutputFileName = strProgramDescription & "_Console_Output.txt"
            End If

            ' Create the batch file
            Using swBatchFile = New StreamWriter(New FileStream(strExePath, FileMode.Create, FileAccess.Write, FileShare.Read))
                swBatchFile.WriteLine(strExePathOriginal & " " & CmdStrOriginal & " > " & strConsoleOutputFileName & " 2>&1")
            End Using

            Threading.Thread.Sleep(100)
        End If

        With CmdRunner
            If blnCaptureConsoleOutputViaDosRedirection OrElse String.IsNullOrEmpty(strConsoleOutputFileName) Then
                .CreateNoWindow = False
                .EchoOutputToConsole = False
                .CacheStandardOutput = False
                .WriteConsoleOutputToFile = False
            Else
                .CreateNoWindow = True
                .EchoOutputToConsole = True
                .CacheStandardOutput = False
                .WriteConsoleOutputToFile = True
                .ConsoleOutputFilePath = Path.Combine(m_WorkDir, strConsoleOutputFileName)
            End If

        End With

        Dim intMaxRuntimeSeconds As Integer = intMaxRuntimeMinutes * 60

        blnSuccess = CmdRunner.RunProgram(strExePath, CmdStr, strProgramDescription, True, intMaxRuntimeSeconds)

        If mCmdRunnerErrors.Count = 0 And Not String.IsNullOrEmpty(CmdRunner.CachedConsoleError) Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Cached console error is not empty, but mCmdRunnerErrors is empty; need to add code to parse CmdRunner.CachedConsoleError")
        End If

        If blnCaptureConsoleOutputViaDosRedirection Then
            ParseConsoleOutputFileForErrors(Path.Combine(m_WorkDir, strConsoleOutputFileName))

        ElseIf mCmdRunnerErrors.Count > 0 Then

            ' Append the error messages to the log
            ' Note that clsProgRunner will have already included them in the ConsoleOutput.txt file
            For Each strError As String In mCmdRunnerErrors
                If Not strError.ToLower().StartsWith("warning") Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "... " & strError)
                End If
            Next

        End If

        If Not blnSuccess Then

            m_message = "Error running " & strProgramDescription
            If mCmdRunnerErrors.Count > 0 Then
                m_message &= ": " & mCmdRunnerErrors.First
            End If

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)

            If CmdRunner.ExitCode <> 0 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, strProgramDescription & " returned a non-zero exit code: " & CmdRunner.ExitCode.ToString)
            Else
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Call to " & strProgramDescription & " failed (but exit code is 0)")
            End If

        Else
            m_StatusTools.UpdateAndWrite(m_progress)
            If m_DebugLevel >= 3 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, strProgramDescription & " Complete")
            End If

        End If

        Return blnSuccess

    End Function

    ''' <summary>
    ''' Stores the tool version info in the database
    ''' </summary>
    ''' <remarks></remarks>
    Private Function StoreToolVersionInfo(strIDPickerProgLoc As String, blnSkipIDPicker As Boolean) As Boolean

        Dim strToolVersionInfo As String = String.Empty
        Dim strExePath As String

        Dim ioIDPicker As FileInfo
        Dim blnSuccess As Boolean

        If m_DebugLevel >= 2 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info")
        End If

        ' We will store paths to key files in ioToolFiles
        Dim ioToolFiles As New List(Of FileInfo)

        ' Determine the path to the PeptideListToXML.exe
        mPeptideListToXMLExePath = DetermineProgramLocation("PeptideListToXML", "PeptideListToXMLProgLoc", PEPTIDE_LIST_TO_XML_EXE)

        If blnSkipIDPicker Then
            ' Only store the version of PeptideListToXML.exe in the database
            blnSuccess = MyBase.StoreToolVersionInfoOneFile(strToolVersionInfo, mPeptideListToXMLExePath)
            ioToolFiles.Add(New FileInfo(mPeptideListToXMLExePath))
        Else

            ioIDPicker = New FileInfo(strIDPickerProgLoc)
            If Not ioIDPicker.Exists Then
                Try
                    strToolVersionInfo = "Unknown"
                    Return MyBase.SetStepTaskToolVersion(strToolVersionInfo, New List(Of FileInfo))
                Catch ex As Exception
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion: " & ex.Message)
                    Return False
                End Try

            End If

            mIDPickerProgramFolder = ioIDPicker.DirectoryName

            ' Lookup the version of idpAssemble.exe (which is a .NET app; cannot use idpQonvert.exe since it is a C++ app)
            strExePath = Path.Combine(ioIDPicker.Directory.FullName, IDPicker_Assemble)
            blnSuccess = MyBase.StoreToolVersionInfoViaSystemDiagnostics(strToolVersionInfo, strExePath)
            ioToolFiles.Add(New FileInfo(strExePath))

            ' Lookup the version of idpReport.exe
            strExePath = Path.Combine(ioIDPicker.Directory.FullName, IDPicker_Report)
            blnSuccess = MyBase.StoreToolVersionInfoViaSystemDiagnostics(strToolVersionInfo, strExePath)
            ioToolFiles.Add(New FileInfo(strExePath))

            ' Also include idpQonvert.exe in ioToolFiles (version determination does not work)
            strExePath = Path.Combine(ioIDPicker.Directory.FullName, IDPicker_Qonvert)
            ioToolFiles.Add(New FileInfo(strExePath))

            ' Lookup the version of PeptideListToXML.exe			
            blnSuccess = MyBase.StoreToolVersionInfoOneFile(strToolVersionInfo, mPeptideListToXMLExePath)
            ioToolFiles.Add(New FileInfo(mPeptideListToXMLExePath))

        End If

        Try
            Return MyBase.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles)
        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion: " & ex.Message)
            Return False
        End Try

    End Function

    Private Function ZipPepXMLFile() As Boolean

        Dim strZippedPepXMLFilePath As String

        Try
            strZippedPepXMLFilePath = Path.Combine(m_WorkDir, m_Dataset & "_pepXML.zip")

            If Not MyBase.ZipFile(mPepXMLFilePath, False, strZippedPepXMLFilePath) Then
                Dim Msg As String = "Error zipping PepXML file, job " & m_JobNum
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
                m_message = clsGlobal.AppendToComment(m_message, "Error zipping PepXML file")
                Return False
            End If

            ' Add the .pepXML file to .FilesToDelete since we only want to keep the Zipped version
            m_jobParams.AddResultFileToSkip(Path.GetFileName(mPepXMLFilePath))

        Catch ex As Exception
            Dim Msg As String = "clsAnalysisToolRunnerIDPicker.ZipPepXMLFile, Exception zipping output files, job " & m_JobNum & ": " & ex.Message
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
            m_message = clsGlobal.AppendToComment(m_message, "Error zipping PepXML file")
            Return False
        End Try

        Return True

    End Function

#End Region

#Region "Event Handlers"

    Private Sub CmdRunner_ConsoleErrorEvent(NewText As String, ex As Exception)

        If Not mCmdRunnerErrors Is Nothing Then
            ' Split NewText on newline characters
            Dim strSplitLine() As String
            Dim chNewLineChars = New Char() {ControlChars.Cr, ControlChars.Lf}

            strSplitLine = NewText.Split(chNewLineChars, StringSplitOptions.RemoveEmptyEntries)

            If Not strSplitLine Is Nothing Then
                For Each strItem As String In strSplitLine
                    strItem = strItem.Trim(chNewLineChars)
                    If Not String.IsNullOrEmpty(strItem) Then

                        ' Confirm that strItem does not contain any text in mCmdRunnerErrorsToIgnore
                        If Not IgnoreError(strItem) Then
                            mCmdRunnerErrors.Add(strItem)
                        End If

                    End If
                Next
            End If

        End If

    End Sub

    ''' <summary>
    ''' Event handler for CmdRunner.LoopWaiting event
    ''' </summary>
    ''' <remarks></remarks>
    Private Sub CmdRunner_LoopWaiting()

        UpdateStatusFile()

        LogProgress("IDPicker")

    End Sub

    Private Sub CmdRunner_Timeout()
        If m_DebugLevel >= 2 Then
            LogError("Aborted " & mCmdRunnerDescription)
        End If
    End Sub

#End Region

End Class
