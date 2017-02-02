Option Strict On

Imports AnalysisManagerBase
Imports System.IO
Imports System.Text.RegularExpressions

Public Class clsAnalysisToolRunnerMSAlignQuant
    Inherits clsAnalysisToolRunnerBase

    '*********************************************************************************************************
    'Class for running TargetedQuant
    '*********************************************************************************************************

#Region "Module Variables"

    Protected Const TARGETED_QUANT_XML_FILE_NAME As String = "TargetedWorkflowParams.xml"
    Protected Const TARGETED_WORKFLOWS_CONSOLE_OUTPUT As String = "TargetedWorkflow_ConsoleOutput.txt"
    Protected Const PROGRESS_PCT_CREATING_PARAMETERS As Integer = 5

    Protected Const PROGRESS_TARGETED_WORKFLOWS_STARTING As Integer = 10
    Protected Const PROGRESS_TARGETED_WORKFLOWS_CREATING_XIC As Integer = 11
    Protected Const PROGRESS_TARGETED_WORKFLOWS_XIC_CREATED As Integer = 15
    Protected Const PROGRESS_TARGETED_WORKFLOWS_PEAKS_LOADED As Integer = 15
    Protected Const PROGRESS_TARGETED_WORKFLOWS_PROCESSING_COMPLETE As Integer = 95

    Protected Const PROGRESS_TARGETED_WORKFLOWS_COMPLETE As Integer = 98
    Protected Const PROGRESS_PCT_COMPLETE As Integer = 99

    Protected mConsoleOutputErrorMsg As String

    Protected mTargetedWorkflowsProgLoc As String
    Protected mConsoleOutputProgressMap As Dictionary(Of String, Integer)

    Protected mCmdRunner As clsRunDosProgram
#End Region

#Region "Structures"
#End Region

#Region "Methods"
    ''' <summary>
    ''' Runs TargetedWorkFlowConsole tool
    ''' </summary>
    ''' <returns>CloseOutType enum indicating success or failure</returns>
    ''' <remarks></remarks>
    Public Overrides Function RunTool() As IJobParams.CloseOutType
        Dim result As IJobParams.CloseOutType
        Dim blnProcessingError = False

        Dim blnSuccess As Boolean

        Dim strTargetedQuantParamFilePath As String

        Try
            'Call base class for initial setup
            If Not MyBase.RunTool = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            If m_DebugLevel > 4 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerMSAlignQuant.RunTool(): Enter")
            End If

            ' Determine the path to the TargetedWorkflowConsole.exe program
            ' Note that it is part of project DeconTools.Workflows
            mTargetedWorkflowsProgLoc = DetermineProgramLocation("MSAlign_Quant", "TargetedWorkflowsProgLoc", "TargetedWorkflowConsole.exe")

            If String.IsNullOrWhiteSpace(mTargetedWorkflowsProgLoc) Then
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            ' Store the TargetedWorkflowsConsole version info in the database
            If Not StoreToolVersionInfo(mTargetedWorkflowsProgLoc) Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Aborting since StoreToolVersionInfo returned false")
                m_message = "Error determining TargetedWorkflowsConsole version"
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            ' Create the TargetedWorkflowParams.xml file
            m_progress = PROGRESS_PCT_CREATING_PARAMETERS

            strTargetedQuantParamFilePath = CreateTargetedQuantParamFile()
            If String.IsNullOrEmpty(strTargetedQuantParamFilePath) Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Aborting since CreateTargetedQuantParamFile returned false")
                If String.IsNullOrEmpty(m_message) Then
                    m_message = "Error creating " & TARGETED_QUANT_XML_FILE_NAME
                End If
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            mConsoleOutputErrorMsg = String.Empty

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Running TargetedWorkflowsConsole")


            ' Set up and execute a program runner to run TargetedWorkflowsConsole
            Dim strRawDataType As String = m_jobParams.GetParam("RawDataType")
            Dim cmdStr As String

            Select Case strRawDataType.ToLower
                Case clsAnalysisResources.RAW_DATA_TYPE_DOT_RAW_FILES
                    cmdStr = " " & PossiblyQuotePath(Path.Combine(m_WorkDir, m_Dataset & clsAnalysisResources.DOT_RAW_EXTENSION))
                Case clsAnalysisResources.RAW_DATA_TYPE_BRUKER_FT_FOLDER, clsAnalysisResources.RAW_DATA_TYPE_DOT_D_FOLDERS
                    ' Bruker_FT folders are actually .D folders
                    cmdStr = " " & PossiblyQuotePath(Path.Combine(m_WorkDir, m_Dataset) & clsAnalysisResources.DOT_D_EXTENSION)
                Case Else
                    m_message = "Dataset type " & strRawDataType & " is not supported"
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, m_message)
                    Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End Select


            cmdStr &= " " & PossiblyQuotePath(strTargetedQuantParamFilePath)

            If m_DebugLevel >= 1 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, mTargetedWorkflowsProgLoc & cmdStr)
            End If

            mCmdRunner = New clsRunDosProgram(m_WorkDir)
            RegisterEvents(mCmdRunner)
            AddHandler mCmdRunner.LoopWaiting, AddressOf CmdRunner_LoopWaiting

            With mCmdRunner
                .CreateNoWindow = True
                .CacheStandardOutput = True
                .EchoOutputToConsole = True
                .WriteConsoleOutputToFile = True
                .ConsoleOutputFilePath = Path.Combine(m_WorkDir, TARGETED_WORKFLOWS_CONSOLE_OUTPUT)
            End With

            m_progress = PROGRESS_TARGETED_WORKFLOWS_STARTING

            blnSuccess = mCmdRunner.RunProgram(mTargetedWorkflowsProgLoc, cmdStr, "TargetedWorkflowsConsole", True)

            If Not mCmdRunner.WriteConsoleOutputToFile Then
                ' Write the console output to a text file
                Threading.Thread.Sleep(250)

                Using swConsoleOutputfile As New StreamWriter(New FileStream(mCmdRunner.ConsoleOutputFilePath, FileMode.Create, FileAccess.Write, FileShare.Read))
                    swConsoleOutputfile.WriteLine(mCmdRunner.CachedConsoleOutput)
                End Using

            End If

            ' Parse the console output file one more time to check for errors
            Threading.Thread.Sleep(250)
            ParseConsoleOutputFile(mCmdRunner.ConsoleOutputFilePath)

            If Not String.IsNullOrEmpty(mConsoleOutputErrorMsg) Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mConsoleOutputErrorMsg)
            End If

            If blnSuccess Then
                ' Make sure that the quantitation output file was created
                Dim strOutputFileName As String = m_Dataset & "_quant.txt"

                If Not File.Exists(Path.Combine(m_WorkDir, strOutputFileName)) Then
                    m_message = "MSAlign_Quant result file not found (" & strOutputFileName & ")"
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
                    blnSuccess = False
                End If

            End If

            If Not blnSuccess Then
                Dim msg = "Error running TargetedWorkflowsConsole"

                If Not String.IsNullOrEmpty(mConsoleOutputErrorMsg) Then
                    m_message = clsGlobal.AppendToComment(m_message, msg & "; " & mConsoleOutputErrorMsg)
                Else
                    m_message = clsGlobal.AppendToComment(m_message, msg)
                End If

                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg & ", job " & m_JobNum)

                If mCmdRunner.ExitCode <> 0 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "TargetedWorkflowsConsole returned a non-zero exit code: " & mCmdRunner.ExitCode.ToString())
                Else
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Call to TargetedWorkflowsConsole failed (but exit code is 0)")
                End If

                blnProcessingError = True

            Else
                m_progress = PROGRESS_PCT_COMPLETE
                m_StatusTools.UpdateAndWrite(m_progress)
                If m_DebugLevel >= 3 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "TargetedWorkflowsConsole Quantitation Complete")
                End If

                Dim fiConsoleOutputfile As FileInfo
                Dim fiDeconWorkflowsLogFile As FileInfo
                fiConsoleOutputfile = New FileInfo(Path.Combine(m_WorkDir, TARGETED_WORKFLOWS_CONSOLE_OUTPUT))
                fiDeconWorkflowsLogFile = New FileInfo(Path.Combine(m_WorkDir, m_Dataset & "_log.txt"))

                If fiConsoleOutputfile.Exists AndAlso fiDeconWorkflowsLogFile.Exists AndAlso fiConsoleOutputfile.Length > fiDeconWorkflowsLogFile.Length Then
                    ' Don't keep the _log.txt file since the Console_Output file has all of the same information
                    m_jobParams.AddResultFileToSkip(fiDeconWorkflowsLogFile.Name)
                End If

                ' Don't keep the _peaks.txt file since it can get quite large
                m_jobParams.AddResultFileToSkip(m_Dataset & "_peaks.txt")
            End If

            m_progress = PROGRESS_PCT_COMPLETE

            'Stop the job timer
            m_StopTime = DateTime.UtcNow

            'Add the current job data to the summary file
            If Not UpdateSummaryFile() Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Error creating summary file, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step"))
            End If

            'Make sure objects are released
            Threading.Thread.Sleep(500)         ' 500 msec delay
            PRISM.Processes.clsProgRunner.GarbageCollectNow()

            If blnProcessingError Or result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                ' Something went wrong
                ' In order to help diagnose things, we will move whatever files were created into the result folder, 
                '  archive it using CopyFailedResultsToArchiveFolder, then return IJobParams.CloseOutType.CLOSEOUT_FAILED
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

            result = CopyResultsFolderToServer()
            If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                ' Note that CopyResultsFolderToServer should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
                Return result
            End If

        Catch ex As Exception
            m_message = "Exception in MSAlignQuantPlugin->RunTool"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    Protected Sub CopyFailedResultsToArchiveFolder()

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
    ''' Creates the targeted quant params XML file
    ''' </summary>
    ''' <returns>The full path to the file, if successful.  Otherwise, and empty string</returns>
    ''' <remarks></remarks>
    Protected Function CreateTargetedQuantParamFile() As String

        Dim strTargetedQuantParamFilePath As String

        Try
            strTargetedQuantParamFilePath = Path.Combine(m_WorkDir, TARGETED_QUANT_XML_FILE_NAME)
            Dim strMSAlignResultTableName = m_Dataset & clsAnalysisResourcesMSAlignQuant.MSALIGN_RESULT_TABLE_SUFFIX

            '' Optionally make a trimmed version of the ResultTable file for testing purposes

            'Dim strFullResultsPath = Path.Combine(m_WorkDir, strMSAlignResultTableName)
            'Dim strTrimmedFilePath = Path.Combine(m_WorkDir, m_Dataset & "_TrimmedResults.tmp")

            'Using srFullResults As New StreamReader(New FileStream(strFullResultsPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)),
            '    swTrimmedResults As New StreamWriter(New FileStream(strTrimmedFilePath, FileMode.Create, FileAccess.Write, FileShare.Read))

            '    Dim linesRead = 0
            '    While Not srFullResults.EndOfStream AndAlso linesRead < 30
            '        swTrimmedResults.WriteLine(srFullResults.ReadLine())
            '        linesRead += 1
            '    End While
            'End Using

            '' Replace the original file with the trimmed one
            'Threading.Thread.Sleep(100)
            'File.Delete(strFullResultsPath)
            'Threading.Thread.Sleep(100)

            'File.Move(strTrimmedFilePath, strFullResultsPath)

            Dim strWorkflowParamFileName = m_jobParams.GetParam("MSAlignQuantParamFile")
            If String.IsNullOrEmpty(strWorkflowParamFileName) Then
                m_message = NotifyMissingParameter(m_jobParams, "MSAlignQuantParamFile")
                Return String.Empty
            End If

            Using swTargetedQuantXMLFile = New Xml.XmlTextWriter(strTargetedQuantParamFilePath, Text.Encoding.UTF8)
                swTargetedQuantXMLFile.Formatting = Xml.Formatting.Indented
                swTargetedQuantXMLFile.Indentation = 4

                swTargetedQuantXMLFile.WriteStartDocument()
                swTargetedQuantXMLFile.WriteStartElement("WorkflowParameters")

                WriteXMLSetting(swTargetedQuantXMLFile, "CopyRawFileLocal", "false")
                WriteXMLSetting(swTargetedQuantXMLFile, "DeleteLocalDatasetAfterProcessing", "false")
                WriteXMLSetting(swTargetedQuantXMLFile, "FileContainingDatasetPaths", "")
                WriteXMLSetting(swTargetedQuantXMLFile, "FolderPathForCopiedRawDataset", "")
                WriteXMLSetting(swTargetedQuantXMLFile, "LoggingFolder", m_WorkDir)
                WriteXMLSetting(swTargetedQuantXMLFile, "TargetsFilePath", Path.Combine(m_WorkDir, strMSAlignResultTableName))
                WriteXMLSetting(swTargetedQuantXMLFile, "TargetType", "LcmsFeature")
                WriteXMLSetting(swTargetedQuantXMLFile, "ResultsFolder", m_WorkDir)
                WriteXMLSetting(swTargetedQuantXMLFile, "WorkflowParameterFile", Path.Combine(m_WorkDir, strWorkflowParamFileName))
                WriteXMLSetting(swTargetedQuantXMLFile, "WorkflowType", "TopDownTargetedWorkflowExecutor1")

                swTargetedQuantXMLFile.WriteEndElement()    ' WorkflowParameters

                swTargetedQuantXMLFile.WriteEndDocument()

            End Using

        Catch ex As Exception
            m_message = "Exception creating " & TARGETED_QUANT_XML_FILE_NAME
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message)
            Return String.Empty
        End Try

        Return strTargetedQuantParamFilePath

    End Function

    ''' <summary>
    ''' Parse the TargetedWorkflowsConsole console output file to track progress
    ''' </summary>
    ''' <param name="strConsoleOutputFilePath"></param>
    ''' <remarks></remarks>
    Protected Sub ParseConsoleOutputFile(strConsoleOutputFilePath As String)

        ' Example Console output:
        '   8/13/2012 2:29:48 PM    Started Processing....
        '   8/13/2012 2:29:48 PM    Dataset = E:\DMS_WorkDir2\Proteus_Peri_intact_ETD.raw
        '   8/13/2012 2:29:48 PM    Run initialized successfully.
        '   8/13/2012 2:29:48 PM    Creating extracted ion chromatogram (XIC) source data... takes 1-5 minutes.. only needs to be done once.
        '   8/13/2012 2:30:18 PM    Done creating XIC source data.
        '   8/13/2012 2:30:18 PM    Peak loading started...
        '   Peak importer progress (%) = 4
        '   Peak importer progress (%) = 27
        '   Peak importer progress (%) = 50
        '   Peak importer progress (%) = 73
        '   Peak importer progress (%) = 92
        '   8/13/2012 2:30:21 PM    Peak Loading complete.
        '   Proteus_Peri_intact_ETD NOT aligned.
        '   8/13/2012 2:30:21 PM    FYI - Run has NOT been mass aligned.
        '   8/13/2012 2:30:21 PM    Warning - Run has NOT been NET aligned.
        '   8/13/2012 2:30:21 PM    Processing...
        '   8/13/2012 2:31:27 PM    Percent complete = 3%   Target 100 of 3917
        '   8/13/2012 2:32:24 PM    Percent complete = 5%   Target 200 of 3917
        '   8/13/2012 2:33:20 PM    Percent complete = 8%   Target 300 of 3917
        '   ...
        '   8/13/2012 1:56:55 PM    ---- PROCESSING COMPLETE ---------------

        Static reSubProgress As Regex = New Regex("Percent complete = ([0-9.]+)", RegexOptions.Compiled)

        Try

            If mConsoleOutputProgressMap Is Nothing OrElse mConsoleOutputProgressMap.Count = 0 Then
                mConsoleOutputProgressMap = New Dictionary(Of String, Integer)

                mConsoleOutputProgressMap.Add("Creating extracted ion chromatogram", PROGRESS_TARGETED_WORKFLOWS_CREATING_XIC)
                mConsoleOutputProgressMap.Add("Done creating XIC source data", PROGRESS_TARGETED_WORKFLOWS_XIC_CREATED)
                mConsoleOutputProgressMap.Add("Peak Loading complete", PROGRESS_TARGETED_WORKFLOWS_PEAKS_LOADED)
                mConsoleOutputProgressMap.Add("---- PROCESSING COMPLETE ----", PROGRESS_TARGETED_WORKFLOWS_PROCESSING_COMPLETE)
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
            Dim strLineInLCase As String

            Dim intCharIndex As Integer

            Dim dblSubProgressAddon As Double

            Dim intEffectiveProgress As Integer
            intEffectiveProgress = PROGRESS_TARGETED_WORKFLOWS_STARTING

            mConsoleOutputErrorMsg = String.Empty

            Using srInFile = New StreamReader(New FileStream(strConsoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))

                Do While Not srInFile.EndOfStream
                    strLineIn = srInFile.ReadLine()

                    If Not String.IsNullOrWhiteSpace(strLineIn) Then

                        strLineInLCase = strLineIn.ToLower()

                        ' Update progress if the line contains any one of the expected phrases
                        For Each oItem As KeyValuePair(Of String, Integer) In mConsoleOutputProgressMap
                            If strLineIn.Contains(oItem.Key) Then
                                If intEffectiveProgress < oItem.Value Then
                                    intEffectiveProgress = oItem.Value
                                End If
                            End If
                        Next

                        If intEffectiveProgress = PROGRESS_TARGETED_WORKFLOWS_PEAKS_LOADED Then
                            Dim oMatch = reSubProgress.Match(strLineIn)
                            If oMatch.Success Then
                                If Double.TryParse(oMatch.Groups(1).Value, dblSubProgressAddon) Then
                                    dblSubProgressAddon /= 100
                                End If
                            End If
                        End If

                        intCharIndex = strLineInLCase.IndexOf("exception of type", StringComparison.Ordinal)
                        If intCharIndex < 0 Then
                            intCharIndex = strLineInLCase.IndexOf(ControlChars.Tab & "error", StringComparison.Ordinal)

                            If intCharIndex > 0 Then
                                intCharIndex += 1
                            ElseIf strLineInLCase.StartsWith("error") Then
                                intCharIndex = 0
                            End If
                        End If

                        If intCharIndex >= 0 Then
                            ' Error message found; update m_message
                            Dim strNewError As String = strLineIn.Substring(intCharIndex)

                            If strNewError.Contains("all peptides contain unknown modifications") Then
                                strNewError = "Error: every peptide in the mass tags file had an unknown modification; known mods are Acetylation (C2H2O, 42.01 Da), Phosphorylation (HPO3, 79.97 Da), or Pyroglutomate (H3N1, 17.03 Da)"
                            End If

                            mConsoleOutputErrorMsg = clsGlobal.AppendToComment(mConsoleOutputErrorMsg, strNewError)
                        End If

                    End If
                Loop

            End Using

            Dim sngEffectiveProgress As Single = intEffectiveProgress

            ' Bump up the effective progress if finding features in positive or negative data
            If intEffectiveProgress = PROGRESS_TARGETED_WORKFLOWS_PEAKS_LOADED Then
                sngEffectiveProgress += CSng((PROGRESS_TARGETED_WORKFLOWS_PROCESSING_COMPLETE - PROGRESS_TARGETED_WORKFLOWS_PEAKS_LOADED) * dblSubProgressAddon)
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
    ''' Stores the tool version info in the database
    ''' </summary>
    ''' <remarks></remarks>
    Protected Function StoreToolVersionInfo(strTargetedWorkflowsConsoleProgLoc As String) As Boolean

        Dim strToolVersionInfo As String = String.Empty
        Dim ioTargetedWorkflowsConsole As FileInfo
        Dim blnSuccess As Boolean

        If m_DebugLevel >= 2 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info")
        End If

        ioTargetedWorkflowsConsole = New FileInfo(strTargetedWorkflowsConsoleProgLoc)
        If Not ioTargetedWorkflowsConsole.Exists Then
            Try
                strToolVersionInfo = "Unknown"
                Return MyBase.SetStepTaskToolVersion(strToolVersionInfo, New List(Of FileInfo))
            Catch ex As Exception
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion: " & ex.Message)
                Return False
            End Try

        End If

        ' Lookup the version of the TargetedWorkflowsConsole application
        blnSuccess = MyBase.StoreToolVersionInfoOneFile(strToolVersionInfo, ioTargetedWorkflowsConsole.FullName)
        If Not blnSuccess Then Return False

        ' Store paths to key DLLs in ioToolFiles
        Dim ioToolFiles As New List(Of FileInfo)
        ioToolFiles.Add(ioTargetedWorkflowsConsole)

        ioToolFiles.Add(New FileInfo(Path.Combine(ioTargetedWorkflowsConsole.DirectoryName, "DeconTools.Backend.dll")))
        ioToolFiles.Add(New FileInfo(Path.Combine(ioTargetedWorkflowsConsole.DirectoryName, "DeconTools.Workflows.dll")))

        Try
            Return MyBase.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles)
        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion: " & ex.Message)
            Return False
        End Try

    End Function

    Public Sub WriteXMLSetting(swOutFile As Xml.XmlTextWriter, strSettingName As String, strSettingValue As String)
        swOutFile.WriteStartElement(strSettingName)
        swOutFile.WriteValue(strSettingValue)
        swOutFile.WriteEndElement()
    End Sub

#End Region

#Region "Event Handlers"

    ''' <summary>
    ''' Event handler for CmdRunner.LoopWaiting event
    ''' </summary>
    ''' <remarks></remarks>
    Protected Sub CmdRunner_LoopWaiting()

        Static dtLastConsoleOutputParse As DateTime = DateTime.UtcNow

        UpdateStatusFile()

        If DateTime.UtcNow.Subtract(dtLastConsoleOutputParse).TotalSeconds >= 15 Then
            dtLastConsoleOutputParse = DateTime.UtcNow

            ParseConsoleOutputFile(Path.Combine(m_WorkDir, TARGETED_WORKFLOWS_CONSOLE_OUTPUT))

            LogProgress("MSAlign Quant")
        End If

    End Sub

#End Region

End Class
