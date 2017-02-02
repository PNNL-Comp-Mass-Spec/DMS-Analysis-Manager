Option Strict On

'*********************************************************************************************************
' Written by Matt Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2009, Battelle Memorial Institute
'
'*********************************************************************************************************

Imports AnalysisManagerBase
Imports Mage
Imports System.Linq
Imports System.IO
Imports System.Runtime.InteropServices
Imports System.Text.RegularExpressions
Imports PHRPReader

Public Class clsAnalysisToolRunnerPhosphoFdrAggregator
    Inherits clsAnalysisToolRunnerBase

    '*********************************************************************************************************
    'Class for running Phospho_FDRAggregator analysis
    '*********************************************************************************************************

#Region "Constants and Enums"
    Protected Const ASCORE_CONSOLE_OUTPUT_PREFIX As String = "AScore_ConsoleOutput"

    Protected Const FILE_SUFFIX_ASCORE_RESULTS As String = "_ascore.txt"
    Protected Const FILE_SUFFIX_SYN_PLUS_ASCORE As String = "_plus_ascore.txt"

    Protected Const PROGRESS_PCT_PHOSPHO_FDR_RUNNING As Single = 5
    Protected Const PROGRESS_PCT_PHOSPHO_FDR_COMPLETE As Single = 99

    Protected Enum DatasetTypeConstants
        Unknown = 0
        CID = 1
        ETD = 2
        HCD = 3
    End Enum
#End Region

#Region "Structures"

    Protected Structure udtJobMetadataForAScore
        Public Job As Integer
        Public Dataset As String
        Public ToolName As String
        Public FirstHitsFilePath As String
        Public SynopsisFilePath As String
        Public ToolNameForAScore As String
        Public SpectrumFilePath As String
    End Structure

#End Region

#Region "Module Variables"

    Protected mConsoleOutputErrorMsg As String

    Protected mJobFoldersProcessed As Integer
    Protected mTotalJobFolders As Integer

    Protected mCmdRunner As clsRunDosProgram
#End Region

#Region "Methods"
    ''' <summary>
    ''' Runs PhosphoFdrAggregator tool
    ''' </summary>
    ''' <returns>CloseOutType enum indicating success or failure</returns>
    ''' <remarks></remarks>
    Public Overrides Function RunTool() As IJobParams.CloseOutType

        Dim result As IJobParams.CloseOutType

        Try
            ' Call base class for initial setup
            If Not MyBase.RunTool = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            If m_DebugLevel > 4 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerPhosphoFdrAggregator.RunTool(): Enter")
            End If

            ' Determine the path to the Ascore program
            ' AScoreProgLoc will be something like this: "C:\DMS_Programs\AScore\AScore_Console.exe"          
            Dim progLocAScore As String = m_mgrParams.GetParam("AScoreprogloc")
            If Not File.Exists(progLocAScore) Then
                If String.IsNullOrWhiteSpace(progLocAScore) Then progLocAScore = "Parameter 'AScoreprogloc' not defined for this manager"
                m_message = "Cannot find AScore program file"
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & progLocAScore)
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            ' Store the AScore version info in the database
            If Not StoreToolVersionInfo(progLocAScore) Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Aborting since StoreToolVersionInfo returned false")
                m_message = "Error determining AScore version"
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            ' Run AScore for each of the jobs in the data package
            Dim fileSuffixesToCombine As List(Of String) = Nothing
            Dim processingRuntimes As Dictionary(Of String, Double) = Nothing

            Dim success = ProcessSynopsisFiles(progLocAScore, fileSuffixesToCombine, processingRuntimes)

            If Not fileSuffixesToCombine Is Nothing Then
                ' Concatenate the results
                For Each fileSuffix In fileSuffixesToCombine

                    Dim concatenateSuccess = ConcatenateResultFiles(fileSuffix & FILE_SUFFIX_ASCORE_RESULTS)
                    If Not concatenateSuccess Then
                        success = False
                    End If

                    concatenateSuccess = ConcatenateResultFiles(fileSuffix & FILE_SUFFIX_SYN_PLUS_ASCORE)
                    If Not concatenateSuccess Then
                        success = False
                    End If

                Next
            End If

            ' Concatenate the log files
            ConcatenateLogFiles(processingRuntimes)

            m_progress = PROGRESS_PCT_PHOSPHO_FDR_COMPLETE

            ' Stop the job timer
            m_StopTime = DateTime.UtcNow

            ' Add the current job data to the summary file
            If Not UpdateSummaryFile() Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN, "Error creating summary file, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step"))
            End If

            mCmdRunner = Nothing

            ' Make sure objects are released
            System.Threading.Thread.Sleep(1000)
            PRISM.Processes.clsProgRunner.GarbageCollectNow()

            If Not success Then
                ' Move the source files and any results to the Failed Job folder
                ' Useful for debugging problems
                CopyFailedResultsToArchiveFolder()
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            ' Override the dataset name and transfer folder path so that the results get copied to the correct location
            MyBase.RedefineAggregationJobDatasetAndTransferFolder()

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
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

        Catch ex As Exception
            m_message = "Error in PhosphoFdrAggregator->RunTool"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    Protected Function AddMSGFSpecProbValues(jobNumber As Integer, synFilePath As String, fileTypeTag As String) As Boolean

        Try
            Dim fiSynFile = New FileInfo(synFilePath)
            Dim fiMsgfFile = New FileInfo(Path.Combine(fiSynFile.Directory.FullName, Path.GetFileNameWithoutExtension(fiSynFile.Name) & "_MSGF.txt"))

            If Not fiMsgfFile.Exists Then
                Dim warningMessage = "MSGF file not found for job " & jobNumber
                m_EvalMessage = clsGlobal.AppendToComment(m_EvalMessage, warningMessage)

                warningMessage &= "; cannot add MSGF_SpecProb values to the " & fileTypeTag & " file; " & fiMsgfFile.FullName
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, warningMessage)
                Return True
            End If

            ' Use Mage to create an updated synopsis file, with column MSGF_SpecProb added

            ' First cache the MSGFSpecProb values using a delimited file reader

            Dim msgfReader = New DelimitedFileReader()
            msgfReader.FilePath = fiMsgfFile.FullName

            Dim lookupSink = New KVSink()
            lookupSink.KeyColumnName = "Result_ID"
            lookupSink.ValueColumnName = "SpecProb"

            Dim cachePipeline = ProcessingPipeline.Assemble("Lookup pipeline", msgfReader, lookupSink)
            cachePipeline.RunRoot(Nothing)

            If lookupSink.Values.Count = 0 Then
                m_message = fiMsgfFile.Name & " was empty for job " & jobNumber
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
                Return False
            End If

            ' Next create the updated synopsis / first hits file using the values cached in lookupSink 

            Dim fiUpdatedfile = New FileInfo(fiMsgfFile.FullName & ".msgf")

            Dim synReader = New DelimitedFileReader() With {
                .FilePath = fiSynFile.FullName
            }

            Dim synWriter = New DelimitedFileWriter() With {
                .FilePath = fiUpdatedfile.FullName
            }

            Dim mergeFilter = New MergeFromLookup() With {
                .OutputColumnList = "*, MSGF_SpecProb|+|text",
                .LookupKV = lookupSink.Values,
                .KeyColName = "HitNum",
                .MergeColName = "MSGF_SpecProb"
            }

            Dim mergePipeline = ProcessingPipeline.Assemble("Main pipeline", synReader, mergeFilter, synWriter)
            mergePipeline.RunRoot(Nothing)

            fiUpdatedfile.Refresh()
            If Not fiUpdatedfile.Exists Then
                m_message = "Mage did not create " & fiUpdatedfile.Name & " for job " & jobNumber
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
                Return False
            End If

            If fiUpdatedfile.Length = 0 Then
                m_message = fiUpdatedfile.Name & " is 0 bytes for job " & jobNumber
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
                Return False
            End If

            Threading.Thread.Sleep(100)

            ' Replace the original file with the new one
            Dim originalFilePath = fiSynFile.FullName

            fiSynFile.MoveTo(fiSynFile.FullName & ".old")

            fiUpdatedfile.MoveTo(originalFilePath)

            Return True

        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error in AddMSGFSpecProbValues: " & ex.Message)
            Return False
        End Try

    End Function

    Protected Sub CacheFileSuffix(fileSuffixesToCombine As List(Of String), datasetName As String, fileName As String)

        Dim baseName = Path.GetFileNameWithoutExtension(fileName)
        baseName = baseName.Substring(datasetName.Length)

        If Not fileSuffixesToCombine.Contains(baseName) Then
            fileSuffixesToCombine.Add(baseName)
        End If
    End Sub

    Protected Function ConcatenateLogFiles(processingRuntimes As Dictionary(Of String, Double)) As Boolean

        Try

            Dim targetFile = Path.Combine(m_WorkDir, ASCORE_CONSOLE_OUTPUT_PREFIX & ".txt")
            Using swConcatenatedFile = New StreamWriter(New FileStream(targetFile, FileMode.Create, FileAccess.Write, FileShare.Read))

                Dim jobFolderlist = GetJobFolderList()
                For Each jobFolder In jobFolderlist

                    Dim jobNumber = jobFolder.Key

                    Dim logFiles = jobFolder.Value.GetFiles(ASCORE_CONSOLE_OUTPUT_PREFIX & "*").ToList()
                    If logFiles.Count = 0 Then
                        Continue For
                    End If

                    swConcatenatedFile.WriteLine("----------------------------------------------------------")
                    swConcatenatedFile.WriteLine("Job: " & jobNumber)

                    For Each logFile In logFiles

                        ' Logfile name should be of the form AScore_ConsoleOutput_syn.txt
                        ' Parse out the tag from it -- in this case "syn"
                        Dim fileTypeTag = Path.GetFileNameWithoutExtension(logFile.Name).Substring(ASCORE_CONSOLE_OUTPUT_PREFIX.Length + 1)

                        Dim runtimeMinutes As Double
                        processingRuntimes.TryGetValue(jobNumber & fileTypeTag, runtimeMinutes)

                        Using srInputFile = New StreamReader(New FileStream(logFile.FullName, FileMode.Open, FileAccess.Read, FileShare.Read))

                            Do While Not srInputFile.EndOfStream
                                Dim dataLine = srInputFile.ReadLine()
                                If Not String.IsNullOrWhiteSpace(dataLine) Then

                                    If dataLine.StartsWith("Percent Completion") Then Continue Do
                                    If dataLine.Trim.StartsWith("Skipping PHRP result") Then Continue Do

                                    swConcatenatedFile.WriteLine(dataLine)
                                End If
                            Loop

                        End Using

                        swConcatenatedFile.WriteLine("Processing time: " & runtimeMinutes.ToString("0.0") & " minutes")
                        swConcatenatedFile.WriteLine()

                    Next

                Next

            End Using

        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "ConcatenateLogFiles: " & ex.Message)
            Return False
        End Try

        Return True

    End Function

    Protected Function ConcatenateResultFiles(fileSuffix As String) As Boolean

        Dim currentFile As String = String.Empty
        Dim firstfileProcessed = False

        Try

            Dim targetFile = Path.Combine(m_WorkDir, "Concatenated" & fileSuffix)
            Using swConcatenatedFile = New StreamWriter(New FileStream(targetFile, FileMode.Create, FileAccess.Write, FileShare.Read))

                Dim jobFolderlist = GetJobFolderList()
                For Each jobFolder In jobFolderlist

                    Dim jobNumber = jobFolder.Key

                    Dim filesToCombine = jobFolder.Value.GetFiles("*" & fileSuffix).ToList()

                    For Each fiResultFile In filesToCombine
                        currentFile = Path.GetFileName(fiResultFile.FullName)

                        Using srInputFile = New StreamReader(New FileStream(fiResultFile.FullName, FileMode.Open, FileAccess.Read, FileShare.Read))

                            If srInputFile.EndOfStream Then Continue For

                            Dim dataLine = srInputFile.ReadLine()
                            Dim replaceFirstColumnWithJob = dataLine.ToLower().StartsWith("job")

                            If firstfileProcessed Then
                                ' Skip this header line
                            Else
                                ' Write the header line
                                If replaceFirstColumnWithJob Then
                                    ' The Job column is already present
                                    swConcatenatedFile.WriteLine(dataLine)
                                Else
                                    ' Add the Job column header
                                    swConcatenatedFile.WriteLine("Job" & ControlChars.Tab & dataLine)
                                End If

                                firstfileProcessed = True
                            End If

                            Do While Not srInputFile.EndOfStream
                                dataLine = srInputFile.ReadLine()
                                If Not String.IsNullOrWhiteSpace(dataLine) Then

                                    If replaceFirstColumnWithJob Then
                                        ' Remove the first column from dataLine
                                        Dim charIndex = dataLine.IndexOf(ControlChars.Tab)
                                        If charIndex >= 0 Then
                                            dataLine = dataLine.Substring(charIndex + 1)
                                        End If
                                    End If

                                    swConcatenatedFile.WriteLine(jobNumber & ControlChars.Tab & dataLine)
                                End If
                            Loop

                        End Using

                    Next    ' For Each fiResultFile
                Next    ' For Each jobFolder

            End Using

            Return True

        Catch ex As Exception
            m_message = "File could not be concatenated: " & currentFile
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "ConcatenateResultFiles, " & m_message & ": " & ex.Message)
            Return False
        End Try

    End Function

    Protected Sub CopyFailedResultsToArchiveFolder()

        Dim result As IJobParams.CloseOutType

        Dim strFailedResultsFolderPath As String = m_mgrParams.GetParam("FailedResultsFolderPath")
        If String.IsNullOrWhiteSpace(strFailedResultsFolderPath) Then strFailedResultsFolderPath = "??Not Defined??"

        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Processing interrupted; copying results to archive folder: " & strFailedResultsFolderPath)

        ' Bump up the debug level if less than 2
        If m_DebugLevel < 2 Then m_DebugLevel = 2

        ' Try to save whatever files are in the work directory (ignore the Job subfolders)
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

    Protected Sub CreateJobToDatasetMapFile(jobsProcessed As List(Of udtJobMetadataForAScore))

        Dim outputFilePath = Path.Combine(m_WorkDir, "Job_to_Dataset_Map.txt")

        Using swMapFile = New StreamWriter(New FileStream(outputFilePath, FileMode.Create, FileAccess.Write, FileShare.Read))
            swMapFile.WriteLine("Job" & ControlChars.Tab & "Tool" & ControlChars.Tab & "Dataset")

            For Each job In jobsProcessed
                swMapFile.WriteLine(job.Job & ControlChars.Tab & job.ToolName & ControlChars.Tab & job.Dataset)
            Next

        End Using

    End Sub

    Protected Function DetermineAScoreParamFilePath(settingsFileName As String) As String
        Dim bestAScoreParamFileName As String

        Dim datasetType = DatasetTypeConstants.Unknown

        If settingsFileName.ToLower().Contains("_cid") Then
            datasetType = DatasetTypeConstants.CID
        End If

        If settingsFileName.ToLower().Contains("_etd") Then
            datasetType = DatasetTypeConstants.ETD
        End If

        If settingsFileName.ToLower().Contains("_hcd") Then
            datasetType = DatasetTypeConstants.HCD
        End If

        Select Case datasetType
            Case DatasetTypeConstants.CID, DatasetTypeConstants.Unknown
                bestAScoreParamFileName = GetBestAScoreParamFile(New List(Of String) From {"AScoreCIDParamFile", "AScoreHCDParamFile", "AScoreETDParamFile"})
            Case DatasetTypeConstants.ETD
                bestAScoreParamFileName = GetBestAScoreParamFile(New List(Of String) From {"AScoreETDParamFile", "AScoreHCDParamFile", "AScoreCIDParamFile"})
            Case DatasetTypeConstants.HCD
                bestAScoreParamFileName = GetBestAScoreParamFile(New List(Of String) From {"AScoreHCDParamFile", "AScoreCIDParamFile", "AScoreETDParamFile"})

            Case Else
                Throw New Exception("Programming bug in ProcessSynopsisFiles; unrecognized value for datasetType: " & datasetType.ToString())
        End Select

        If String.IsNullOrWhiteSpace(bestAScoreParamFileName) Then
            m_message = "Programming bug, AScore parameter file not found in ProcessSynopsisFiles (clsAnalysisResourcesPhosphoFdrAggregator.GetResources should have already flagged this as an error)"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
            Return String.Empty
        End If

        Return Path.Combine(m_WorkDir, bestAScoreParamFileName)

    End Function

    Protected Function DetermineInputFilePaths(
      jobFolder As DirectoryInfo,
      ByRef udtJobMetadata As udtJobMetadataForAScore,
      fileSuffixesToCombine As List(Of String)) As Boolean

        Dim fhtfile = String.Empty
        Dim synFile = String.Empty
        Dim runningSequest = False

        If udtJobMetadata.ToolName.ToLower().StartsWith("sequest") Then
            runningSequest = True
            fhtfile = udtJobMetadata.Dataset & "_fht.txt"
            synFile = udtJobMetadata.Dataset & "_syn.txt"
            udtJobMetadata.ToolNameForAScore = "sequest"
        End If

        If udtJobMetadata.ToolName.ToLower().StartsWith("xtandem") Then
            fhtfile = udtJobMetadata.Dataset & "_xt_fht.txt"
            synFile = udtJobMetadata.Dataset & "_xt_syn.txt"
            udtJobMetadata.ToolNameForAScore = "xtandem"
        End If

        If udtJobMetadata.ToolName.ToLower().StartsWith("msgfplus") Then
            fhtfile = udtJobMetadata.Dataset & "_msgfplus_fht.txt"
            synFile = udtJobMetadata.Dataset & "_msgfplus_syn.txt"
            udtJobMetadata.ToolNameForAScore = "msgfplus"
        End If

        If String.IsNullOrWhiteSpace(fhtfile) Then
            m_message = "Analysis tool " & udtJobMetadata.ToolName & " is not supported by the PhosphoFdrAggregator"
            Return False
        End If

        udtJobMetadata.FirstHitsFilePath = Path.Combine(jobFolder.FullName, fhtfile)
        udtJobMetadata.SynopsisFilePath = Path.Combine(jobFolder.FullName, synFile)

        Dim success As Boolean

        If Not File.Exists(udtJobMetadata.FirstHitsFilePath) Then
            Dim fhtFileAlternate = clsPHRPReader.AutoSwitchToLegacyMSGFDBIfRequired(udtJobMetadata.FirstHitsFilePath, "Dataset_msgfdb.txt")
            If File.Exists(fhtFileAlternate) Then
                udtJobMetadata.FirstHitsFilePath = fhtFileAlternate
                fhtfile = Path.GetFileName(fhtFileAlternate)
            End If
        End If

        If Not File.Exists(udtJobMetadata.SynopsisFilePath) Then
            Dim synFileAlternate = clsPHRPReader.AutoSwitchToLegacyMSGFDBIfRequired(udtJobMetadata.SynopsisFilePath, "Dataset_msgfdb.txt")
            If File.Exists(synFileAlternate) Then
                udtJobMetadata.SynopsisFilePath = synFileAlternate
                synFile = Path.GetFileName(synFileAlternate)
            End If
        End If

        If File.Exists(udtJobMetadata.FirstHitsFilePath) Then
            CacheFileSuffix(fileSuffixesToCombine, udtJobMetadata.Dataset, fhtfile)
            If runningSequest Then
                success = AddMSGFSpecProbValues(udtJobMetadata.Job, udtJobMetadata.FirstHitsFilePath, "fht")
                If Not success Then Return False
            End If
        Else
            udtJobMetadata.FirstHitsFilePath = String.Empty
        End If

        If File.Exists(udtJobMetadata.SynopsisFilePath) Then
            CacheFileSuffix(fileSuffixesToCombine, udtJobMetadata.Dataset, synFile)
            If runningSequest Then
                success = AddMSGFSpecProbValues(udtJobMetadata.Job, udtJobMetadata.SynopsisFilePath, "syn")
                If Not success Then Return False
            End If
        Else
            udtJobMetadata.SynopsisFilePath = String.Empty
        End If

        If String.IsNullOrWhiteSpace(udtJobMetadata.FirstHitsFilePath) AndAlso String.IsNullOrWhiteSpace(udtJobMetadata.SynopsisFilePath) Then
            LogWarning("Did not find a synopsis or first hits file for job " & udtJobMetadata.Job)
            Return False
        End If

        Return True

    End Function

    Private Function DetermineSpectrumFilePath(diJobFolder As DirectoryInfo) As String

        Dim dtaFiles = diJobFolder.GetFiles("*_dta.zip")
        If dtaFiles.Count > 0 Then
            Dim dtaFile = dtaFiles.First
            If Not UnzipFile(dtaFile.FullName) Then
                m_message = "Error unzipping " & dtaFile.Name
                Return String.Empty
            End If

            Return Path.Combine(m_WorkDir, Path.GetFileNameWithoutExtension(dtaFile.Name) & ".txt")
        End If

        Dim mzMLFiles = diJobFolder.GetFiles("*.mzML.gz")
        If mzMLFiles.Count > 0 Then
            Dim mzMLFile = mzMLFiles.First
            If Not GUnzipFile(mzMLFile.FullName) Then
                m_message = "Error unzipping " & mzMLFile.Name
                Return String.Empty
            End If

            Return Path.Combine(m_WorkDir, Path.GetFileNameWithoutExtension(mzMLFile.Name))
        End If

        m_message = "Folder " & diJobFolder.Name & " does not have a _dta.zip file or .mzML.gz file"
        Return String.Empty

    End Function

    Private Function GetBestAScoreParamFile(parameterNames As IEnumerable(Of String)) As String

        For Each paramName In parameterNames
            Dim paramFileName = m_jobParams.GetJobParameter(paramName, String.Empty)
            If String.IsNullOrWhiteSpace(paramFileName) Then
                Continue For
            End If

            If File.Exists(Path.Combine(m_WorkDir, paramFileName)) Then
                Return paramFileName
            End If
        Next

        Return String.Empty

    End Function

    ''' <summary>
    ''' Finds the folders that start with Job
    ''' </summary>
    ''' <returns>Dictionary where key is the Job number and value is a DirectoryInfo object</returns>
    ''' <remarks></remarks>
    Protected Function GetJobFolderList() As Dictionary(Of Integer, DirectoryInfo)

        Dim jobFolderList = New Dictionary(Of Integer, DirectoryInfo)

        Dim diWorkingFolder = New DirectoryInfo(m_WorkDir)

        For Each jobFolder In diWorkingFolder.GetDirectories("Job*")

            Dim jobNumberText = jobFolder.Name.Substring(3)
            Dim jobNumber As Integer

            If Integer.TryParse(jobNumberText, jobNumber) Then
                jobFolderList.Add(jobNumber, jobFolder)
            End If
        Next

        Return jobFolderList

    End Function

    ''' <summary>
    ''' Parse the ProMex console output file to track the search progress
    ''' </summary>
    ''' <param name="strConsoleOutputFilePath"></param>
    ''' <remarks></remarks>
    Private Sub ParseConsoleOutputFile(strConsoleOutputFilePath As String)

        ' Example Console output
        '
        ' Fragment Type:  HCD
        ' Mass Tolerance: 0.05 Da
        ' Caching data in E:\DMS_WorkDir\Job1153717\HarrisMS_batch2_Ppp_A4-2_22Dec14_Frodo_14-12-07_msgfplus_syn.txt
        ' Computing AScore values and Writing results to E:\DMS_WorkDir
        ' Modifications for Dataset: HarrisMS_batch2_Ppp_A4-2_22Dec14_Frodo_14-12-07
        ' 	Static,   57.021465 on C
        ' 	Dynamic,  79.966331 on STY
        ' Percent Completion 0%
        ' Percent Completion 0%
        ' Percent Completion 1%
        ' Percent Completion 1%
        ' Percent Completion 1%
        ' Percent Completion 2%
        ' Percent Completion 2%

        Const REGEX_AScore_PROGRESS = "Percent Completion (\d+)\%"

        Static reCheckProgress As New Regex(REGEX_AScore_PROGRESS, RegexOptions.Compiled Or RegexOptions.IgnoreCase)

        Try
            If Not File.Exists(strConsoleOutputFilePath) Then
                If m_DebugLevel >= 4 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Console output file not found: " & strConsoleOutputFilePath)
                End If

                Exit Sub
            End If

            If m_DebugLevel >= 4 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Parsing file " & strConsoleOutputFilePath)
            End If

            ' Value between 0 and 100
            Dim ascoreProgress = 0
            mConsoleOutputErrorMsg = String.Empty

            Using srInFile = New StreamReader(New FileStream(strConsoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))

                Do While Not srInFile.EndOfStream
                    Dim strLineIn = srInFile.ReadLine()

                    If Not String.IsNullOrWhiteSpace(strLineIn) Then

                        Dim strLineInLCase = strLineIn.ToLower()

                        If strLineInLCase.StartsWith("error:") OrElse strLineInLCase.Contains("unhandled exception") Then
                            If String.IsNullOrEmpty(mConsoleOutputErrorMsg) Then
                                mConsoleOutputErrorMsg = "Error running AScore:"
                            End If
                            mConsoleOutputErrorMsg &= "; " & strLineIn
                            Continue Do

                        Else
                            Dim oMatch As Match = reCheckProgress.Match(strLineIn)
                            If oMatch.Success Then
                                Integer.TryParse(oMatch.Groups(1).ToString(), ascoreProgress)
                                Continue Do
                            End If

                        End If

                    End If
                Loop

            End Using

            Dim percentCompleteStart = mJobFoldersProcessed / CSng(mTotalJobFolders) * 100.0F
            Dim percentCompleteEnd = (mJobFoldersProcessed + 1) / CSng(mTotalJobFolders) * 100.0F
            Dim subtaskProgress = ComputeIncrementalProgress(percentCompleteStart, percentCompleteEnd, ascoreProgress)

            Dim progressComplete = ComputeIncrementalProgress(PROGRESS_PCT_PHOSPHO_FDR_RUNNING, PROGRESS_PCT_PHOSPHO_FDR_COMPLETE, subtaskProgress)

            If m_progress < progressComplete Then
                m_progress = progressComplete
            End If

        Catch ex As Exception
            ' Ignore errors here
            If m_DebugLevel >= 2 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error parsing console output file (" & strConsoleOutputFilePath & "): " & ex.Message)
            End If
        End Try

    End Sub

    ''' <summary>
    ''' Run AScore against the Synopsis and First hits files in the Job subfolders
    ''' </summary>
    ''' <param name="progLoc">AScore exe path</param>
    ''' <param name="fileSuffixesToCombine">Output parameter: File suffixes that were processed</param>
    ''' <param name="processingRuntimes">Output parameter: AScore Runtime (in minutes) for each job/tag combo</param>
    ''' <returns>True if success, false if an error</returns>
    ''' <remarks></remarks>
    Protected Function ProcessSynopsisFiles(
      progLoc As String,
      <Out> ByRef fileSuffixesToCombine As List(Of String),
      <Out> ByRef processingRuntimes As Dictionary(Of String, Double)) As Boolean

        Dim successOverall = True

        fileSuffixesToCombine = New List(Of String)
        processingRuntimes = New Dictionary(Of String, Double)

        Try

            ' Extract the dataset raw file paths
            Dim jobToDatasetMap = ExtractPackedJobParameterDictionary(clsAnalysisResources.JOB_PARAM_DICTIONARY_JOB_DATASET_MAP)
            Dim jobToSettingsFileMap = ExtractPackedJobParameterDictionary(clsAnalysisResources.JOB_PARAM_DICTIONARY_JOB_SETTINGS_FILE_MAP)
            Dim jobToToolMap = ExtractPackedJobParameterDictionary(clsAnalysisResources.JOB_PARAM_DICTIONARY_JOB_TOOL_MAP)
            Dim jobsProcessed = New List(Of udtJobMetadataForAScore)

            Dim jobCountSkipped = 0

            Dim jobFolderlist = GetJobFolderList()

            m_progress = PROGRESS_PCT_PHOSPHO_FDR_RUNNING

            mCmdRunner = New clsRunDosProgram(m_WorkDir)
            RegisterEvents(mCmdRunner)
            AddHandler mCmdRunner.LoopWaiting, AddressOf CmdRunner_LoopWaiting

            mJobFoldersProcessed = 0
            mTotalJobFolders = jobFolderlist.Count

            ' Process each Job folder
            For Each jobFolder In jobFolderlist

                Dim synopsisFiles = jobFolder.Value.GetFiles("*syn*.txt")

                Dim firstHitsFiles = jobFolder.Value.GetFiles("*fht*.txt")
                If synopsisFiles.Count + firstHitsFiles.Count = 0 Then
                    Continue For
                End If

                Dim udtJobMetadata = New udtJobMetadataForAScore()
                udtJobMetadata.Job = jobFolder.Key

                Dim datasetName = String.Empty
                Dim settingsFileName = String.Empty

                If Not jobToDatasetMap.TryGetValue(udtJobMetadata.Job.ToString(), datasetName) Then
                    m_message = "Job " & udtJobMetadata.Job & " not found in packed job parameter " & clsAnalysisResources.JOB_PARAM_DICTIONARY_JOB_DATASET_MAP
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error in ProcessSynopsisFiles: " & m_message)
                    Return False
                End If

                udtJobMetadata.Dataset = datasetName

                settingsFileName = jobToSettingsFileMap(udtJobMetadata.Job.ToString())
                udtJobMetadata.ToolName = jobToToolMap(udtJobMetadata.Job.ToString())

                ' Determine the AScore parameter file to use
                Dim bestAScoreParamFilePath = DetermineAScoreParamFilePath(settingsFileName)

                If String.IsNullOrWhiteSpace(bestAScoreParamFilePath) Then
                    Return False
                End If

                ' Find the spectrum file; should be _dta.zip or .mzML.gz
                udtJobMetadata.SpectrumFilePath = DetermineSpectrumFilePath(jobFolder.Value)

                If String.IsNullOrWhiteSpace(udtJobMetadata.SpectrumFilePath) Then
                    Return False
                End If

                ' Find any first hits and synopsis files
                Dim success = DetermineInputFilePaths(jobFolder.Value, udtJobMetadata, fileSuffixesToCombine)
                If Not success Then
                    jobCountSkipped += 1
                Else

                    If Not String.IsNullOrWhiteSpace(udtJobMetadata.FirstHitsFilePath) Then
                        ' Analyze the first hits file with AScore
                        success = RunAscore(progLoc, udtJobMetadata, udtJobMetadata.FirstHitsFilePath, bestAScoreParamFilePath, "fht", processingRuntimes)
                        If Not success Then
                            ' An error has already been logged, and m_message has been updated
                            successOverall = False
                        End If
                    End If

                    If Not String.IsNullOrWhiteSpace(udtJobMetadata.SynopsisFilePath) Then
                        ' Analyze the synopsis file with AScore
                        success = RunAscore(progLoc, udtJobMetadata, udtJobMetadata.SynopsisFilePath, bestAScoreParamFilePath, "syn", processingRuntimes)
                        If Not success Then
                            ' An error has already been logged, and m_message has been updated
                            successOverall = False
                        End If
                    End If

                    jobsProcessed.Add(udtJobMetadata)
                End If

                ' Delete the unzipped spectrum file
                Try
                    File.Delete(udtJobMetadata.SpectrumFilePath)
                Catch ex As Exception
                    ' Ignore errors
                End Try

                mJobFoldersProcessed += 1
                Dim subTaskProgress = mJobFoldersProcessed / CSng(mTotalJobFolders) * 100.0F

                m_progress = ComputeIncrementalProgress(PROGRESS_PCT_PHOSPHO_FDR_RUNNING, PROGRESS_PCT_PHOSPHO_FDR_COMPLETE, subTaskProgress)
            Next

            If jobCountSkipped > 0 Then
                m_message = clsGlobal.AppendToComment(m_message, "Skipped " & jobCountSkipped & " job(s) because a synopsis or first hits file was not found")
            End If


            ' Create the job to dataset map file
            CreateJobToDatasetMapFile(jobsProcessed)


        Catch ex As Exception
            If String.IsNullOrEmpty(m_message) Then m_message = "Error in ProcessSynopsisFiles"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error in ProcessSynopsisFiles: " & ex.Message)
            Return False
        End Try

        Return successOverall

    End Function

    ''' <summary>
    ''' Runs ascore on the specified file
    ''' </summary>
    ''' <param name="progLoc"></param>
    ''' <param name="udtJobMetadata"></param>
    ''' <param name="inputFilePath"></param>
    ''' <param name="ascoreParamFilePath"></param>
    ''' <param name="fileTypeTag">Should be syn or fht; appened to the AScore_ConsoleOutput file</param>
    ''' <param name="processingRuntimes">Output parameter: AScore Runtime (in minutes) for each job/tag combo</param>
    ''' <returns>True if success, false if an error</returns>
    ''' <remarks></remarks>
    Protected Function RunAscore(
      progLoc As String,
      udtJobMetadata As udtJobMetadataForAScore,
      inputFilePath As String,
      ascoreParamFilePath As String,
      fileTypeTag As String,
      processingRuntimes As Dictionary(Of String, Double)) As Boolean

        ' Set up and execute a program runner to run AScore

        mConsoleOutputErrorMsg = String.Empty

        Dim fiSourceFile = New FileInfo(inputFilePath)
        Dim currentWorkingDir = fiSourceFile.Directory.FullName
        Dim updatedInputFileName = Path.GetFileNameWithoutExtension(fiSourceFile.Name) & FILE_SUFFIX_SYN_PLUS_ASCORE

        Dim cmdStr = ""

        ' Search engine name
        cmdStr &= " -T:" & udtJobMetadata.ToolNameForAScore

        ' Input file path
        cmdStr &= " -F:" & PossiblyQuotePath(inputFilePath)

        ' DTA or mzML file path
        cmdStr &= " -D:" & PossiblyQuotePath(udtJobMetadata.SpectrumFilePath)

        ' AScore parameter file
        cmdStr &= " -P:" & PossiblyQuotePath(ascoreParamFilePath)

        ' Output folder
        cmdStr &= " -O:" & PossiblyQuotePath(currentWorkingDir)

        ' Create an updated version of the input file, with updated peptide sequences and appended AScore-related columns
        cmdStr &= " -U:" & PossiblyQuotePath(updatedInputFileName)

        If m_DebugLevel >= 1 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, progLoc & cmdStr)
        End If

        mCmdRunner = New clsRunDosProgram(m_WorkDir)
        RegisterEvents(mCmdRunner)
        AddHandler mCmdRunner.LoopWaiting, AddressOf CmdRunner_LoopWaiting

        If String.IsNullOrWhiteSpace(fileTypeTag) Then
            fileTypeTag = ""
        End If

        With mCmdRunner
            .CreateNoWindow = True
            .CacheStandardOutput = False
            .EchoOutputToConsole = True

            .WriteConsoleOutputToFile = True
            .ConsoleOutputFilePath = Path.Combine(currentWorkingDir, ASCORE_CONSOLE_OUTPUT_PREFIX & "_" & fileTypeTag & ".txt")
        End With

        Dim dtStartTime = DateTime.UtcNow

        Dim blnSuccess = mCmdRunner.RunProgram(progLoc, cmdStr, "AScore", True)

        Dim runtimeMinutes = DateTime.UtcNow.Subtract(dtStartTime).TotalMinutes
        processingRuntimes.Add(udtJobMetadata.Job & fileTypeTag, runtimeMinutes)

        If Not mCmdRunner.WriteConsoleOutputToFile Then
            ' Write the console output to a text file
            System.Threading.Thread.Sleep(250)

            Dim swConsoleOutputfile = New StreamWriter(New FileStream(mCmdRunner.ConsoleOutputFilePath, FileMode.Create, FileAccess.Write, FileShare.Read))
            swConsoleOutputfile.WriteLine(mCmdRunner.CachedConsoleOutput)
            swConsoleOutputfile.Close()
        End If

        If Not String.IsNullOrEmpty(mConsoleOutputErrorMsg) Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mConsoleOutputErrorMsg)
        End If

        ' Parse the console output file one more time to check for errors
        System.Threading.Thread.Sleep(250)
        ParseConsoleOutputFile(mCmdRunner.ConsoleOutputFilePath)

        If Not String.IsNullOrEmpty(mConsoleOutputErrorMsg) Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mConsoleOutputErrorMsg)
        End If


        If Not blnSuccess Then
            Dim msg As String = "Error running AScore for job " & udtJobMetadata.Job
            m_message = clsGlobal.AppendToComment(m_message, msg)

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg & ", file " & fiSourceFile.Name & ", data package job " & udtJobMetadata.Job)

            If mCmdRunner.ExitCode <> 0 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "AScore returned a non-zero exit code: " & mCmdRunner.ExitCode.ToString)
            Else
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Call to AScore failed (but exit code is 0)")
            End If

            Return False

        End If

        m_StatusTools.UpdateAndWrite(m_progress)
        If m_DebugLevel >= 3 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "AScore search complete for data package job " & udtJobMetadata.Job)
        End If

        Return True

    End Function

    ''' <summary>
    ''' Stores the tool version info in the database
    ''' </summary>
    ''' <remarks></remarks>
    Protected Function StoreToolVersionInfo(strProgLoc As String) As Boolean

        Dim strToolVersionInfo As String = String.Empty
        Dim blnSuccess As Boolean

        If m_DebugLevel >= 2 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info")
        End If

        Dim fiProgram = New FileInfo(strProgLoc)
        If Not fiProgram.Exists Then
            Try
                strToolVersionInfo = "Unknown"
                Return MyBase.SetStepTaskToolVersion(strToolVersionInfo, New List(Of FileInfo), blnSaveToolVersionTextFile:=False)
            Catch ex As Exception
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion: " & ex.Message)
                Return False
            End Try

        End If

        ' Lookup the version of the .NET application
        blnSuccess = MyBase.StoreToolVersionInfoOneFile(strToolVersionInfo, fiProgram.FullName)
        If Not blnSuccess Then Return False


        ' Store paths to key DLLs in ioToolFiles
        Dim ioToolFiles = New List(Of FileInfo)
        ioToolFiles.Add(fiProgram)

        ioToolFiles.Add(New FileInfo(Path.Combine(fiProgram.Directory.FullName, "AScore_DLL.dll")))

        Try
            Return MyBase.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles, blnSaveToolVersionTextFile:=False)
        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion: " & ex.Message)
            Return False
        End Try

    End Function

    ''' <summary>
    ''' Event handler for mCmdRunner.LoopWaiting event
    ''' </summary>
    ''' <remarks></remarks>
    Private Sub CmdRunner_LoopWaiting()

        Static dtLastConsoleOutputParse As DateTime = DateTime.UtcNow

        UpdateStatusFile()

        ' Parse the console output file every 15 seconds
        If DateTime.UtcNow.Subtract(dtLastConsoleOutputParse).TotalSeconds >= 15 Then
            dtLastConsoleOutputParse = DateTime.UtcNow

            ParseConsoleOutputFile(mCmdRunner.ConsoleOutputFilePath)

            LogProgress("PhosphoFdrAggregator")
        End If

    End Sub

#End Region

End Class
