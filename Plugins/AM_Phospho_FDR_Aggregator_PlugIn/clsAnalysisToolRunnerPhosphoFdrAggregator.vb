Option Strict On

'*********************************************************************************************************
' Written by Matt Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2009, Battelle Memorial Institute
'
'*********************************************************************************************************

Imports AnalysisManagerBase
Imports System.Linq
Imports System.IO
Imports System.Runtime.InteropServices

Public Class clsAnalysisToolRunnerPhosphoFdrAggregator
    Inherits clsAnalysisToolRunnerBase

    '*********************************************************************************************************
    'Class for running Phospho_FDRAggregator analysis
    '*********************************************************************************************************

#Region "Enums"
    Protected Enum DatasetTypeConstants
        Unknown = 0
        CID = 1
        ETD = 2
        HCD = 3
    End Enum
#End Region

#Region "Module Variables"
    Protected Const PROGRESS_PCT_PHOSPHO_FDR_RUNNING As Single = 5
    Protected Const PROGRESS_PCT_PHOSPHO_FDR_START As Single = 95
    Protected Const PROGRESS_PCT_PHOSPHO_FDR_COMPLETE As Single = 99

    Protected WithEvents CmdRunner As clsRunDosProgram
#End Region

#Region "Methods"
    ''' <summary>
    ''' Runs PhosphoFdrAggregator tool
    ''' </summary>
    ''' <returns>CloseOutType enum indicating success or failure</returns>
    ''' <remarks></remarks>
    Public Overrides Function RunTool() As IJobParams.CloseOutType

        Dim CmdStr As String
        Dim result As IJobParams.CloseOutType

        'Do the base class stuff
        If Not MyBase.RunTool = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        ' Store the AScore version info in the database
        If Not StoreToolVersionInfo() Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Aborting since StoreToolVersionInfo returned false")
            m_message = "Error determining AScore version"
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If


        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Running AScore")

        CmdRunner = New clsRunDosProgram(m_WorkDir)

        If m_DebugLevel > 4 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerPhosphoFdrAggregator.RunTool(): Enter")
        End If

        ' Verify that program file exists
        ' AScoreProgLoc will be something like this: "C:\DMS_Programs\AScore\AScore_Console.exe"
        Dim progLoc As String = m_mgrParams.GetParam("AScoreprogloc")
        If Not System.IO.File.Exists(progLoc) Then
            If progLoc.Length = 0 Then progLoc = "Parameter 'AScoreprogloc' not defined for this manager"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Cannot find AScore program file: " & progLoc)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        ' Run AScore for each of the jobs in the data package
        Dim success = ProcessSynopsisFiles(progLoc)

        ' Sleep 2 seconds before continuing
        System.Threading.Thread.Sleep(2000)        '2 second delay

        If Not String.IsNullOrEmpty(m_jobParams.GetParam("AScoreCIDParamFile")) Then
            result = ConcatenateResultFiles("_cid_outputAScore.txt")
            If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                'TODO: What do we do here?
                Return result
            End If
        End If

        'Stop the job timer
        m_StopTime = System.DateTime.UtcNow

        'Add the current job data to the summary file
        If Not UpdateSummaryFile() Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN, "Error creating summary file, job " & m_JobNum & ", step " & m_jobParams.GetParam("Step"))
        End If

        'Make sure objects are released
        System.Threading.Thread.Sleep(2000)        '2 second delay
        PRISM.Processes.clsProgRunner.GarbageCollectNow()

        ' Override the dataset name and transfer folder path so that the results get copied to the correct location
        MyBase.RedefineAggregationJobDatasetAndTransferFolder()

        result = MakeResultsFolder()
        If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            'TODO: What do we do here?
            Return result
        End If

        Dim DumFiles() As String

        'update list of files to be deleted after run
        DumFiles = System.IO.Directory.GetFiles(m_WorkDir, "*_outputAScore*")
        For Each FileToSave As String In DumFiles
            m_jobParams.AddResultFileToKeep(System.IO.Path.GetFileName(FileToSave))
        Next

        result = MoveResultFiles()
        If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            'TODO: What do we do here?
            Return result
        End If

        result = CopyResultsFolderToServer()
        If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            'TODO: What do we do here?
            Return result
        End If

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS 'ZipResult

    End Function

    Private Function ProcessSynopsisFiles(ByVal progLoc As String) As Boolean

        Try
            Dim diWorkingFolder = New DirectoryInfo(m_WorkDir)


            ' Extract the dataset raw file paths
            Dim jobToDatasetMap = ExtractPackedJobParameterDictionary(clsAnalysisResources.JOB_PARAM_DICTIONARY_JOB_DATASET_MAP)
            Dim jobToSettingsFileMap = ExtractPackedJobParameterDictionary(clsAnalysisResources.JOB_PARAM_DICTIONARY_JOB_SETTINGS_FILE_MAP)
            Dim jobToToolMap = ExtractPackedJobParameterDictionary(clsAnalysisResources.JOB_PARAM_DICTIONARY_JOB_TOOL_MAP)

            ' Find folders with synopsis files
            For Each jobFolder In diWorkingFolder.GetDirectories("Job*")

                Dim synopsisFiles = jobFolder.GetFiles("*syn*.txt")
                If synopsisFiles.Count = 0 Then
                    Continue For
                End If

                Dim jobNumber = Integer.Parse(jobFolder.Name.Substring(3))
                Dim datasetName = String.Empty
                Dim settingsFileName = String.Empty
                Dim toolName = String.Empty

                If Not jobToDatasetMap.TryGetValue(jobNumber.ToString(), datasetName) Then
                    m_message = "Job " & jobNumber & " not found in packed job parameter " & clsAnalysisResources.JOB_PARAM_DICTIONARY_JOB_DATASET_MAP
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error in ProcessSynopsisFiles: " & m_message)
                    Return False
                End If

                settingsFileName = jobToSettingsFileMap(jobNumber.ToString())
                toolName = jobToToolMap(jobNumber.ToString())

                ' Determine the AScore parameter file to use
                Dim bestAScoreParamFilePath = DetermineAScoreParamFilePath(settingsFileName)

                If String.IsNullOrWhiteSpace(bestAScoreParamFilePath) Then
                    Return False
                End If

                ' Find the spectrum file; should be _dta.zip or .mzML.gz
                Dim spectrumFilePath = DetermineSpectrumFilePath(jobFolder)

                If String.IsNullOrWhiteSpace(spectrumFilePath) Then
                    Return False
                End If
                
                ' Find the first hits and synopsis files
                Dim fhtFile = String.Empty
                Dim synFile = String.Empty

                Dim success = DetermineInputFilePaths(jobFolder, jobNumber, datasetName, toolName, fhtFile, synFile)
                If Not success Then
                    Return False
                End If


                Dim cmdStr = String.Empty
                cmdStr &= fhtFile & synFile


                With CmdRunner
                    .CreateNoWindow = False
                    .CacheStandardOutput = False
                    .EchoOutputToConsole = False

                    .WriteConsoleOutputToFile = True
                End With

                If Not CmdRunner.RunProgram(progLoc, cmdStr, "AScore", True) Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error running AScore, job " & m_JobNum)

                    Return False
                End If


            Next

        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error in ProcessSynopsisFiles: " & ex.Message)
            Return False
        End Try

    End Function

    Protected Function ConcatenateResultFiles(ByVal FilterExtension As String) As IJobParams.CloseOutType
        Dim result As Boolean = True
        Dim ConcatenateAScoreFiles() As String
        Dim FileToConcatenate As String = String.Empty
        Dim bSkipFirstLine As Boolean = False

        Try

            ConcatenateAScoreFiles = System.IO.Directory.GetFiles(m_WorkDir, "*" & FilterExtension)

            ' Create an instance of StreamWriter to write to a file.
            Using swConcatenatedFile As System.IO.StreamWriter = New System.IO.StreamWriter(System.IO.Path.Combine(m_WorkDir, "Concatenated" & FilterExtension))

                For Each FullFileToConcatenate As String In ConcatenateAScoreFiles
                    FileToConcatenate = System.IO.Path.GetFileName(FullFileToConcatenate)

                    ' Create an instance of StreamReader to read from a file.
                    Using srInputFile As System.IO.StreamReader = New System.IO.StreamReader(System.IO.Path.Combine(m_WorkDir, FileToConcatenate))

                        Dim inpLine As String
                        If bSkipFirstLine Then
                            If srInputFile.Peek > -1 Then
                                srInputFile.ReadLine()
                            End If
                        Else
                            ' Skip the first line (the header line) on subsequent files
                            bSkipFirstLine = True
                        End If

                        Do While srInputFile.Peek > -1
                            inpLine = srInputFile.ReadLine()
                            If Not inpLine Is Nothing Then
                                swConcatenatedFile.WriteLine(inpLine)
                            End If
                        Loop

                    End Using

                Next

            End Using

        Catch ex As Exception
            ' Let the user know what went wrong.
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerPhosphoFdrAggregator.ConcatenateResultFiles, The file could not be concatenated: " & FileToConcatenate & ex.Message)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
    End Function

    Protected Sub CopyFailedResultsToArchiveFolder()

        Dim result As IJobParams.CloseOutType

        Dim strFailedResultsFolderPath As String = m_mgrParams.GetParam("FailedResultsFolderPath")
        If String.IsNullOrEmpty(strFailedResultsFolderPath) Then strFailedResultsFolderPath = "??Not Defined??"

        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Processing interrupted; copying results to archive folder: " & strFailedResultsFolderPath)

        ' Bump up the debug level if less than 2
        If m_DebugLevel < 2 Then m_DebugLevel = 2

        ' Try to save whatever files are in the work directory (however, delete the _dta.zip file first)
        Dim strFolderPathToArchive As String
        strFolderPathToArchive = String.Copy(m_WorkDir)

        Try
            System.IO.File.Delete(System.IO.Path.Combine(m_WorkDir, m_Dataset & "_dta.zip"))
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

    Private Function DetermineAScoreParamFilePath(ByVal settingsFileName As String) As String
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

    Private Function DetermineInputFilePaths(
      ByVal jobFolder As DirectoryInfo,
      ByVal jobNumber As Integer,
      ByVal datasetName As String,
      ByVal toolName As String,
      <Out> ByRef fhtFile As String,
      <Out> ByRef synFile As String) As Boolean

        fhtFile = String.Empty
        synFile = String.Empty


        If toolName.ToLower().StartsWith("sequest") Then
            fhtFile = datasetName & "_fht.txt"
            synFile = datasetName & "_syn.txt"
        End If

        If toolName.ToLower().StartsWith("xtandem") Then
            fhtFile = datasetName & "_xt_fht.txt"
            synFile = datasetName & "_xt_syn.txt"
        End If

        If toolName.ToLower().StartsWith("msgfplus") Then
            fhtFile = datasetName & "_msgfdb_fht.txt"
            synFile = datasetName & "_msgfdb_syn.txt"
        End If

        If String.IsNullOrWhiteSpace(fhtFile) Then
            m_message = "Analysis tool " & toolName & " is not supported by the PhosphoFdrAggregator"
            Return False
        End If

        fhtFile = Path.Combine(jobFolder.FullName, fhtFile)
        synFile = Path.Combine(jobFolder.FullName, synFile)

        If Not File.Exists(fhtFile) Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "FHT file not found for job " & jobNumber)
            fhtFile = String.Empty
        End If

        If Not File.Exists(synFile) Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "SYN file not found for job " & jobNumber)
            synFile = String.Empty
        End If

        Return True

    End Function

    Private Function DetermineSpectrumFilePath(ByVal diJobFolder As DirectoryInfo) As String

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

    Private Function GetBestAScoreParamFile(ByVal parameterNames As IEnumerable(Of String)) As String

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
    ''' Stores the tool version info in the database
    ''' </summary>
    ''' <remarks></remarks>
    Protected Function StoreToolVersionInfo() As Boolean

        Dim strToolVersionInfo As String = String.Empty

        If m_DebugLevel >= 2 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info")
        End If

        ' Store paths to key files in ioToolFiles
        Dim ioToolFiles As New System.Collections.Generic.List(Of System.IO.FileInfo)
        ioToolFiles.Add(New System.IO.FileInfo(m_mgrParams.GetParam("AScoreprogloc")))

        Try
            Return MyBase.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles)
        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion: " & ex.Message)
            Return False
        End Try

    End Function
    
    ''' <summary>
    ''' Event handler for CmdRunner.LoopWaiting event
    ''' </summary>
    ''' <remarks></remarks>
    Private Sub CmdRunner_LoopWaiting() Handles CmdRunner.LoopWaiting
        Static dtLastStatusUpdate As System.DateTime = System.DateTime.UtcNow

        ' Synchronize the stored Debug level with the value stored in the database
        Const MGR_SETTINGS_UPDATE_INTERVAL_SECONDS As Integer = 300
        MyBase.GetCurrentMgrSettingsFromDB(MGR_SETTINGS_UPDATE_INTERVAL_SECONDS)

        'Update the status file (limit the updates to every 5 seconds)
        If System.DateTime.UtcNow.Subtract(dtLastStatusUpdate).TotalSeconds >= 5 Then
            dtLastStatusUpdate = System.DateTime.UtcNow
            m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.RUNNING_TOOL, PROGRESS_PCT_PHOSPHO_FDR_RUNNING, 0, "", "", "", False)
        End If

    End Sub

#End Region

End Class
