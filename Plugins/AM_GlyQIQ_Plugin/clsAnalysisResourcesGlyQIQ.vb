' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Created 05/29/2014
'
'*********************************************************************************************************

Option Strict On

Imports AnalysisManagerBase
Imports System.IO

Public Class clsAnalysisResourcesGlyQIQ
	Inherits clsAnalysisResources

    ' Public Const WORKING_PARAMETERS_FOLDER_NAME As String = "WorkingParameters"
    Protected Const LOCKS_FOLDER_NAME As String = "LocksFolder"
    
    Public Const JOB_PARAM_ACTUAL_CORE_COUNT = "GlyQ_IQ_ActualCoreCount"

    Public Const EXECUTOR_PARAMETERS_FILE As String = "ExecutorParametersSK.xml"
    Public Const START_PROGRAM_BATCH_FILE_PREFIX As String = "StartProgram_Core"
    Public Const GLYQIQ_PARAMS_FILE_PREFIX As String = "GlyQIQ_Params_"

    Public Const ALIGNMENT_PARAMETERS_FILENAME As String = "AlignmentParameters.xml"

    Protected Structure udtGlyQIQParams
        'Public ApplicationsFolderPath As String
        Public WorkingParameterFolders As Dictionary(Of Integer, DirectoryInfo)
        Public FactorsName As String
        Public TargetsName As String
        Public NumTargets As Integer
        'Public TimeStamp As String
        Public ConsoleOperatingParametersFileName As String
        'Public OperationParametersFileName As String
        Public IQParamFileName As String
    End Structure

#Region "Classwide variables"
    Private mGlyQIQParams As udtGlyQIQParams
#End Region

    Public Overrides Function GetResources() As IJobParams.CloseOutType

        mGlyQIQParams = New udtGlyQIQParams()

        Dim coreCountText = m_jobParams.GetJobParameter("GlyQ-IQ", "Cores", "All")

        ' Use all the cores if the system has 4 or fewer cores
        ' Otherwise, use TotalCoreCount - 1
        Dim maxAllowedCores = m_StatusTools.GetCoreCount()
        If maxAllowedCores > 4 Then maxAllowedCores -= 1

        Dim coreCount As Integer = clsAnalysisToolRunnerBase.ParseThreadCount(coreCountText, maxAllowedCores, m_StatusTools)

        m_jobParams.AddAdditionalParameter("GlyQ-IQ", JOB_PARAM_ACTUAL_CORE_COUNT, coreCount.ToString())

        mGlyQIQParams.WorkingParameterFolders = CreateSubFolders(coreCount)
        If mGlyQIQParams.WorkingParameterFolders.Count = 0 Then
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        If Not RetrieveGlyQIQParameters(coreCount) Then
            Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
        End If

        If Not RetrievePeaksAndRawData() Then
            Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
        End If

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    Private Function CopyFileToWorkingDirectories(
        ByVal sourceFileName As String,
        ByVal sourceFolderPath As String,
        ByVal fileDesription As String) As Boolean

        For Each workingDirectory In mGlyQIQParams.WorkingParameterFolders
            If Not CopyFileToWorkDir(sourceFileName, sourceFolderPath, workingDirectory.Value.FullName) Then
                m_message &= " (" & fileDesription & ")"
                Return False
            End If
        Next

        Return True

    End Function

    Private Function CountTargets(ByVal targetsFilePath As String) As Integer
        Try
            ' numTargets is initialized to -1 because we don't want to count the header line
            Dim numTargets As Integer = -1

            Using srInFile = New StreamReader(New FileStream(targetsFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))

                While Not srInFile.EndOfStream
                    srInFile.ReadLine()
                    numTargets += 1
                End While

            End Using

            Return numTargets

        Catch ex As Exception
            m_message = "Exception counting the targets in " & Path.GetFileName(targetsFilePath)
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message)
            Return 0
        End Try

    End Function

    Private Function CreateConsoleOperatingParametersFile() As Boolean

        Try

            ' Define the output file name
            mGlyQIQParams.ConsoleOperatingParametersFileName = GLYQIQ_PARAMS_FILE_PREFIX & m_DatasetName & ".txt"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Creating the Operating Parameters file, " & mGlyQIQParams.ConsoleOperatingParametersFileName)

            For Each workingDirectory In mGlyQIQParams.WorkingParameterFolders

                Dim outputFilePath = Path.Combine(workingDirectory.Value.FullName, mGlyQIQParams.ConsoleOperatingParametersFileName)

                Using swOutFile = New StreamWriter(New FileStream(outputFilePath, FileMode.Create, FileAccess.Write, FileShare.Read))
                    swOutFile.WriteLine("ResultsFolderPath" & "," & Path.Combine(m_WorkingDir, "Results"))
                    swOutFile.WriteLine("LoggingFolderPath" & "," & Path.Combine(m_WorkingDir, "Results"))
                    swOutFile.WriteLine("FactorsFile" & "," & mGlyQIQParams.FactorsName & ".txt")
                    swOutFile.WriteLine("ExecutorParameterFile" & "," & EXECUTOR_PARAMETERS_FILE)
                    swOutFile.WriteLine("XYDataFolder" & "," & "XYDataWriter")
                    swOutFile.WriteLine("WorkflowParametersFile" & "," & mGlyQIQParams.IQParamFileName)
                    swOutFile.WriteLine("Alignment" & "," & Path.Combine(workingDirectory.Value.FullName, ALIGNMENT_PARAMETERS_FILENAME))

                    ' The following file doesn't have to exist
                    swOutFile.WriteLine("BasicTargetedParameters" & "," & Path.Combine(workingDirectory.Value.FullName, "BasicTargetedWorkflowParameters.xml"))

                End Using

            Next

            Return True

        Catch ex As Exception
            m_message = "Exception in CreateConsoleOperatingParametersFile"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message)
            Return False
        End Try

    End Function

    Private Function CreateLauncherBatchFiles(ByVal splitTargetFileInfo As Dictionary(Of Integer, FileInfo)) As Boolean

        Try

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Creating the Launcher batch files")

            ' Determine the path to the IQGlyQ program

            Dim progLoc = clsAnalysisToolRunnerBase.DetermineProgramLocation("GlyQIQ", "GlyQIQProgLoc", "IQGlyQ_Console.exe", "", m_mgrParams, m_message)
            If String.IsNullOrEmpty(progLoc) Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "DetermineProgramLocation returned an empty string: " & m_message)
                Return False
            End If

            For Each workingDirectory In mGlyQIQParams.WorkingParameterFolders

                Dim core = workingDirectory.Key

                Dim batchFilePath = Path.Combine(m_WorkingDir, START_PROGRAM_BATCH_FILE_PREFIX & workingDirectory.Key & ".bat")

                Using swOutFile = New StreamWriter(New FileStream(batchFilePath, FileMode.Create, FileAccess.Write, FileShare.Read))

                    ' Note that clsGlyQIqRunner expects this batch file to be in a specific format:
                    ' GlyQIQProgramPath "WorkingDirectoryPath" "DatasetName" "DatasetSuffix" "TargetsFileName" "ParamFileName" "WorkingParametersFolderPath" "LockFileName" "ResultsFolderPath" "CoreNumber"
                    '
                    ' It will read and parse the batch file to determine the TargetsFile name and folder path so that it can cache the target code values
                    ' Thus, if you change this code, also update clsGlyQIqRunner

                    swOutFile.Write(clsGlobal.PossiblyQuotePath(progLoc))

                    swOutFile.Write(" " & """" & m_WorkingDir & """")
                    swOutFile.Write(" " & """" & m_DatasetName & """")
                    swOutFile.Write(" " & """" & "raw" & """")

                    Dim targetsFile As FileInfo = Nothing
                    If Not splitTargetFileInfo.TryGetValue(core, targetsFile) Then
                        LogError("Logic error; core " & core & " not found in dictionary splitTargetFileInfo")
                        Return False
                    End If

                    swOutFile.Write(" " & """" & targetsFile.Name & """")

                    swOutFile.Write(" " & """" & mGlyQIQParams.ConsoleOperatingParametersFileName & """")

                    swOutFile.Write(" " & """" & workingDirectory.Value.FullName & """")

                    swOutFile.Write(" " & """" & "Lock_" & core & """")

                    swOutFile.Write(" " & """" & Path.Combine(m_WorkingDir, "Results") & """")

                    swOutFile.Write(" " & """" & core & """")

                    swOutFile.WriteLine()

                End Using

            Next

            Return True

        Catch ex As Exception
            m_message = "Exception in CreateLauncherBatchFiles"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message)
            Return False
        End Try

    End Function

    Private Function CreateSubFolders(ByVal coreCount As Integer) As Dictionary(Of Integer, DirectoryInfo)

        Try
            ' Make sure that required subfolders exist in the working directory

            Dim lstWorkingDirectories = New Dictionary(Of Integer, DirectoryInfo)

            For core = 1 To coreCount
                Dim folderName As String = "WorkingParametersCore" & core

                lstWorkingDirectories.Add(core, New DirectoryInfo(Path.Combine(m_WorkingDir, folderName)))

            Next

            For Each workingDirectory In lstWorkingDirectories

                If Not workingDirectory.Value.Exists Then
                    workingDirectory.Value.Create()
                End If

                Dim diLocksFolder = New DirectoryInfo(Path.Combine(workingDirectory.Value.FullName, LOCKS_FOLDER_NAME))
                If Not diLocksFolder.Exists Then diLocksFolder.Create()
            Next

            Return lstWorkingDirectories

        Catch ex As Exception
            m_message = "Exception in CreateSubFolders"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message)
            Return New Dictionary(Of Integer, DirectoryInfo)
        End Try

    End Function

    Private Function RetrieveGlyQIQParameters(ByVal coreCount As Integer) As Boolean

        Try

            Dim sourceFolderPath As String
            Dim sourceFileName As String

            ' Define the base source folder path
            ' Typically \\gigasax\DMS_Parameter_Files\GlyQ-IQ
            Dim paramFileStoragePathBase = m_jobParams.GetParam("ParmFileStoragePath")

            mGlyQIQParams.IQParamFileName = m_jobParams.GetJobParameter("ParmFileName", "")
            If String.IsNullOrEmpty(mGlyQIQParams.IQParamFileName) Then
                LogError("Job Parameter File name is empty")
                Return False
            End If

            ' Retrieve the GlyQ-IQ parameter file
            ' Typically \\gigasax\DMS_Parameter_Files\GlyQ-IQ\ParameterFiles
            sourceFolderPath = Path.Combine(paramFileStoragePathBase, "ParameterFiles")
            sourceFileName = String.Copy(mGlyQIQParams.IQParamFileName)

            If Not CopyFileToWorkingDirectories(sourceFileName, sourceFolderPath, "IQ Parameter File") Then
                Return False
            End If

            mGlyQIQParams.FactorsName = m_jobParams.GetJobParameter("Factors", String.Empty)
            mGlyQIQParams.TargetsName = m_jobParams.GetJobParameter("Targets", String.Empty)

            ' Make sure factor name and target name do not have an extension
            mGlyQIQParams.FactorsName = Path.GetFileNameWithoutExtension(mGlyQIQParams.FactorsName)
            mGlyQIQParams.TargetsName = Path.GetFileNameWithoutExtension(mGlyQIQParams.TargetsName)

            If String.IsNullOrEmpty(mGlyQIQParams.FactorsName) Then
                LogError("Factors parameter is empty")
                Return False
            End If

            If String.IsNullOrEmpty(mGlyQIQParams.TargetsName) Then
                LogError("Targets parameter is empty")
                Return False
            End If

            ' Retrieve the factors file
            sourceFolderPath = Path.Combine(paramFileStoragePathBase, "Factors")
            sourceFileName = mGlyQIQParams.FactorsName & ".txt"

            If Not CopyFileToWorkingDirectories(sourceFileName, sourceFolderPath, "Factors File") Then
                Return False
            End If

            ' Retrieve the Targets file
            sourceFolderPath = Path.Combine(paramFileStoragePathBase, "Libraries")
            sourceFileName = mGlyQIQParams.TargetsName & ".txt"

            If Not CopyFileToWorkDir(sourceFileName, sourceFolderPath, m_WorkingDir) Then
                m_message &= " (Targets File)"
                Return False
            End If

            ' There is no need to store the targets file in the job result folder
            m_jobParams.AddResultFileToSkip(sourceFileName)

            Dim fiTargetsFile = New FileInfo(Path.Combine(m_WorkingDir, sourceFileName))

            ' Count the number of targets
            mGlyQIQParams.NumTargets = CountTargets(fiTargetsFile.FullName)
            If mGlyQIQParams.NumTargets < 1 Then
                LogError( "Targets file is empty: " & Path.Combine(sourceFolderPath, sourceFileName))
                Return False
            End If

            If mGlyQIQParams.NumTargets < coreCount Then
                For coreToRemove = mGlyQIQParams.NumTargets + 1 To coreCount
                    mGlyQIQParams.WorkingParameterFolders(coreToRemove).Delete(True)
                    mGlyQIQParams.WorkingParameterFolders.Remove(coreToRemove)
                Next

                coreCount = mGlyQIQParams.NumTargets
                m_jobParams.AddAdditionalParameter("GlyQ-IQ", JOB_PARAM_ACTUAL_CORE_COUNT, coreCount.ToString())

            End If

            Dim splitTargetFileInfo As Dictionary(Of Integer, FileInfo)

            If mGlyQIQParams.WorkingParameterFolders.Count = 1 Then
                ' Running on just one core
                fiTargetsFile.MoveTo(Path.Combine(mGlyQIQParams.WorkingParameterFolders.First.Value.FullName, sourceFileName))

                splitTargetFileInfo = New Dictionary(Of Integer, FileInfo)
                splitTargetFileInfo.Add(1, fiTargetsFile)

            Else
                ' Split the targets file based on the number of cores
                splitTargetFileInfo = SplitTargetsFile(fiTargetsFile, mGlyQIQParams.NumTargets)
            End If

            ' Retrieve the alignment parameters
            sourceFolderPath = Path.Combine(paramFileStoragePathBase, "BaseFiles")
            sourceFileName = ALIGNMENT_PARAMETERS_FILENAME

            If Not CopyFileToWorkingDirectories(sourceFileName, sourceFolderPath, "AlignmentParameters File") Then
                Return False
            End If

            ' Retrieve the Executor Parameters
            ' Note that the file paths in this file don't matter, but the file is required, so we retrieve it
            sourceFolderPath = Path.Combine(paramFileStoragePathBase, "BaseFiles")
            sourceFileName = EXECUTOR_PARAMETERS_FILE

            If Not CopyFileToWorkingDirectories(sourceFileName, sourceFolderPath, "Executor File") Then
                Return False
            End If

            ' Create the ConsoleOperating Parameters file
            If Not CreateConsoleOperatingParametersFile() Then
                Return False
            End If

            ' Create the Launcher Batch files (one for each core)
            If Not CreateLauncherBatchFiles(splitTargetFileInfo) Then
                Return False
            End If

            Return True

        Catch ex As Exception
            m_message = "Exception in RetrieveGlyQIQParameters"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message)
            Return False
        End Try

    End Function

    Private Function RetrievePeaksAndRawData() As Boolean

        Try
            Dim rawDataType As String = m_jobParams.GetJobParameter("RawDataType", "")
            Dim eRawDataType = GetRawDataType(rawDataType)

            If eRawDataType = eRawDataTypeConstants.ThermoRawFile Then
                m_jobParams.AddResultFileExtensionToSkip(DOT_RAW_EXTENSION)
            Else
                LogError("GlyQ-IQ presently only supports Thermo .Raw files")
                Return False
            End If

            ' Retrieve the _peaks.txt file
            Dim fileToFind As String
            Dim sourceFolderPath As String = String.Empty

            fileToFind = m_DatasetName & "_peaks.txt"
            If Not FindAndRetrieveMiscFiles(fileToFind, Unzip:=False, SearchArchivedDatasetFolder:=False, sourceFolderPath:=sourceFolderPath) Then
                m_message = "Could not find the _peaks.txt file; this is typically created by the DeconPeakDetector job step; rerun that job step if it has been deleted"
                Return False
            End If
            m_jobParams.AddResultFileToSkip(fileToFind)
            m_jobParams.AddResultFileExtensionToSkip("_peaks.txt")

            Dim diTransferFolder = New DirectoryInfo(m_jobParams.GetParam("transferFolderPath"))
            Dim diSourceFolder = New DirectoryInfo(sourceFolderPath)
            If (diSourceFolder.FullName.ToLower().StartsWith(diTransferFolder.FullName.ToLower())) Then
                ' The Peaks.txt file is in the transfer folder
                ' If the analysis finishes successfully, then we can delete the file from the transfer folder
                m_jobParams.AddServerFileToDelete(Path.Combine(sourceFolderPath, fileToFind))
            End If

            ' Retrieve the instrument data file
            If Not RetrieveSpectra(rawDataType) Then
                If String.IsNullOrEmpty(m_message) Then
                    m_message = "Error retrieving instrument data file"
                End If

                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisResourcesGlyQIQ.GetResources: " & m_message)
                Return False
            End If

            If Not MyBase.ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders) Then
                Return False
            End If

            Return True

        Catch ex As Exception
            m_message = "Exception in RetrievePeaksAndRawData"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message)
            Return False
        End Try

    End Function

    ''' <summary>
    ''' 
    ''' </summary>
    ''' <param name="fiTargetsFile"></param>
    ''' <param name="numTargets"></param>
    ''' <returns>List of FileInfo objects for the newly created target files (key is core number, value is the Targets file path)</returns>
    ''' <remarks></remarks>
    Private Function SplitTargetsFile(ByVal fiTargetsFile As FileInfo, ByVal numTargets As Integer) As Dictionary(Of Integer, FileInfo)

        Try
            Dim lstOutputFiles = New Dictionary(Of Integer, FileInfo)

            Using srReader = New StreamReader(New FileStream(fiTargetsFile.FullName, FileMode.Open, FileAccess.Read, FileShare.Read))
                ' Read the header line
                Dim headerLine = srReader.ReadLine

                ' Create the output files
                Dim lstWriters = New List(Of StreamWriter)
                For Each workingDirectory In mGlyQIQParams.WorkingParameterFolders
                    Dim core = workingDirectory.Key

                    Dim outputFilePath = Path.Combine(workingDirectory.Value.FullName, Path.GetFileNameWithoutExtension(fiTargetsFile.Name) & "_Part" & core.ToString() & ".txt")
                    lstWriters.Add(New StreamWriter(New FileStream(outputFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                    lstOutputFiles.Add(core, New FileInfo(outputFilePath))
                Next

                ' Write the header line ot each writer
                For Each targetFileWriter In lstWriters
                    targetFileWriter.WriteLine(headerLine)
                Next

                ' When targetsWritten reaches nextThreshold, we will switch to the next file
                Dim nextThreshold = CInt(Math.Floor(numTargets / CSng(mGlyQIQParams.WorkingParameterFolders.Count)))
                If nextThreshold < 1 Then nextThreshold = 1

                Dim targetsWritten = 0
                Dim outputFileIndex = 0
                Dim outputFileIndexMax = mGlyQIQParams.WorkingParameterFolders.Count - 1

                ' Read the targets
                While Not srReader.EndOfStream
                    Dim lineIn = srReader.ReadLine

                    If outputFileIndex > outputFileIndexMax Then
                        ' This shouldn't happen, but double checking to be sure
                        outputFileIndex = outputFileIndexMax
                    End If

                    lstWriters(outputFileIndex).WriteLine(lineIn)

                    targetsWritten += 1
                    If targetsWritten >= nextThreshold Then
                        ' Advance the output file index
                        outputFileIndex += 1

                        Dim newThreshold = CInt(Math.Floor(numTargets / CSng(mGlyQIQParams.WorkingParameterFolders.Count) * (outputFileIndex + 1)))
                        If newThreshold > nextThreshold Then
                            nextThreshold = newThreshold
                        Else
                            nextThreshold += 1
                        End If
                    End If
                End While

                For Each targetFileWriter In lstWriters
                    targetFileWriter.Close()
                Next
            End Using

            Return lstOutputFiles

        Catch ex As Exception
            m_message = "Exception in SplitTargetsFile"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message)
            Return New Dictionary(Of Integer, FileInfo)
        End Try

    End Function

End Class
