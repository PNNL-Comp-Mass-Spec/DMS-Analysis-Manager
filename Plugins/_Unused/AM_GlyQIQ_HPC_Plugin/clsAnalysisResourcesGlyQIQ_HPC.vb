' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Created 05/29/2014
'
'*********************************************************************************************************

Option Strict On

Imports AnalysisManagerBase
Imports System.IO

Public Class clsAnalysisResourcesGlyQIQ_HPC
    Inherits clsAnalysisResources

    Public Const GLYQ_IQ_LAUNCHER_FILE_PARAM_NAME As String = "GlyQIQLauncherFilePath"

    Protected Const WORKING_PARAMETERS_FOLDER_NAME As String = "WorkingParameters"
    Protected Const EXECUTOR_PARAMETERS_FILE As String = "ExecutorParametersSK.xml"

    Protected Const DISABLE_HPC_JOB_SUBMISSION As Boolean = True

    Protected Structure udtGlyQIQParams
        Public ApplicationsFolderPath As String
        Public DatasetNameTruncated As String
        Public WorkingParametersFolderPathLocal As String
        Public WorkingParametersFolderPathRemote As String
        Public WorkingDirectoryRemote As String
        Public FactorsName As String
        Public TargetsName As String
        Public NumTargets As Integer
        Public TimeStamp As String
        Public ConsoleOperatingParametersFileName As String
        Public OperationParametersFileName As String
        Public IQParamFileName As String
        Public CoreCountActual As Integer
    End Structure

#Region "Classwide variables"
    Private mGlyQIQParams As udtGlyQIQParams
#End Region

    Public Overrides Function GetResources() As IJobParams.CloseOutType

        mGlyQIQParams = New udtGlyQIQParams()

        If Not CreateSubFolders() Then
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        If Not RetrieveGlyQIQParameters() Then
            Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
        End If

        If Not RetrievePeaksAndRawData() Then
            Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
        End If

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    Private Function CreateSubFolders() As Boolean

        Try
            ' Make sure that required subfolders exist

            Dim lstSubFolders = New List(Of String)
            lstSubFolders.Add("RawData")
            lstSubFolders.Add("Results")
            lstSubFolders.Add("ResultsSummary")
            lstSubFolders.Add(WORKING_PARAMETERS_FOLDER_NAME)

            For Each folderName In lstSubFolders
                Dim diTargetFolder = New DirectoryInfo(Path.Combine(m_WorkingDir, folderName))
                If Not diTargetFolder.Exists Then diTargetFolder.Create()
            Next

            Return True

        Catch ex As Exception
            m_message = "Exception in CreateSubFolders"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message)
            Return False
        End Try

    End Function

    Private Function CountTargets(ByVal targetsFilePath As String) As Integer
        Try
            ' numTargets is initialized to -1 because we don't want to count the header line
            Dim numTargets As Integer = -1

            Using srInFile = New StreamReader(New FileStream(targetsFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))

                While srInFile.Peek() > -1
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

            ' Define the output file path
            mGlyQIQParams.ConsoleOperatingParametersFileName = "GlyQIQ_Params_" & mGlyQIQParams.DatasetNameTruncated & ".txt"

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Creating the Operating Parameters file, " & mGlyQIQParams.ConsoleOperatingParametersFileName)

            Dim outputFilePath = Path.Combine(mGlyQIQParams.WorkingParametersFolderPathLocal, mGlyQIQParams.ConsoleOperatingParametersFileName)

            Using swOutFile = New StreamWriter(New FileStream(outputFilePath, FileMode.Create, FileAccess.Write, FileShare.Read))
                swOutFile.WriteLine("ResultsFolderPath" & "," & Path.Combine(mGlyQIQParams.WorkingDirectoryRemote, "Results"))
                swOutFile.WriteLine("LoggingFolderPath" & "," & Path.Combine(mGlyQIQParams.WorkingDirectoryRemote, "Results"))
                swOutFile.WriteLine("FactorsFile" & "," & mGlyQIQParams.FactorsName & ".txt")
                swOutFile.WriteLine("ExecutorParameterFile" & "," & EXECUTOR_PARAMETERS_FILE)
                swOutFile.WriteLine("XYDataFolder" & "," & "XYDataWriter")
                swOutFile.WriteLine("WorkflowParametersFile" & "," & mGlyQIQParams.IQParamFileName)
                swOutFile.WriteLine("Allignment" & "," & Path.Combine(mGlyQIQParams.WorkingDirectoryRemote, WORKING_PARAMETERS_FOLDER_NAME, "AlignmentParameters.xml"))

                ' The following file doesn't have to exist
                swOutFile.WriteLine("BasicTargetedParameters" & "," & Path.Combine(mGlyQIQParams.WorkingDirectoryRemote, WORKING_PARAMETERS_FOLDER_NAME, "BasicTargetedWorkflowParameters.xml"))

            End Using

            Return True

        Catch ex As Exception
            m_message = "Exception in CreateConsoleOperatingParametersFile"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message)
            Return False
        End Try

    End Function

    Private Function CreateCleanupBatchFiles() As Boolean

        Try

            ' Warning: The dall to DeleteViaHPCList.exe or HPC_DeleteCloud might delete the ApplicationFiles folder, so these are currently commented out

            Const NODE_GROUP As String = "PrePost"
            Const NUM_CORES As Integer = 16

            ' Example path: \\winhpcfs\Projects\DMS\DMS_Work_Dir\Pub-61-3\0x_Launch_DatasetName_201405291749.bat
            Dim outputFilePath As String = Path.Combine(m_WorkingDir, "1x_FrankenDelete_" & mGlyQIQParams.DatasetNameTruncated & ".bat")
            m_jobParams.AddResultFileToSkip(Path.GetFileName(outputFilePath))

            Using swOutFile = New StreamWriter(New FileStream(outputFilePath, FileMode.Create, FileAccess.Write, FileShare.Read))
                swOutFile.WriteLine(
                  "rem Commented out since can be skipped: " &
                  Path.Combine(mGlyQIQParams.ApplicationsFolderPath, "GlyQ-IQ_HPC_DeleteFilesList\Release\DeleteViaHPCList.exe") & " " &
                  Path.Combine(mGlyQIQParams.WorkingDirectoryRemote, mGlyQIQParams.OperationParametersFileName))

                swOutFile.WriteLine(
                  "rem Commented out since can be skipped: " &
                  Path.Combine(mGlyQIQParams.ApplicationsFolderPath, "GlyQ-IQ_HPC_DeleteCloud\Release\HPC_DeleteCloud.exe") & " " &
                  mGlyQIQParams.WorkingDirectoryRemote & " " & NODE_GROUP & " " & NUM_CORES)
            End Using

            ' Example path: \\winhpcfs\Projects\DMS\DMS_Work_Dir\Pub-61-3\0x_Launch_DatasetName_201405291749.bat
            outputFilePath = Path.Combine(m_WorkingDir, "2x_DeleteResultsFolder_" & mGlyQIQParams.DatasetNameTruncated & ".bat")
            m_jobParams.AddResultFileToSkip(Path.GetFileName(outputFilePath))

            Using swOutFile = New StreamWriter(New FileStream(outputFilePath, FileMode.Create, FileAccess.Write, FileShare.Read))
                swOutFile.WriteLine(
                  "rem Commented out for safety: " &
                  "rmdir " & Path.Combine(mGlyQIQParams.WorkingDirectoryRemote, "Results") & " /S /Q")
            End Using

            Return True

        Catch ex As Exception
            m_message = "Exception in CreateCleanupBatchFiles"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message)
            Return False
        End Try

    End Function

    Private Function CreateLauncherBatchFile() As Boolean

        Try
            If String.IsNullOrEmpty(mGlyQIQParams.TimeStamp) Then
                m_message = "Logic error, call CreateOperationsParametersFile before calling CreateLauncherBatchFile"
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
                Return False
            End If

            Dim launcherFileName = "0x_Launch_" & mGlyQIQParams.DatasetNameTruncated & "_" & mGlyQIQParams.TimeStamp & ".bat"
            Dim outputFilePath As String = Path.Combine(m_WorkingDir, launcherFileName)

            Dim commentText As String = String.Empty
            If DISABLE_HPC_JOB_SUBMISSION Then
                commentText = "rem COMMENTED OUT FOR DEBUGGING PURPOSES: "
            End If

            Using swOutFile = New StreamWriter(New FileStream(outputFilePath, FileMode.Create, FileAccess.Write, FileShare.Read))
                swOutFile.WriteLine("@echo off")
                swOutFile.WriteLine("rem Customize this work directory")
                swOutFile.WriteLine("set WorkDir=" & mGlyQIQParams.WorkingDirectoryRemote)
                swOutFile.WriteLine()
                swOutFile.WriteLine("echo Deleting existing results")
                swOutFile.WriteLine()
                swOutFile.WriteLine("del Results\*.* /s /q")
                swOutFile.WriteLine()
                swOutFile.WriteLine("echo Auto create parameter files using the OperationParameters file")
                swOutFile.WriteLine(Path.Combine(mGlyQIQParams.ApplicationsFolderPath, "GlyQ-IQ_WriteHPCFiles\Release\WriteHPCPrepFiles.exe") & " %WorkDir%\" & mGlyQIQParams.OperationParametersFileName)
                swOutFile.WriteLine()
                swOutFile.WriteLine("if exist %WorkDir%\HPC_Stopwatch_" & mGlyQIQParams.DatasetNameTruncated & ".txt del %WorkDir%\HPC_Stopwatch_" & mGlyQIQParams.DatasetNameTruncated & ".txt /q")
                swOutFile.WriteLine()
                swOutFile.WriteLine("echo Dividing up the targets into " & mGlyQIQParams.CoreCountActual & " groups")
                swOutFile.WriteLine("call %WorkDir%\2_HPC_DivideTargets.bat %WorkDir%")
                swOutFile.WriteLine()
                swOutFile.WriteLine("echo Auto creating more parameter files using the OperationParameters file")
                swOutFile.WriteLine(Path.Combine(mGlyQIQParams.ApplicationsFolderPath, "GlyQ-IQ_WriteHPCFiles\Release\WriteHPCPrepFiles.exe") & " %WorkDir%\" & mGlyQIQParams.OperationParametersFileName)
                swOutFile.WriteLine()
                swOutFile.WriteLine("echo Spawning GlyQ-IQ analysis tasks")
                swOutFile.WriteLine("call %WorkDir%\HPC_ScottsFirstHPLCLauncher_" & mGlyQIQParams.DatasetNameTruncated & ".bat")
                swOutFile.WriteLine()
                swOutFile.WriteLine("echo Starting timer that watches for results")
                swOutFile.WriteLine("echo   Note that ScottsFirstHPLCLauncher.exe should have already called 1_HPC_StartCollectResults_SPIN_SN138_16Dec13_40uL.bat")
                swOutFile.WriteLine("echo   so this call may not be necessary")
                swOutFile.WriteLine(commentText & "%WorkDir%\1_HPC_StartCollectResults_" & mGlyQIQParams.DatasetNameTruncated & ".bat %WorkDir%")
                swOutFile.WriteLine()
                swOutFile.WriteLine("echo Done Launching analysis; Result Collector will display progress messages here")
            End Using

            ' Construct and store the remote launcher file path
            ' Example path: \\winhpcfs\Projects\DMS\DMS_Work_Dir\Pub-61-3\0x_Launch_DatasetName_201405291749.bat

            Dim remoteBatchFilePath = Path.Combine(mGlyQIQParams.WorkingDirectoryRemote, launcherFileName)
            m_jobParams.AddAdditionalParameter("StepParameters", GLYQ_IQ_LAUNCHER_FILE_PARAM_NAME, remoteBatchFilePath)

            Return True

        Catch ex As Exception
            m_message = "Exception in CreateLauncherBatchFile"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message)
            Return False
        End Try

    End Function

    Private Function CreateLockFiles() As Boolean

        Try
            Dim diTargetFolder = New DirectoryInfo(Path.Combine(m_WorkingDir, WORKING_PARAMETERS_FOLDER_NAME, "LocksFolder"))
            If Not diTargetFolder.Exists Then diTargetFolder.Create()

            ' Yes, we're creating one extra lock file here (not sure if it's really needed)
            For i As Integer = 0 To mGlyQIQParams.CoreCountActual
                Dim fiTargetFile = New FileInfo(Path.Combine(diTargetFolder.FullName, "Lock_" & i & ".txt"))
                Using fiTargetFile.CreateText()
                    ' Just create the file; leave it blank
                End Using
            Next

            Return True
        Catch ex As Exception
            m_message = "Exception in CreateLockFiles"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message)
            Return False
        End Try

    End Function

    Private Function CreateOperationsParametersFile(ByVal udtHpcOptions As udtHPCOptionsType) As Boolean

        Try

            Dim datasetNameTruncated = TruncateDatasetNameIfRequired(m_DatasetName)

            mGlyQIQParams.TimeStamp = DateTime.Now.ToString("yyyyMMddhhmm")
            mGlyQIQParams.OperationParametersFileName = "0y_HPC_OperationParameters_" & mGlyQIQParams.TimeStamp & ".txt"

            ' Example path: \\winhpcfs\Projects\DMS\DMS_Work_Dir\Pub-61-3\0y_HPC_OperationParameters_201405291749.bat
            Dim operationParametersFilePath As String = Path.Combine(m_WorkingDir, mGlyQIQParams.OperationParametersFileName)

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Creating the Operation Parameters file, " & mGlyQIQParams.OperationParametersFileName)

            mGlyQIQParams.CoreCountActual = m_jobParams.GetJobParameter("HPCMaxCores", 500)

            If mGlyQIQParams.CoreCountActual > mGlyQIQParams.NumTargets Then
                mGlyQIQParams.CoreCountActual = mGlyQIQParams.NumTargets
            End If

            Dim sPICHPCUsername = m_mgrParams.GetParam("PICHPCUser", "")
            Dim sPICHPCPassword = m_mgrParams.GetParam("PICHPCPassword", "")

            Using swOutFile = New StreamWriter(New FileStream(operationParametersFilePath, FileMode.Create, FileAccess.Write, FileShare.Read))

                swOutFile.WriteLine("Cores," & mGlyQIQParams.CoreCountActual)

                Dim diMgrWorkingDirectoryRemote = New DirectoryInfo(mGlyQIQParams.WorkingDirectoryRemote)

                ' This working directory is the parent of the manager working directory
                swOutFile.WriteLine("WorkingDirectory," & diMgrWorkingDirectoryRemote.Parent.FullName)

                swOutFile.WriteLine("WorkingFolder," & diMgrWorkingDirectoryRemote.Name)
                swOutFile.WriteLine("DataSetDirectory," & Path.Combine(mGlyQIQParams.WorkingDirectoryRemote, "RawData"))

                ' Note that datasetNameTruncated will be, at most, 25 characters long
                swOutFile.WriteLine("DatasetFileName," & datasetNameTruncated)
                swOutFile.WriteLine("WorkingDirectoryForExe," & mGlyQIQParams.ApplicationsFolderPath)

                ' Targets name here should not end in .txt
                swOutFile.WriteLine("Targets," & mGlyQIQParams.TargetsName)

                ' Factors name here needs to end in .txt
                swOutFile.WriteLine("FactorsName," & mGlyQIQParams.FactorsName & ".txt")
                swOutFile.WriteLine("ExecutorParameterFile," & EXECUTOR_PARAMETERS_FILE)
                swOutFile.WriteLine("IQParameterFile," & mGlyQIQParams.IQParamFileName)
                swOutFile.WriteLine("MakeResultsListName,HPC_MakeResultsList_Asterisks")
                swOutFile.WriteLine("DivideTargetsParameterFile,HPC-Parameters_DivideTargetsPIC_Asterisks.txt")

                If String.IsNullOrEmpty(mGlyQIQParams.ConsoleOperatingParametersFileName) Then
                    m_message = "Logic error; call CreateConsoleOperatingParametersFile before calling this function"
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
                    Return False
                End If

                swOutFile.WriteLine("ConsoleOperatingParameters," & mGlyQIQParams.ConsoleOperatingParametersFileName)

                swOutFile.WriteLine("ScottsFirstHPLCLauncher,HPC_ScottsFirstHPLCLauncher")
                swOutFile.WriteLine("HPC_MultiSleepParameterFileGlobal_Root,HPC_MultiSleepParameterFileGlobal")

                swOutFile.WriteLine("ipaddress," & diMgrWorkingDirectoryRemote.FullName)
                swOutFile.WriteLine("LogIpAddress," & diMgrWorkingDirectoryRemote.FullName)
                swOutFile.WriteLine("HPCExeLaunchDirectory," & mGlyQIQParams.ApplicationsFolderPath)

                swOutFile.WriteLine("HPCClusterGroupName-ComputeNodes-AzureNodes-@PNNL-Kronies,ComputeNodes")
                swOutFile.WriteLine("isThisToBeRunOnHPC,true")
                swOutFile.WriteLine("DataSetFileExtension,raw")
                swOutFile.WriteLine("FrankenDelete,true")

                swOutFile.WriteLine("ClusterName," & udtHpcOptions.HeadNode)
                swOutFile.WriteLine("TemplateName,GlyQIQ")

                swOutFile.WriteLine("MaxTargetNumber," & mGlyQIQParams.NumTargets)

                swOutFile.WriteLine("PICHPCUsername," & sPICHPCUsername)
                swOutFile.WriteLine("PICHPCPassword," & sPICHPCPassword)
            End Using

            Return True

        Catch ex As Exception
            m_message = "Exception in CreateOperationsParametersFile"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message)
            Return False
        End Try

    End Function

    Public Shared Function GetGlyQIQAppFilesPath(ByVal udtHpcOptions As udtHPCOptionsType) As String
        Return Path.Combine(udtHpcOptions.SharePath, "GlyQ_ApplicationFiles")
    End Function

    Private Function RetrieveGlyQIQParameters() As Boolean

        Try

            Dim sourceFolderPath As String
            Dim sourceFileName As String

            ' Make sure the dataset name is, at most, 25 characters
            mGlyQIQParams.DatasetNameTruncated = TruncateDatasetNameIfRequired(m_DatasetName)

            ' Lookup the HPC options
            Dim udtHPCOptions As udtHPCOptionsType = GetHPCOptions(m_jobParams, m_MgrName)
            mGlyQIQParams.WorkingDirectoryRemote = String.Copy(udtHPCOptions.WorkDirPath)

            mGlyQIQParams.WorkingParametersFolderPathLocal = Path.Combine(m_WorkingDir, WORKING_PARAMETERS_FOLDER_NAME)
            mGlyQIQParams.WorkingParametersFolderPathRemote = Path.Combine(udtHPCOptions.WorkDirPath, WORKING_PARAMETERS_FOLDER_NAME)

            mGlyQIQParams.ApplicationsFolderPath = GetGlyQIQAppFilesPath(udtHPCOptions)

            ' Define the base source folder path
            ' Typically \\gigasax\DMS_Parameter_Files\GlyQ-IQ
            Dim paramFileStoragePathBase = m_jobParams.GetParam("ParmFileStoragePath")

            mGlyQIQParams.IQParamFileName = m_jobParams.GetJobParameter("ParmFileName", "")
            If String.IsNullOrEmpty(mGlyQIQParams.IQParamFileName) Then
                m_message = "Job Parameter File name is empty"
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
                Return False
            End If

            ' Retrieve the parameter file
            ' Typically \\gigasax\DMS_Parameter_Files\GlyQ-IQ\ParameterFiles
            sourceFolderPath = Path.Combine(paramFileStoragePathBase, "ParameterFiles")
            sourceFileName = String.Copy(mGlyQIQParams.IQParamFileName)

            If Not CopyFileToWorkDir(sourceFileName, sourceFolderPath, mGlyQIQParams.WorkingParametersFolderPathLocal) Then
                m_message &= " (IQ Parameter File)"
                Return False
            End If

            mGlyQIQParams.FactorsName = m_jobParams.GetJobParameter("Factors", String.Empty)
            mGlyQIQParams.TargetsName = m_jobParams.GetJobParameter("Targets", String.Empty)

            ' Make sure factor name and target name do not have an extension
            mGlyQIQParams.FactorsName = Path.GetFileNameWithoutExtension(mGlyQIQParams.FactorsName)
            mGlyQIQParams.TargetsName = Path.GetFileNameWithoutExtension(mGlyQIQParams.TargetsName)

            If String.IsNullOrEmpty(mGlyQIQParams.FactorsName) Then
                m_message = "Factors parameter is empty"
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
                Return False
            End If

            If String.IsNullOrEmpty(mGlyQIQParams.TargetsName) Then
                m_message = "Targets parameter is empty"
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
                Return False
            End If

            ' Retrieve the factors file
            sourceFolderPath = Path.Combine(paramFileStoragePathBase, "Factors")
            sourceFileName = mGlyQIQParams.FactorsName & ".txt"

            If Not CopyFileToWorkDir(sourceFileName, sourceFolderPath, mGlyQIQParams.WorkingParametersFolderPathLocal) Then
                m_message &= " (Factors File)"
                Return False
            End If

            ' Retrieve the Targets file
            sourceFolderPath = Path.Combine(paramFileStoragePathBase, "Libraries")
            sourceFileName = mGlyQIQParams.TargetsName & ".txt"

            If Not CopyFileToWorkDir(sourceFileName, sourceFolderPath, mGlyQIQParams.WorkingParametersFolderPathLocal) Then
                m_message &= " (Targets File)"
                Return False
            End If

            ' Count the number of targets
            mGlyQIQParams.NumTargets = CountTargets(Path.Combine(mGlyQIQParams.WorkingParametersFolderPathLocal, sourceFileName))
            If mGlyQIQParams.NumTargets < 1 Then
                m_message = "Targets file is empty: " & Path.Combine(sourceFolderPath, sourceFileName)
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
                Return False
            End If

            ' Retrieve the alignment parameters
            sourceFolderPath = Path.Combine(paramFileStoragePathBase, "BaseFiles")
            sourceFileName = "AlignmentParameters.xml"

            If Not CopyFileToWorkDir(sourceFileName, sourceFolderPath, mGlyQIQParams.WorkingParametersFolderPathLocal) Then
                m_message &= " (AlignmentParameters File)"
                Return False
            End If

            ' Retrieve the Executor Parameters
            ' Note that the file paths in this file don't matter, but the file is likely required, so we retrieve it
            sourceFolderPath = Path.Combine(paramFileStoragePathBase, "BaseFiles")
            sourceFileName = EXECUTOR_PARAMETERS_FILE

            If Not CopyFileToWorkDir(sourceFileName, sourceFolderPath, mGlyQIQParams.WorkingParametersFolderPathLocal) Then
                Return False
            End If

            ' Create the ConsoleOperating Parameters file
            If Not CreateConsoleOperatingParametersFile() Then
                Return False
            End If

            ' Create the OperationsParameter file
            If Not CreateOperationsParametersFile(udtHPCOptions) Then
                Return False
            End If

            ' Create the locks folder and lock files (not sure if these are actually used)
            CreateLockFiles()

            ' Create the Launcher batch file
            If Not CreateLauncherBatchFile() Then
                Return False
            End If

            ' Create the cleanup batch files
            If Not CreateCleanupBatchFiles() Then
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
                m_message = "GlyQ-IQ presently only supports Thermo .Raw files"
                Return False
            End If

            ' Retrieve the _peaks.txt file
            Dim fileToFind As String
            Dim sourceFolderPath As String = String.Empty

            fileToFind = m_DatasetName & "_peaks.txt"
            If Not FindAndRetrieveMiscFiles(fileToFind, Unzip:=False, SearchArchivedDatasetFolder:=False, sourceFolderPath:=sourceFolderPath) Then
                'Errors were reported in function call, so just return
                Return False
            End If
            m_jobParams.AddResultFileToSkip(fileToFind)
            m_jobParams.AddResultFileExtensionToSkip("_peaks.txt")

            Dim diTransferFolder = New DirectoryInfo(m_jobParams.GetParam("transferFolderPath"))
            Dim diSourceFolder = New DirectoryInfo(sourceFolderPath)
            If String.Compare(diTransferFolder.FullName, diSourceFolder.FullName, True) = 0 Then
                ' The Peaks.txt file is in the transfer folder
                ' If the analysis finishes successfully, then we can delete the file from the transfer folder
                m_jobParams.AddServerFileToDelete(Path.Combine(sourceFolderPath, fileToFind))
            End If

            ' Retrieve the instrument data file
            If Not RetrieveSpectra(rawDataType) Then
                If String.IsNullOrEmpty(m_message) Then
                    m_message = "Error retrieving instrument data file"
                End If

                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisResourcesGlyQIQ_HPC.GetResources: " & m_message)
                Return False
            End If

            If Not MyBase.ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders) Then
                Return False
            End If

            Threading.Thread.Sleep(500)

            ' Move the instrument data files into the RawData subfolder
            ' We will rename the files at this time to match the truncated dataset name
            Dim strTargetFolderPath = Path.Combine(m_WorkingDir, "RawData")

            Dim fiSourceFile As FileInfo
            Dim targetFilePath As String

            fiSourceFile = New FileInfo(Path.Combine(m_WorkingDir, m_DatasetName & DOT_RAW_EXTENSION))
            If Not fiSourceFile.Exists Then
                m_message = "Thermo raw file not found: " & fiSourceFile.Name
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
                Return False
            End If
            targetFilePath = Path.Combine(strTargetFolderPath, mGlyQIQParams.DatasetNameTruncated) & fiSourceFile.Extension
            fiSourceFile.MoveTo(targetFilePath)

            fiSourceFile = New FileInfo(Path.Combine(m_WorkingDir, m_DatasetName & "_peaks.txt"))
            If Not fiSourceFile.Exists Then
                m_message = "Peaks file not found: " & fiSourceFile.Name
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
                Return False
            End If
            targetFilePath = Path.Combine(strTargetFolderPath, mGlyQIQParams.DatasetNameTruncated) & "_peaks.txt"
            fiSourceFile.MoveTo(targetFilePath)

            Return True

        Catch ex As Exception
            m_message = "Exception in RetrievePeaksAndRawData"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message)
            Return False
        End Try

    End Function

    Public Shared Function TruncateDatasetNameIfRequired(ByVal datasetName As String) As String

        Dim strDatasetNameTruncated = String.Copy(datasetName)

        If strDatasetNameTruncated.Length > 25 Then
            ' Shorten the dataset name to prevent paths from getting too long
            strDatasetNameTruncated = strDatasetNameTruncated.Substring(0, 25)

            ' Possibly shorten a bit more if an underscore or dash is present between char index 15 and 25
            ' (we do this so that the truncated dataset name doesn't truncate in the middle of a word)

            Dim chSepChars = New Char() {"_"c, "-"c}

            Dim charIndex = strDatasetNameTruncated.LastIndexOfAny(chSepChars)

            If charIndex >= 15 Then
                strDatasetNameTruncated = strDatasetNameTruncated.Substring(0, charIndex)
            End If

        End If

        Return strDatasetNameTruncated

    End Function

End Class
