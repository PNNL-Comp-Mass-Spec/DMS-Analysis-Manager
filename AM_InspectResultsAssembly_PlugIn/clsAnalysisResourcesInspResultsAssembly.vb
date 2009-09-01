' Last modified 06/15/2009 JDS - Added logging using log4net
Imports AnalysisManagerBase
Imports System.IO
Imports System
Imports PRISM.Files.clsFileTools

Public Class clsAnalysisResourcesInspResultsAssembly
    Inherits clsAnalysisResources


#Region "Methods"
    ''' <summary>
    ''' Retrieves files necessary for performance of Sequest analysis
    ''' </summary>
    ''' <returns>IJobParams.CloseOutType indicating success or failure</returns>
    ''' <remarks></remarks>
    Public Overrides Function GetResources() As AnalysisManagerBase.IJobParams.CloseOutType
        Dim numClonedSteps As String
        Dim WorkingDir As String = m_mgrParams.GetParam("workdir")
        Dim DatasetName As String = m_jobParams.GetParam("datasetNum")
        Dim transferFolderName As String = Path.Combine(m_jobParams.GetParam("transferFolderPath"), DatasetName)
        Dim zippedResultName As String = DatasetName & "_inspect.zip"
        Dim searchLogResultName As String = "InspectSearchLog.txt"

        transferFolderName = Path.Combine(transferFolderName, m_jobParams.GetParam("OutputFolderName"))

        'Clear out list of files to delete or keep when packaging the results
        clsGlobal.ResetFilesToDeleteOrKeep()

        'Retrieve Fasta file
        If Not RetrieveOrgDB(m_mgrParams.GetParam("orgdbdir")) Then Return IJobParams.CloseOutType.CLOSEOUT_FAILED

        'Retrieve param file
        If Not RetrieveGeneratedParamFile( _
         m_jobParams.GetParam("ParmFileName"), _
         m_jobParams.GetParam("ParmFileStoragePath"), _
         WorkingDir) _
        Then Return IJobParams.CloseOutType.CLOSEOUT_FAILED

        ' Retrieve the Inspect Input Params file
        If Not RetrieveFile(clsAnalysisToolRunnerInspResultsAssembly.INSPECT_INPUT_PARAMS_FILENAME, transferFolderName, WorkingDir) Then
            'Errors were reported in function call, so just return
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        numClonedSteps = m_jobParams.GetParam("NumberOfClonedSteps")
        If [String].IsNullOrEmpty(numClonedSteps) Then
            ' Retrieve the zipped Inspect result file
            If Not RetrieveFile(zippedResultName, transferFolderName, WorkingDir) Then
                If m_DebugLevel >= 3 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "RetrieveFile returned False for " & zippedResultName & " using folder " & transferFolderName)
                End If
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            'Unzip Inspect result file
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Unzipping Inspect result file")
            If UnzipFileStart(Path.Combine(WorkingDir, zippedResultName), WorkingDir, "clsAnalysisResourcesInspResultsAssembly.GetResources", False) Then
                If m_DebugLevel >= 1 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Inspect result file unzipped")
                End If
            End If

            ' Rtrieve the Inspect search log file
            If Not RetrieveFile(searchLogResultName, transferFolderName, WorkingDir) Then
                If m_DebugLevel >= 3 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "RetrieveFile returned False for " & searchLogResultName & " using folder " & transferFolderName)
                End If
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If
            clsGlobal.m_FilesToDeleteExt.Add(searchLogResultName)
            clsGlobal.m_FilesToDeleteExt.Add("_inspect.zip")
        Else
            'Retrieve multi inspect result files
            If Not RetrieveMultiInspectResultFiles() Then
                'Errors were reported in function call, so just return
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If
        End If

        'Add all the extensions of the files to delete after run
        clsGlobal.m_FilesToDeleteExt.Add("_inspect.txt")
        clsGlobal.m_FilesToDeleteExt.Add("_error.txt")
        clsGlobal.m_FilesToDeleteExt.Add("_.txt")

        Dim ext As String
        Dim DumFiles() As String

        'update list of files to be deleted after run
        For Each ext In clsGlobal.m_FilesToDeleteExt
            DumFiles = Directory.GetFiles(WorkingDir, "*" & ext) 'Zipped DTA
            For Each FileToDel As String In DumFiles
                clsGlobal.FilesToDelete.Add(FileToDel)
            Next
        Next

        'All finished
        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    ''' <summary>
    ''' Retrieves inspect and inspect log and error files
    ''' </summary>
    ''' <returns>TRUE for success, FALSE for error</returns>
    ''' <remarks></remarks>
    Public Overridable Function RetrieveMultiInspectResultFiles() As Boolean

        'Retrieve zipped DTA file
        Dim InspectResultsFile As String
        Dim ErrorFilename As String
        Dim InspectSearchLogFile As String
        Dim numOfResultFiles As Integer
        Dim fileNum As Integer
        Dim DatasetName As String = m_jobParams.GetParam("datasetNum")
        Dim WorkingDir As String = m_mgrParams.GetParam("workdir")
        Dim transferFolderName As String = Path.Combine(m_jobParams.GetParam("transferFolderPath"), DatasetName)
        Dim dtaFilename As String
        Dim intFileCopyCount As Integer = 0

        transferFolderName = Path.Combine(transferFolderName, m_jobParams.GetParam("OutputFolderName"))

        Try
            numOfResultFiles = CInt(m_jobParams.GetParam("NumberOfClonedSteps"))
        Catch ex As Exception
            numOfResultFiles = 0
        End Try

        If numOfResultFiles < 1 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Job parameter 'NumberOfClonedSteps' is empty or 0; unable to continue")
            Return False
        End If

        For fileNum = 1 To numOfResultFiles
            'Copy each Inspect result file from the transfer directory
            InspectResultsFile = DatasetName & "_" & fileNum & "_inspect.txt"
            dtaFilename = DatasetName & "_" & fileNum & "_dta.txt"
            If File.Exists(Path.Combine(transferFolderName, InspectResultsFile)) Then
                If Not CopyFileToWorkDir(InspectResultsFile, transferFolderName, WorkingDir) Then
                    ' Error copying file (error will have already been logged)
                    If m_DebugLevel >= 3 Then
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "CopyFileToWorkDir returned False for " & InspectResultsFile & " using folder " & transferFolderName)
                    End If
                    Return False
                End If
                intFileCopyCount += 1
                clsGlobal.m_ServerFilesToDelete.Add(Path.Combine(transferFolderName, InspectResultsFile))
                'Also add dta file to list to remove from server
                clsGlobal.m_ServerFilesToDelete.Add(Path.Combine(transferFolderName, dtaFilename))
            End If

            'Copy the Inspect error file from the transfer directory
            ErrorFilename = DatasetName & "_" & fileNum & "_error.txt"
            If File.Exists(Path.Combine(transferFolderName, ErrorFilename)) Then
                If Not CopyFileToWorkDir(ErrorFilename, transferFolderName, WorkingDir) Then
                    ' Error copying file (error will have already been logged)
                    If m_DebugLevel >= 3 Then
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "CopyFileToWorkDir returned False for " & ErrorFilename & " using folder " & transferFolderName)
                    End If
                    Return False
                End If
                intFileCopyCount += 1
                clsGlobal.m_ServerFilesToDelete.Add(Path.Combine(transferFolderName, ErrorFilename))
            End If

            'Copy each Inspect search log file from the transfer directory
            InspectSearchLogFile = "InspectSearchLog_" & fileNum & ".txt"
            If File.Exists(Path.Combine(transferFolderName, InspectSearchLogFile)) Then
                If Not CopyFileToWorkDir(InspectSearchLogFile, transferFolderName, WorkingDir) Then
                    ' Error copying file (error will have already been logged)
                    If m_DebugLevel >= 3 Then
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "CopyFileToWorkDir returned False for " & InspectSearchLogFile & " using folder " & transferFolderName)
                    End If
                    Return False
                End If
                intFileCopyCount += 1
                clsGlobal.m_ServerFilesToDelete.Add(Path.Combine(transferFolderName, InspectSearchLogFile))
            End If
        Next

        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Multi Inspect Result Files copied to local working directory; copied " & intFileCopyCount & " files")

        Return True

    End Function

#End Region

End Class
