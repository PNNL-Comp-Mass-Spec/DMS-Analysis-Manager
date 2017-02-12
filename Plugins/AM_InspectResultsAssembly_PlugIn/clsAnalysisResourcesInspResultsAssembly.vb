Option Strict On

Imports AnalysisManagerBase
Imports System.IO

Public Class clsAnalysisResourcesInspResultsAssembly
    Inherits clsAnalysisResources

#Region "Methods"

    Public Overrides Sub Setup(mgrParams As IMgrParams, jobParams As IJobParams, statusTools As IStatusFile, myEMSLUtilities As clsMyEMSLUtilities)
        MyBase.Setup(mgrParams, jobParams, statusTools, myEmslUtilities)
        SetOption(clsGlobal.eAnalysisResourceOptions.OrgDbRequired, True)
    End Sub

    ''' <summary>
    ''' Retrieves files necessary for performance of Sequest analysis
    ''' </summary>
    ''' <returns>CloseOutType indicating success or failure</returns>
    ''' <remarks></remarks>
    Public Overrides Function GetResources() As CloseOutType

        ' Retrieve shared resources, including the JobParameters file from the previous job step
        Dim result = GetSharedResources()
        If result <> CloseOutType.CLOSEOUT_SUCCESS Then
            Return result
        End If

        Dim numClonedSteps As String

        Dim transferFolderName As String = Path.Combine(m_jobParams.GetParam("transferFolderPath"), DatasetName)
        Dim zippedResultName As String = DatasetName & "_inspect.zip"
        Const searchLogResultName = "InspectSearchLog.txt"

        transferFolderName = Path.Combine(transferFolderName, m_jobParams.GetParam("OutputFolderName"))

        'Retrieve Fasta file (used by the PeptideToProteinMapper)
        If Not RetrieveOrgDB(m_mgrParams.GetParam("orgdbdir")) Then Return CloseOutType.CLOSEOUT_FAILED

        'Retrieve param file
        If Not RetrieveGeneratedParamFile(m_jobParams.GetParam("ParmFileName")) Then
            Return CloseOutType.CLOSEOUT_FAILED
        End If

        ' Retrieve the Inspect Input Params file
        If Not FileSearch.RetrieveFile(clsAnalysisToolRunnerInspResultsAssembly.INSPECT_INPUT_PARAMS_FILENAME, transferFolderName) Then
            'Errors were reported in function call, so just return
            Return CloseOutType.CLOSEOUT_FAILED
        End If

        numClonedSteps = m_jobParams.GetParam("NumberOfClonedSteps")
        If String.IsNullOrEmpty(numClonedSteps) Then
            ' This is not a parallelized job
            ' Retrieve the zipped Inspect result file
            If Not FileSearch.RetrieveFile(zippedResultName, transferFolderName) Then
                If m_DebugLevel >= 3 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "FileSearch.RetrieveFile returned False for " & zippedResultName & " using folder " & transferFolderName)
                End If
                Return CloseOutType.CLOSEOUT_FAILED
            End If

            ' Unzip Inspect result file
            If m_DebugLevel >= 2 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Unzipping Inspect result file")
            End If
            If UnzipFileStart(Path.Combine(m_WorkingDir, zippedResultName), m_WorkingDir, "clsAnalysisResourcesInspResultsAssembly.GetResources", False) Then
                If m_DebugLevel >= 1 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Inspect result file unzipped")
                End If
            End If

            ' Retrieve the Inspect search log file
            If Not FileSearch.RetrieveFile(searchLogResultName, transferFolderName) Then
                If m_DebugLevel >= 3 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "FileSearch.RetrieveFile returned False for " & searchLogResultName & " using folder " & transferFolderName)
                End If
                Return CloseOutType.CLOSEOUT_FAILED
            End If

            m_jobParams.AddResultFileExtensionToSkip(searchLogResultName)

        Else
            ' This is a parallelized job
            ' Retrieve multi inspect result files
            If Not RetrieveMultiInspectResultFiles() Then
                'Errors were reported in function call, so just return
                Return CloseOutType.CLOSEOUT_FAILED
            End If
        End If

        'All finished
        Return CloseOutType.CLOSEOUT_SUCCESS

    End Function

    ''' <summary>
    ''' Retrieves inspect and inspect log and error files
    ''' </summary>
    ''' <returns>TRUE for success, FALSE for error</returns>
    ''' <remarks></remarks>
    Protected Function RetrieveMultiInspectResultFiles() As Boolean

        Dim InspectResultsFile As String
        Dim strFileName As String = String.Empty

        Dim numOfResultFiles As Integer
        Dim fileNum As Integer
        Dim DatasetName As String = m_jobParams.GetParam("datasetNum")
        Dim transferFolderName As String = Path.Combine(m_jobParams.GetParam("transferFolderPath"), DatasetName)
        Dim dtaFilename As String

        Dim intFileCopyCount = 0
        Dim intLogFileIndex As Integer

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
                If Not CopyFileToWorkDir(InspectResultsFile, transferFolderName, m_WorkingDir) Then
                    ' Error copying file (error will have already been logged)
                    If m_DebugLevel >= 3 Then
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "CopyFileToWorkDir returned False for " & InspectResultsFile & " using folder " & transferFolderName)
                    End If
                    Return False
                End If
                intFileCopyCount += 1

                ' Update the list of files to delete from the server
                m_jobParams.AddServerFileToDelete(Path.Combine(transferFolderName, InspectResultsFile))
                m_jobParams.AddServerFileToDelete(Path.Combine(transferFolderName, dtaFilename))

                ' Update the list of local files to delete
                m_jobParams.AddResultFileToSkip(InspectResultsFile)
            End If

            ' Copy the various log files
            For intLogFileIndex = 1 To 3
                Select Case intLogFileIndex
                    Case 1
                        'Copy the Inspect error file from the transfer directory
                        strFileName = DatasetName & "_" & fileNum & "_error.txt"
                    Case 2
                        'Copy each Inspect search log file from the transfer directory
                        strFileName = "InspectSearchLog_" & fileNum & ".txt"
                    Case 3
                        'Copy each Inspect console output file from the transfer directory
                        strFileName = "InspectConsoleOutput_" & fileNum & ".txt"
                End Select

                If File.Exists(Path.Combine(transferFolderName, strFileName)) Then
                    If Not CopyFileToWorkDir(strFileName, transferFolderName, m_WorkingDir) Then
                        ' Error copying file (error will have already been logged)
                        If m_DebugLevel >= 3 Then
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "CopyFileToWorkDir returned False for " & strFileName & " using folder " & transferFolderName)
                        End If
                        Return False
                    End If
                    intFileCopyCount += 1

                    ' Update the list of files to delete from the server
                    m_jobParams.AddServerFileToDelete(Path.Combine(transferFolderName, strFileName))

                    ' Update the list of local files to delete
                    m_jobParams.AddResultFileToSkip(strFileName)
                End If

            Next intLogFileIndex

        Next

        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Multi Inspect Result Files copied to local working directory; copied " & intFileCopyCount & " files")

        Return True

    End Function

#End Region

End Class
