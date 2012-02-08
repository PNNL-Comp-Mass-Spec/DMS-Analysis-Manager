Option Strict On

' Last modified 06/15/2009 JDS - Added logging using log4net
Imports AnalysisManagerBase
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

        Dim DatasetName As String = m_jobParams.GetParam("datasetNum")
        Dim transferFolderName As String = System.IO.Path.Combine(m_jobParams.GetParam("transferFolderPath"), DatasetName)
        Dim zippedResultName As String = DatasetName & "_inspect.zip"
        Dim searchLogResultName As String = "InspectSearchLog.txt"

        transferFolderName = System.IO.Path.Combine(transferFolderName, m_jobParams.GetParam("OutputFolderName"))

        'Clear out list of files to delete or keep when packaging the results
        clsGlobal.ResetFilesToDeleteOrKeep()

        'Retrieve Fasta file (used by the PeptideToProteinMapper)
        If Not RetrieveOrgDB(m_mgrParams.GetParam("orgdbdir")) Then Return IJobParams.CloseOutType.CLOSEOUT_FAILED

        'Retrieve param file
        If Not RetrieveGeneratedParamFile( _
         m_jobParams.GetParam("ParmFileName"), _
         m_jobParams.GetParam("ParmFileStoragePath"), _
         m_WorkingDir) _
        Then Return IJobParams.CloseOutType.CLOSEOUT_FAILED

        ' Retrieve the Inspect Input Params file
        If Not RetrieveFile(clsAnalysisToolRunnerInspResultsAssembly.INSPECT_INPUT_PARAMS_FILENAME, transferFolderName, m_WorkingDir) Then
            'Errors were reported in function call, so just return
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        numClonedSteps = m_jobParams.GetParam("NumberOfClonedSteps")
        If [String].IsNullOrEmpty(numClonedSteps) Then
            ' This is not a parallelized job
            ' Retrieve the zipped Inspect result file
            If Not RetrieveFile(zippedResultName, transferFolderName, m_WorkingDir) Then
                If m_DebugLevel >= 3 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "RetrieveFile returned False for " & zippedResultName & " using folder " & transferFolderName)
                End If
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            'Unzip Inspect result file
            If m_DebugLevel >= 2 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Unzipping Inspect result file")
            End If
            If UnzipFileStart(System.IO.Path.Combine(m_WorkingDir, zippedResultName), m_WorkingDir, "clsAnalysisResourcesInspResultsAssembly.GetResources", False) Then
                If m_DebugLevel >= 1 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Inspect result file unzipped")
                End If
            End If

            ' Rtrieve the Inspect search log file
            If Not RetrieveFile(searchLogResultName, transferFolderName, m_WorkingDir) Then
                If m_DebugLevel >= 3 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "RetrieveFile returned False for " & searchLogResultName & " using folder " & transferFolderName)
                End If
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            clsGlobal.m_FilesToDeleteExt.Add(searchLogResultName)

        Else
            ' This is a parallelized job
            ' Retrieve multi inspect result files
            If Not RetrieveMultiInspectResultFiles() Then
                'Errors were reported in function call, so just return
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If
        End If

        'All finished
        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    ''' <summary>
    ''' Retrieves inspect and inspect log and error files
    ''' </summary>
    ''' <returns>TRUE for success, FALSE for error</returns>
    ''' <remarks></remarks>
    Public Overridable Function RetrieveMultiInspectResultFiles() As Boolean

        Dim InspectResultsFile As String
        Dim strFileName As String = String.Empty

        Dim numOfResultFiles As Integer
        Dim fileNum As Integer
        Dim DatasetName As String = m_jobParams.GetParam("datasetNum")
        Dim transferFolderName As String = System.IO.Path.Combine(m_jobParams.GetParam("transferFolderPath"), DatasetName)
        Dim dtaFilename As String

        Dim intFileCopyCount As Integer = 0
        Dim intLogFileIndex As Integer

        transferFolderName = System.IO.Path.Combine(transferFolderName, m_jobParams.GetParam("OutputFolderName"))

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

            If System.IO.File.Exists(System.IO.Path.Combine(transferFolderName, InspectResultsFile)) Then
                If Not CopyFileToWorkDir(InspectResultsFile, transferFolderName, m_WorkingDir) Then
                    ' Error copying file (error will have already been logged)
                    If m_DebugLevel >= 3 Then
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "CopyFileToWorkDir returned False for " & InspectResultsFile & " using folder " & transferFolderName)
                    End If
                    Return False
                End If
                intFileCopyCount += 1

                ' Update the list of files to delete from the server
                clsGlobal.m_ServerFilesToDelete.Add(System.IO.Path.Combine(transferFolderName, InspectResultsFile))
                clsGlobal.m_ServerFilesToDelete.Add(System.IO.Path.Combine(transferFolderName, dtaFilename))

                ' Update the list of local files to delete
                clsGlobal.FilesToDelete.Add(InspectResultsFile)
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

                If System.IO.File.Exists(System.IO.Path.Combine(transferFolderName, strFileName)) Then
                    If Not CopyFileToWorkDir(strFileName, transferFolderName, m_WorkingDir) Then
                        ' Error copying file (error will have already been logged)
                        If m_DebugLevel >= 3 Then
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "CopyFileToWorkDir returned False for " & strFileName & " using folder " & transferFolderName)
                        End If
                        Return False
                    End If
                    intFileCopyCount += 1

                    ' Update the list of files to delete from the server
                    clsGlobal.m_ServerFilesToDelete.Add(System.IO.Path.Combine(transferFolderName, strFileName))

                    ' Update the list of local files to delete
                    clsGlobal.FilesToDelete.Add(strFileName)
                End If

            Next intLogFileIndex

        Next

        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Multi Inspect Result Files copied to local working directory; copied " & intFileCopyCount & " files")

        Return True

    End Function

#End Region

End Class
