Option Strict On

Imports System.IO
Imports AnalysisManagerBase

Public Class clsAnalysisResourcesIcr2ls
    Inherits clsAnalysisResources

#Region "Methods"
    Public Overrides Function GetResources() As CloseOutType

        ' Retrieve shared resources, including the JobParameters file from the previous job step
        Dim result = GetSharedResources()
        If result <> CloseOutType.CLOSEOUT_SUCCESS Then
            Return result
        End If

        ' Retrieve param file
        If Not RetrieveFile(m_jobParams.GetParam("ParmFileName"), m_jobParams.GetParam("ParmFileStoragePath")) Then
            Return CloseOutType.CLOSEOUT_FAILED
        End If

        ' Look for an in-progress .PEK file in the transfer folder
        Dim eExistingPEKFileResult = RetrieveExistingTempPEKFile()

        If eExistingPEKFileResult = CloseOutType.CLOSEOUT_FAILED Then
            If String.IsNullOrEmpty(m_message) Then
                m_message = "Call to RetrieveExistingTempPEKFile failed"
            End If
            Return CloseOutType.CLOSEOUT_FAILED
        End If

        ' Get input data file
        If Not RetrieveSpectra(m_jobParams.GetParam("RawDataType")) Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisResourcesIcr2ls.GetResources: Error occurred retrieving spectra.")
            Return CloseOutType.CLOSEOUT_FAILED
        End If

        ' NOTE: GetBrukerSerFile is not MyEMSL-compatible
        If Not GetBrukerSerFile() Then
            Return CloseOutType.CLOSEOUT_FAILED
        End If

        Return CloseOutType.CLOSEOUT_SUCCESS

    End Function

    Private Function GetBrukerSerFile() As Boolean

        Dim strLocalDatasetFolderPath As String

        Dim blnIsFolder As Boolean

        Try
            Dim RawDataType As String = m_jobParams.GetParam("RawDataType")

            If RawDataType = RAW_DATA_TYPE_DOT_RAW_FILES Then
                ' Thermo datasets do not have ser files
                Return True
            End If

            Dim strRemoteDatasetFolderPath = Path.Combine(m_jobParams.GetParam("DatasetArchivePath"), m_jobParams.GetParam("DatasetFolderName"))

            If RawDataType.ToLower() = RAW_DATA_TYPE_BRUKER_FT_FOLDER Then
                strLocalDatasetFolderPath = Path.Combine(m_WorkingDir, DatasetName & ".d")
                strRemoteDatasetFolderPath = Path.Combine(strRemoteDatasetFolderPath, DatasetName & ".d")
            Else
                strLocalDatasetFolderPath = String.Copy(m_WorkingDir)
            End If

            Dim serFileOrFolderPath = FindSerFileOrFolder(strLocalDatasetFolderPath, blnIsFolder)

            If String.IsNullOrEmpty(serFileOrFolderPath) Then
                ' Ser file, fid file, or 0.ser folder not found in the working directory
                ' See if the file exists in the archive			

                serFileOrFolderPath = FindSerFileOrFolder(strRemoteDatasetFolderPath, blnIsFolder)

                If Not String.IsNullOrEmpty(serFileOrFolderPath) Then
                    ' File found in the archive; need to copy it locally

                    Dim dtStartTime As DateTime = Date.UtcNow

                    If blnIsFolder Then
                        Dim diSourceFolder As DirectoryInfo
                        diSourceFolder = New DirectoryInfo(serFileOrFolderPath)

                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Copying 0.ser folder from archive to working directory: " & serFileOrFolderPath)
                        ResetTimestampForQueueWaitTimeLogging()
                        m_FileTools.CopyDirectory(serFileOrFolderPath, Path.Combine(strLocalDatasetFolderPath, diSourceFolder.Name))

                        If m_DebugLevel >= 1 Then
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Successfully copied 0.ser folder in " & Date.UtcNow.Subtract(dtStartTime).TotalSeconds.ToString("0") & " seconds")
                        End If

                    Else
                        Dim fiSourceFile As FileInfo
                        fiSourceFile = New FileInfo(serFileOrFolderPath)

                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Copying " & Path.GetFileName(serFileOrFolderPath) & " file from archive to working directory: " & serFileOrFolderPath)

                        If Not CopyFileToWorkDir(fiSourceFile.Name, fiSourceFile.Directory.FullName, strLocalDatasetFolderPath, clsLogTools.LogLevels.ERROR) Then
                            Return False
                        Else
                            If m_DebugLevel >= 1 Then
                                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO,
                                                     "Successfully copied " & Path.GetFileName(serFileOrFolderPath) & " file in " &
                                                     Date.UtcNow.Subtract(dtStartTime).TotalSeconds.ToString("0") & " seconds")
                            End If
                        End If

                    End If
                End If

            End If

            Return True

        Catch ex As Exception
            m_message = "Exception in GetBrukerSerFile"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message)
            Return False
        End Try

    End Function

    ''' <summary>
    ''' Looks for a ser file, fid file, or 0.ser folder in strFolderToCheck
    ''' </summary>
    ''' <param name="strFolderToCheck"></param>
    ''' <param name="blnIsFolder"></param>
    ''' <returns>The path to the ser file, fid file, or 0.ser folder, if found.  An empty string if not found</returns>
    ''' <remarks></remarks>
    Public Shared Function FindSerFileOrFolder(strFolderToCheck As String, ByRef blnIsFolder As Boolean) As String

        blnIsFolder = False

        ' Look for a ser file in the working directory
        Dim serFileOrFolderPath = Path.Combine(strFolderToCheck, BRUKER_SER_FILE)

        If File.Exists(serFileOrFolderPath) Then
            ' Ser file found
            Return serFileOrFolderPath
        End If

        ' Ser file not found; look for a fid file
        serFileOrFolderPath = Path.Combine(strFolderToCheck, BRUKER_FID_FILE)

        If File.Exists(serFileOrFolderPath) Then
            ' Fid file found
            Return serFileOrFolderPath
        End If

        ' Fid file not found; look for a 0.ser folder in the working directory
        serFileOrFolderPath = Path.Combine(strFolderToCheck, BRUKER_ZERO_SER_FOLDER)
        If Directory.Exists(serFileOrFolderPath) Then
            blnIsFolder = True
            Return serFileOrFolderPath
        End If

        Return String.Empty

    End Function

    ''' <summary>
    ''' Look for file .pek.tmp in the transfer folder
    ''' Retrieves the file if it is found
    ''' </summary>
    ''' <returns>
    ''' CLOSEOUT_SUCCESS if an existing file was found and copied, 
    ''' CLOSEOUT_FILE_NOT_FOUND if an existing file was not found, and 
    ''' CLOSEOUT_FAILURE if an error
    ''' </returns>
    ''' <remarks>
    ''' Does not validate that the ICR-2LS param file matches (in contrast, clsAnalysisResourcesSeq.vb does valid the param file).
    ''' This is done on purpose to allow us to update the param file mid job.
    ''' Scans already deisotoped will have used one parameter file; scans processed from this point forward
    ''' will use a different one; this is OK and allows us to adjust the settings mid-job.
    ''' To prevent this behavior, delete the .pek.tmp file from the transfer folder
    ''' </remarks>
    Private Function RetrieveExistingTempPEKFile() As CloseOutType

        Try

            Dim strJob = m_jobParams.GetParam("Job")
            Dim transferFolderPath = m_jobParams.GetParam("JobParameters", "transferFolderPath")

            If String.IsNullOrWhiteSpace(transferFolderPath) Then
                ' Transfer folder path is not defined
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "transferFolderPath is empty; this is unexpected")
                Return CloseOutType.CLOSEOUT_FAILED
            Else
                transferFolderPath = Path.Combine(transferFolderPath, m_jobParams.GetParam("JobParameters", "DatasetFolderName"))
                transferFolderPath = Path.Combine(transferFolderPath, m_jobParams.GetParam("StepParameters", "OutputFolderName"))
            End If

            If m_DebugLevel >= 4 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Checking for " & clsAnalysisToolRunnerICRBase.PEK_TEMP_FILE & " file at " & transferFolderPath)
            End If

            Dim diSourceFolder = New DirectoryInfo(transferFolderPath)

            If Not diSourceFolder.Exists Then
                ' Transfer folder not found; return false
                If m_DebugLevel >= 4 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "  ... Transfer folder not found: " & diSourceFolder.FullName)
                End If
                Return CloseOutType.CLOSEOUT_FILE_NOT_FOUND
            End If

            Dim pekTempFilePath = Path.Combine(diSourceFolder.FullName, DatasetName & clsAnalysisToolRunnerICRBase.PEK_TEMP_FILE)

            Dim fiTempPekFile = New FileInfo(pekTempFilePath)
            If Not fiTempPekFile.Exists Then
                If m_DebugLevel >= 4 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "  ... " & clsAnalysisToolRunnerICRBase.PEK_TEMP_FILE & " file not found")
                End If
                Return CloseOutType.CLOSEOUT_FILE_NOT_FOUND
            End If

            If m_DebugLevel >= 1 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, clsAnalysisToolRunnerICRBase.PEK_TEMP_FILE & " file found for job " & strJob & " (file size = " & (fiTempPekFile.Length / 1024.0).ToString("#,##0") & " KB)")
            End If

            ' Copy fiTempPekFile locally
            Try
                fiTempPekFile.CopyTo(Path.Combine(m_WorkingDir, fiTempPekFile.Name), True)

                If m_DebugLevel >= 1 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Copied " & fiTempPekFile.Name & " locally; will resume ICR-2LS analysis")
                End If

                ' If the job succeeds, we should delete the .pek.tmp file from the transfer folder
                ' Add the full path to m_ServerFilesToDelete using AddServerFileToDelete
                m_jobParams.AddServerFileToDelete(fiTempPekFile.FullName)

            Catch ex As Exception
                ' Error copying the file; treat this as a failed job
                m_message = " Exception copying " & clsAnalysisToolRunnerICRBase.PEK_TEMP_FILE & " file locally"
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "  ... Exception copying " & fiTempPekFile.FullName & " locally; unable to resume: " & ex.Message)
                Return CloseOutType.CLOSEOUT_FAILED
            End Try

            Return CloseOutType.CLOSEOUT_SUCCESS

        Catch ex As Exception
            m_message = "Exception in RetrieveExistingTempPEKFile"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message)
            Return CloseOutType.CLOSEOUT_FAILED
        End Try

    End Function
#End Region

End Class
