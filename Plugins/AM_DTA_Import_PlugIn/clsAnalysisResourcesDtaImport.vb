Imports AnalysisManagerBase
Imports System.IO
Imports System

Public Class clsAnalysisResourcesDtaImport
    Inherits clsAnalysisResources


#Region "Methods"
    ''' <summary>
    ''' Retrieves files necessary for performance of Sequest analysis
    ''' </summary>
    ''' <returns>IJobParams.CloseOutType indicating success or failure</returns>
    ''' <remarks></remarks>
    Public Overrides Function GetResources() As IJobParams.CloseOutType

        ' Retrieve shared resources, including the JobParameters file from the previous job step
        Dim result = GetSharedResources()
        If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            Return result
        End If

        ' There are really no resources to get, so just clear the list of files to delete or keep and validate zip file
        result = ValidateDTA()
        If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            Return result
        End If

        ' All finished
        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    Private Function ValidateDTA() As IJobParams.CloseOutType

        Dim SourceFolderNamePath As String = String.Empty
        Try
            ' Note: the DTAFolderLocation is defined in the Manager_Control DB, and is specific for this manager
            '       for example: \\pnl\projects\MSSHARE\SPurvine
            ' This folder must contain subfolders whose name matches the output_folder name assigned to each job
            ' Furthermore, each subfolder must have a file named Dataset_dta.zip

            SourceFolderNamePath = System.IO.Path.Combine(m_mgrParams.GetParam("DTAFolderLocation"), m_jobParams.GetParam("OutputFolderName"))

            'Determine if Dta folder in source directory exists
            If Not System.IO.Directory.Exists(SourceFolderNamePath) Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN, "Source Directory for Manually created Dta does not exist: " & SourceFolderNamePath)
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
                'TODO: Handle errors
            End If

            Dim zipFileName As String = m_DatasetName & "_dta.zip"
            Dim fileEntries As String() = Directory.GetFiles(SourceFolderNamePath, zipFileName)

            ' Process the list of files found in the directory.
            Dim fileName As String
            If fileEntries.Length < 1 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN, "DTA zip file was not found in source directory: " & Path.Combine(SourceFolderNamePath, zipFileName))
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            'If valid zip file is found, then uzip the contents
            For Each fileName In fileEntries
                If UnzipFileStart(Path.Combine(m_WorkingDir, fileName), m_WorkingDir, "clsAnalysisResourcesDtaImport.ValidateDTA", False) Then
                    If m_DebugLevel >= 1 Then
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Manual DTA file unzipped")
                    End If
                Else
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN, "An error occurred while unzipping the DTA file: " & Path.Combine(SourceFolderNamePath, zipFileName))
                    Return IJobParams.CloseOutType.CLOSEOUT_FAILED
                End If
            Next fileName

            Dim txtFileName As String = m_DatasetName & "_dta.txt"
            fileEntries = Directory.GetFiles(m_WorkingDir, txtFileName)
            If fileEntries.Length < 1 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN, "DTA text file in the zip file was named incorrectly or not valid: " & Path.Combine(SourceFolderNamePath, txtFileName))
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN, "An exception occurred while validating manually created DTA zip file. " & SourceFolderNamePath & " : " & ex.Message)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function


#End Region

End Class
