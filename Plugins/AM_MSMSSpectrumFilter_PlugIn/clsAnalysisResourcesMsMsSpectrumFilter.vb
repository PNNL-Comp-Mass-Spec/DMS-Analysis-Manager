Option Strict On

' This class was created to support being loaded as a pluggable DLL into the New DMS 
' Analysis Tool Manager program.  Each DLL requires a Resource class.  The new ATM 
' supports the mini-pipeline. It uses class MsMsSpectrumFilter.DLL to filter the .DTA 
' files present in a given folder
'
' Written by John Sandoval for the Department of Energy (PNNL, Richland, WA)
' Copyright 2009, Battelle Memorial Institute
' Started January 20, 2009

Imports System.IO
Imports AnalysisManagerBase
Imports MSMSSpectrumFilter
Imports MyEMSLReader

Public Class clsAnalysisResourcesMsMsSpectrumFilter
    Inherits clsAnalysisResources

#Region "Methods"
    ''' <summary>
    ''' Retrieves files necessary for performance of Sequest analysis
    ''' </summary>
    ''' <returns>IJobParams.CloseOutType indicating success or failure</returns>
    ''' <remarks></remarks>
    Public Overrides Function GetResources() As IJobParams.CloseOutType

        ' Retrieve the _DTA.txt file
        ' Note that if the file was found in MyEMSL then RetrieveDtaFiles will auto-call ProcessMyEMSLDownloadQueue to download the file
        If Not RetrieveDtaFiles() Then
            'Errors were reported in function call, so just return
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        ' Add the _dta.txt file to the list of extensions to delete after the tool finishes
        m_jobParams.AddResultFileExtensionToSkip(m_DatasetName & "_dta.txt") 'Unzipped, concatenated DTA

        ' Add the _Dta.zip file to the list of files to move to the results folder
        ' Note that this .Zip file will contain the filtered _Dta.txt file (not the original _Dta.txt file)
        m_jobParams.AddResultFileToKeep("_dta.zip") 'Zipped DTA


        ' Look at the job parameters
        ' If ScanTypeFilter is defined, or MSCollisionModeFilter is defined, or MSLevelFilter is defined, then we need either of the following
        '  a) The _ScanStats.txt file and _ScanStatsEx.txt file from a MASIC job for this dataset
        '       This is essentially a job-depending-on a job
        '  b) The .Raw file
        '

        Dim strMSLevelFilter As String

        Dim strScanTypeFilter As String
        Dim strScanTypeMatchType As String

        Dim strMSCollisionModeFilter As String
        Dim strMSCollisionModeMatchType As String
        Dim blnNeedScanStatsFiles = False

        strMSLevelFilter = m_jobParams.GetJobParameter("MSLevelFilter", "0")

        strScanTypeFilter = m_jobParams.GetJobParameter("ScanTypeFilter", "")
        strScanTypeMatchType = m_jobParams.GetJobParameter("ScanTypeMatchType", clsMsMsSpectrumFilter.TEXT_MATCH_TYPE_CONTAINS)

        strMSCollisionModeFilter = m_jobParams.GetJobParameter("MSCollisionModeFilter", "")
        strMSCollisionModeMatchType = m_jobParams.GetJobParameter("MSCollisionModeMatchType", clsMsMsSpectrumFilter.TEXT_MATCH_TYPE_CONTAINS)


        If Not strMSLevelFilter Is Nothing AndAlso strMSLevelFilter.Length > 0 AndAlso strMSLevelFilter <> "0" Then
            If m_DebugLevel >= 1 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "GetResources: MSLevelFilter is defined (" & strMSLevelFilter & "); will retrieve or generate the ScanStats files")
            End If
            blnNeedScanStatsFiles = True
        End If

        If Not strScanTypeFilter Is Nothing AndAlso strScanTypeFilter.Length > 0 Then
            If m_DebugLevel >= 1 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "GetResources: ScanTypeFilter is defined (" & strScanTypeFilter & " with match type " & strScanTypeMatchType & "); will retrieve or generate the ScanStats files")
            End If
            blnNeedScanStatsFiles = True
        End If

        If Not strMSCollisionModeFilter Is Nothing AndAlso strMSCollisionModeFilter.Length > 0 Then
            If m_DebugLevel >= 1 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "GetResources: MSCollisionModeFilter is defined (" & strMSCollisionModeFilter & " with match type " & strMSCollisionModeMatchType & "); will retrieve or generate the ScanStats files")
            End If
            blnNeedScanStatsFiles = True
        End If

        If blnNeedScanStatsFiles Then
            ' Find and copy the ScanStats files from an existing job rather than copying over the .Raw file
            ' However, if the _ScanStats.txt file does not have column ScanTypeName, then we will need the .raw file

            Dim blnIsFolder = False
            Dim strDatasetFileOrFolderPath As String
            Dim diDatasetFolder As DirectoryInfo
            Dim blnScanStatsFilesRetrieved = False

            strDatasetFileOrFolderPath = FindDatasetFileOrFolder(blnIsFolder)

            If Not String.IsNullOrEmpty(strDatasetFileOrFolderPath) And Not strDatasetFileOrFolderPath.StartsWith(MYEMSL_PATH_FLAG) Then

                If blnIsFolder Then
                    diDatasetFolder = New DirectoryInfo(strDatasetFileOrFolderPath)
                    diDatasetFolder = diDatasetFolder.Parent
                Else
                    Dim fiDatasetFile = New FileInfo(strDatasetFileOrFolderPath)
                    diDatasetFolder = fiDatasetFile.Directory
                End If

                If FindExistingScanStatsFile(diDatasetFolder.FullName) Then
                    blnScanStatsFilesRetrieved = True
                End If

            End If

            If Not blnScanStatsFilesRetrieved Then

                ' Find the dataset file and either create a StoragePathInfo file or copy it locally

                Dim CreateStoragePathInfoOnly = False
                Dim RawDataType As String = m_jobParams.GetParam("RawDataType")

                Select Case RawDataType.ToLower
                    Case RAW_DATA_TYPE_DOT_RAW_FILES, RAW_DATA_TYPE_DOT_WIFF_FILES, RAW_DATA_TYPE_DOT_UIMF_FILES, RAW_DATA_TYPE_DOT_MZXML_FILES
                        ' Don't actually copy the .Raw (or .wiff, .uimf, etc.) file locally; instead, 
                        '  determine where it is located then create a text file named "DatesetName.raw_StoragePathInfo.txt"
                        '  This new file contains just one line of text: the full path to the actual file
                        CreateStoragePathInfoOnly = True
                    Case Else
                        CreateStoragePathInfoOnly = False
                End Select

                If Not RetrieveSpectra(RawDataType, CreateStoragePathInfoOnly) Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisResourcesMsMsSpectrumFilter.GetResources: Error occurred retrieving spectra.")
                    Return IJobParams.CloseOutType.CLOSEOUT_FAILED
                End If

            End If

            ' Add additional extensions to delete after the tool finishes
            m_jobParams.AddResultFileExtensionToSkip("_ScanStats.txt")
            m_jobParams.AddResultFileExtensionToSkip("_ScanStatsEx.txt")
            m_jobParams.AddResultFileExtensionToSkip("_StoragePathInfo.txt")

            m_jobParams.AddResultFileExtensionToSkip(DOT_WIFF_EXTENSION)
            m_jobParams.AddResultFileExtensionToSkip(DOT_RAW_EXTENSION)
            m_jobParams.AddResultFileExtensionToSkip(DOT_UIMF_EXTENSION)
            m_jobParams.AddResultFileExtensionToSkip(DOT_MZXML_EXTENSION)

            m_jobParams.AddResultFileExtensionToSkip(DOT_MGF_EXTENSION)
            m_jobParams.AddResultFileExtensionToSkip(DOT_CDF_EXTENSION)
        End If

        If Not MyBase.ProcessMyEMSLDownloadQueue(m_WorkingDir, Downloader.DownloadFolderLayout.FlatNoSubfolders) Then
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        'All finished
        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    Private Function FindExistingScanStatsFile(strDatasetFolderPath As String) As Boolean

        Dim diDatasetFolder = New DirectoryInfo(strDatasetFolderPath)
        Dim blnFilesFound = False

        Try
            If Not diDatasetFolder.Exists Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Dataset folder not found: " & strDatasetFolderPath)
            End If

            Dim lstFiles As List(Of FileInfo)
            lstFiles = diDatasetFolder.GetFiles(m_DatasetName & "_ScanStats.txt", SearchOption.AllDirectories).ToList

            If lstFiles.Count = 0 Then
                If m_DebugLevel >= 2 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "No _ScanStats.txt files were found in subfolders below " & strDatasetFolderPath)
                End If
                Return False
            End If

            ' Find the newest file in lstFiles
            Dim lstSortedFiles As List(Of FileInfo) = (From item In lstFiles Order By item.LastWriteTime Descending).ToList()

            Dim fiNewestScanStatsFile As FileInfo = lstSortedFiles(0)

            ' Copy the ScanStats file locally
            fiNewestScanStatsFile.CopyTo(Path.Combine(m_WorkingDir, fiNewestScanStatsFile.Name))

            ' Read the first line of the file and confirm that the _ScanTypeName column exists
            Using srScanStatsFile = New StreamReader(New FileStream(fiNewestScanStatsFile.FullName, FileMode.Open, FileAccess.Read, FileShare.Read))
                Dim strLineIn As String
                strLineIn = srScanStatsFile.ReadLine

                If Not strLineIn.Contains(clsMsMsSpectrumFilter.SCANSTATS_COL_SCAN_TYPE_NAME) Then
                    If m_DebugLevel >= 1 Then
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "The newest _ScanStats.txt file for this dataset does not contain column " & clsMsMsSpectrumFilter.SCANSTATS_COL_SCAN_TYPE_NAME & "; will need to re-generate the file using the .Raw file")
                    End If
                    Return False
                End If
            End Using

            ' Look for the _ScanStatsEx.txt file
            Dim strScanStatsExPath As String = Path.Combine(fiNewestScanStatsFile.Directory.FullName, Path.GetFileNameWithoutExtension(fiNewestScanStatsFile.Name) & "Ex.txt")

            If File.Exists(strScanStatsExPath) Then
                ' Copy it locally
                File.Copy(strScanStatsExPath, Path.Combine(m_WorkingDir, Path.GetFileName(strScanStatsExPath)))

                If m_DebugLevel >= 1 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Using existing _ScanStats.txt from " & fiNewestScanStatsFile.FullName)
                End If

                blnFilesFound = True
            Else
                If m_DebugLevel >= 1 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "The _ScanStats.txt file was found at " & fiNewestScanStatsFile.FullName & " but the _ScanStatsEx.txt file was not present")
                End If
                Return False
            End If

        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception in FindExistingScanStatsFile", ex)
            Return False
        End Try

        Return blnFilesFound
    End Function

#End Region

End Class
