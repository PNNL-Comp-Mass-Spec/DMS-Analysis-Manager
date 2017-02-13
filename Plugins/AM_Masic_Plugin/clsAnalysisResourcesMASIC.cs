Option Strict On

Imports AnalysisManagerBase
Imports System.IO

Public Class clsAnalysisResourcesMASIC
    Inherits clsAnalysisResources

#Region "Methods"
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

        ' Get input data file
        Dim CreateStoragePathInfoOnly = False
        Dim RawDataType As String = m_jobParams.GetParam("RawDataType")
        Dim toolName As String = m_jobParams.GetParam("ToolName")

        Select Case RawDataType.ToLower()
            Case RAW_DATA_TYPE_DOT_RAW_FILES,
                 RAW_DATA_TYPE_DOT_WIFF_FILES,
                 RAW_DATA_TYPE_DOT_UIMF_FILES,
                 RAW_DATA_TYPE_DOT_MZXML_FILES,
                 RAW_DATA_TYPE_DOT_D_FOLDERS

                ' If desired, set the following to True to not actually copy the .Raw 
                ' (or .wiff, .uimf, etc.) file locally, and instead determine where it is 
                ' located, then create a text file named "DatesetName.raw_StoragePathInfo.txt"
                ' This file would contain just one line of text: the full path to the actual file

                ' However, we have found that this can create undo strain on the storage servers (or NWFS Archive)
                ' Thus, we are now setting this to False
                CreateStoragePathInfoOnly = False
            Case Else
                CreateStoragePathInfoOnly = False
        End Select

        If Not FileSearch.RetrieveSpectra(RawDataType, CreateStoragePathInfoOnly) Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisResourcesDecon2ls.GetResources: Error occurred retrieving spectra.")
            Return CloseOutType.CLOSEOUT_FAILED
        End If

        If Not MyBase.ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders) Then
            Return CloseOutType.CLOSEOUT_FAILED
        End If

        If String.Compare(RawDataType, RAW_DATA_TYPE_DOT_RAW_FILES, True) = 0 AndAlso toolName.ToLower().StartsWith("MASIC_Finnigan".ToLower()) Then

            Dim strRawFileName = DatasetName & ".raw"
            Dim strInputFilePath = ResolveStoragePath(m_WorkingDir, strRawFileName)

            If String.IsNullOrWhiteSpace(strInputFilePath) Then
                ' Unable to resolve the file path
                m_message = "Could not find " & strRawFileName & " or " & strRawFileName & STORAGE_PATH_INFO_FILE_SUFFIX & " in the working folder; unable to run MASIC"
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
                Return CloseOutType.CLOSEOUT_FAILED
            End If

            ' Deprecated in December 2016
            '' Examine the size of the .Raw file
            'Dim fiInputFile As New FileInfo(strInputFilePath)

            'If clsAnalysisToolRunnerMASICFinnigan.NeedToConvertRawToMzXML(fiInputFile) Then

            '    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Generating the ScanStats files from the .Raw file since it is over 2 GB (and MASIC will therefore process a .mzXML file)")

            '    If Not GenerateScanStatsFile(deleteRawDataFile:=False) Then
            '        ' Error message should already have been logged and stored in m_message
            '        Return CloseOutType.CLOSEOUT_FAILED
            '    End If

            'End If
        End If

        ' Add additional extensions to delete after the tool finishes
        m_jobParams.AddResultFileExtensionToSkip("_StoragePathInfo.txt")

        ' We'll add the following extensions to m_FilesToDeleteExt
        ' Note, though, that the DeleteDataFile function will delete the .Raw or .mgf/.cdf files
        m_jobParams.AddResultFileExtensionToSkip(clsAnalysisResources.DOT_WIFF_EXTENSION)
        m_jobParams.AddResultFileExtensionToSkip(clsAnalysisResources.DOT_RAW_EXTENSION)
        m_jobParams.AddResultFileExtensionToSkip(clsAnalysisResources.DOT_UIMF_EXTENSION)
        m_jobParams.AddResultFileExtensionToSkip(clsAnalysisResources.DOT_MZXML_EXTENSION)

        m_jobParams.AddResultFileExtensionToSkip(clsAnalysisResources.DOT_MGF_EXTENSION)
        m_jobParams.AddResultFileExtensionToSkip(clsAnalysisResources.DOT_CDF_EXTENSION)

        'Retrieve param file
        If Not FileSearch.RetrieveFile(m_jobParams.GetParam("ParmFileName"), m_jobParams.GetParam("ParmFileStoragePath")) Then
            Return CloseOutType.CLOSEOUT_FAILED
        End If

        'All finished
        Return CloseOutType.CLOSEOUT_SUCCESS

    End Function
#End Region

End Class
