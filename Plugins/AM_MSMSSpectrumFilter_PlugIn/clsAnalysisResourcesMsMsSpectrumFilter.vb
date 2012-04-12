Option Strict On

' This class was created to support being loaded as a pluggable DLL into the New DMS 
' Analysis Tool Manager program.  Each DLL requires a Resource class.  The new ATM 
' supports the mini-pipeline. It uses class clsMsMsSpectrumFilter to filter the .DTA 
' files present in a given folder
'
' Written by John Sandoval for the Department of Energy (PNNL, Richland, WA)
' Copyright 2009, Battelle Memorial Institute
' Started January 20, 2009

Imports AnalysisManagerBase

Public Class clsAnalysisResourcesMsMsSpectrumFilter
    Inherits clsAnalysisResources

#Region "Methods"
    ''' <summary>
    ''' Retrieves files necessary for performance of Sequest analysis
    ''' </summary>
    ''' <returns>IJobParams.CloseOutType indicating success or failure</returns>
    ''' <remarks></remarks>
    Public Overrides Function GetResources() As IJobParams.CloseOutType

        Dim strWorkDir As String = m_mgrParams.GetParam("workdir")

        'Retrieve the dta files (but do not unconcatenate)
        If Not RetrieveDtaFiles(False) Then
            'Errors were reported in function call, so just return
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        ' Add the _dta.txt file to the list of extensions to delete after the tool finishes
        m_JobParams.AddResultFileExtensionToSkip(m_jobParams.GetParam("DatasetNum") & "_dta.txt") 'Unzipped, concatenated DTA

        ' Add the _Dta.zip file to the list of files to move to the results folder
        ' Note that this .Zip file will contain the filtered _Dta.txt file (not the original _Dta.txt file)
        m_jobParams.AddResultFileToKeep("_dta.zip") 'Zipped DTA


        ' Look at the job parameterse
        ' If ScanTypeFilter is defined, or MSCollisionModeFilter is defined, or MSLevelFilter is defined, then we need either of the following
        '  a) The _ScanStats.txt file and _ScanStatsEx.txt file from a MASIC job for this dataset
        '       This is essentially a job-depending-on a job
        '  b) The .Raw file
        '
        ' For safety, we will re-generate the _ScanStats.txt and _ScanStatsEx.txt files in case the MASIC versions are out-of-date or missing the ScanTypeName column (which was added around January 2010)

        Dim strMSLevelFilter As String

        Dim strScanTypeFilter As String
        Dim strScanTypeMatchType As String

        Dim strMSCollisionModeFilter As String
        Dim strMSCollisionModeMatchType As String
        Dim blnNeedScanStatsFiles As Boolean = False

        strMSLevelFilter = m_jobParams.GetJobParameter("MSLevelFilter", "0")

        strScanTypeFilter = m_jobParams.GetJobParameter("ScanTypeFilter", "")
        strScanTypeMatchType = m_jobParams.GetJobParameter("ScanTypeMatchType", MSMSSpectrumFilterAM.clsMsMsSpectrumFilter.TEXT_MATCH_TYPE_CONTAINS)

        strMSCollisionModeFilter = m_jobParams.GetJobParameter("MSCollisionModeFilter", "")
        strMSCollisionModeMatchType = m_jobParams.GetJobParameter("MSCollisionModeMatchType", MSMSSpectrumFilterAM.clsMsMsSpectrumFilter.TEXT_MATCH_TYPE_CONTAINS)


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
            ' Future possible ToDo: try to find and copy the ScanStats files from an existing job rather than copying over the .Raw file

            ' Get input data file

            Dim CreateStoragePathInfoOnly As Boolean = False
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

            If Not RetrieveSpectra(RawDataType, strWorkDir, CreateStoragePathInfoOnly) Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisResourcesMsMsSpectrumFilter.GetResources: Error occurred retrieving spectra.")
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            ' Add additional extensions to delete after the tool finishes
            m_JobParams.AddResultFileExtensionToSkip("_ScanStats.txt")
            m_JobParams.AddResultFileExtensionToSkip("_ScanStatsEx.txt")
            m_JobParams.AddResultFileExtensionToSkip("_StoragePathInfo.txt")

            m_JobParams.AddResultFileExtensionToSkip(clsAnalysisResources.DOT_WIFF_EXTENSION)
            m_JobParams.AddResultFileExtensionToSkip(clsAnalysisResources.DOT_RAW_EXTENSION)
            m_JobParams.AddResultFileExtensionToSkip(clsAnalysisResources.DOT_UIMF_EXTENSION)
            m_JobParams.AddResultFileExtensionToSkip(clsAnalysisResources.DOT_MZXML_EXTENSION)

            m_JobParams.AddResultFileExtensionToSkip(clsAnalysisResources.DOT_MGF_EXTENSION)
            m_JobParams.AddResultFileExtensionToSkip(clsAnalysisResources.DOT_CDF_EXTENSION)
        End If

        'All finished
        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function
#End Region

End Class
