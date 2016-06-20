Option Strict On

Imports AnalysisManagerBase
Imports PHRPReader
Imports System.IO

Public Class clsAnalysisResourcesIDPicker
    Inherits clsAnalysisResources

    Public Const IDPICKER_PARAM_FILENAME_LOCAL As String = "IDPickerParamFileLocal"
    Public Const DEFAULT_IDPICKER_PARAM_FILE_NAME As String = "IDPicker_Defaults.txt"

    Protected mSynopsisFileIsEmpty As Boolean

    ' This dictionary holds any filenames that we need to rename after copying locally
    Protected mInputFileRenames As Dictionary(Of String, String)

    Public Overrides Sub Setup(mgrParams As IMgrParams, jobParams As IJobParams, statusTools As IStatusFile, myEMSLUtilities As clsMyEMSLUtilities)
        MyBase.Setup(mgrParams, jobParams, statusTools, myEmslUtilities)
        SetOption(clsGlobal.eAnalysisResourceOptions.OrgDbRequired, True)
    End Sub

    Public Overrides Function GetResources() As IJobParams.CloseOutType

        Dim eReturnCode = IJobParams.CloseOutType.CLOSEOUT_SUCCESS

        ' Retrieve the parameter file for the associated peptide search tool (Sequest, XTandem, MSGF+, etc.)
        Dim strParamFileName As String = m_jobParams.GetParam("ParmFileName")

        If Not FindAndRetrieveMiscFiles(strParamFileName, False) Then
            Return IJobParams.CloseOutType.CLOSEOUT_NO_PARAM_FILE
        End If
        m_jobParams.AddResultFileToSkip(strParamFileName)

        If Not clsAnalysisToolRunnerIDPicker.ALWAYS_SKIP_IDPICKER Then
            ' Retrieve the IDPicker parameter file specified for this job
            If Not RetrieveIDPickerParamFile() Then
                Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
            End If
        End If


        Dim RawDataType = m_jobParams.GetParam("RawDataType")
        Dim eRawDataType = GetRawDataType(RawDataType)
        Dim blnMGFInstrumentData = m_jobParams.GetJobParameter("MGFInstrumentData", False)

        ' Retrieve the PSM result files, PHRP files, and MSGF file
        If Not GetInputFiles(m_DatasetName, strParamFileName, eReturnCode) Then
            If eReturnCode = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then eReturnCode = IJobParams.CloseOutType.CLOSEOUT_FAILED
            Return eReturnCode
        End If

        If mSynopsisFileIsEmpty Then
            ' Don't retrieve any additional files
            Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
        End If

        If Not blnMGFInstrumentData Then

            ' Retrieve the MASIC ScanStats.txt and ScanStatsEx.txt files
            If eRawDataType = eRawDataTypeConstants.ThermoRawFile Or eRawDataType = eRawDataTypeConstants.UIMF Then

                Dim noScanStats = m_jobParams.GetJobParameter("PepXMLNoScanStats", False)
                If noScanStats Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Not retrieving MASIC files since PepXMLNoScanStats is True")
                Else
                    Dim eResult = RetrieveMASICFilesWrapper()
                    If eResult <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                        Return eResult
                    End If
                End If

            Else
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Not retrieving MASIC files since unsupported data type: " & RawDataType)
            End If

        End If

        If Not m_MyEMSLUtilities.ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders) Then
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        Dim blnSplitFasta = m_jobParams.GetJobParameter("SplitFasta", False)

        If blnSplitFasta Then
            ' Override the SplitFasta job parameter
            m_jobParams.SetParam("SplitFasta", "False")
        End If

        If blnSplitFasta AndAlso clsAnalysisToolRunnerIDPicker.ALWAYS_SKIP_IDPICKER Then
            ' Do not retrieve the fasta file
            ' However, do contact DMS to lookup the name of the legacy fasta file that was used for this job
            m_FastaFileName = LookupLegacyFastaFileName()

            If String.IsNullOrEmpty(m_FastaFileName) Then
                If String.IsNullOrEmpty(m_message) Then
                    m_message = "Unable to determine the legacy fasta file name"
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
                End If
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            m_jobParams.AddAdditionalParameter("PeptideSearch", "generatedFastaName", m_FastaFileName)

        Else
            ' Retrieve the Fasta file
            If Not RetrieveOrgDB(m_mgrParams.GetParam("orgdbdir")) Then Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If


        If blnSplitFasta Then
            ' Restore the setting for SplitFasta
            m_jobParams.SetParam("SplitFasta", "True")
        End If

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    Private Function LookupLegacyFastaFileName() As String

        Dim dmsConnectionString = m_mgrParams.GetParam("connectionstring")
        If String.IsNullOrWhiteSpace(dmsConnectionString) Then
            m_message = "Error in LookupLegacyFastaFileName: manager parameter connectionstring is not defined"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
            Return String.Empty
        End If

        Dim sqlQuery As String = "SELECT OrganismDBName FROM V_Analysis_Job WHERE (Job = " & m_JobNum & ")"

        Dim lstResults = New List(Of String)

        Dim success = clsGlobal.GetQueryResultsTopRow(sqlQuery, dmsConnectionString, lstResults, "LookupLegacyFastaFileName")

        If Not success OrElse lstResults Is Nothing OrElse lstResults.Count = 0 Then
            m_message = "Could not determine the legacy fasta file name (OrganismDBName in V_Analysis_Job) for job " & m_JobNum
            Return String.Empty
        End If

        Return lstResults.First

    End Function

    ''' <summary>
    ''' Copies the required input files to the working directory
    ''' </summary>
    ''' <param name="strDatasetName">Dataset name</param>
    ''' <param name="strSearchEngineParamFileName">Search engine parameter file name</param>
    ''' <param name="eReturnCode">Return code</param>
    ''' <returns>True if success, otherwise false</returns>
    ''' <remarks></remarks>
    Protected Function GetInputFiles(strDatasetName As String, strSearchEngineParamFileName As String, ByRef eReturnCode As IJobParams.CloseOutType) As Boolean

        ' This tracks the filenames to find.  The Boolean value is True if the file is Required, false if not required
        Dim lstFileNamesToGet As SortedList(Of String, Boolean)
        Dim lstExtraFilesToGet As List(Of String)

        Dim strResultType As String

        Dim eResultType As clsPHRPReader.ePeptideHitResultType
        eReturnCode = IJobParams.CloseOutType.CLOSEOUT_SUCCESS

        strResultType = m_jobParams.GetParam("ResultType")

        ' Make sure the ResultType is valid
        eResultType = clsPHRPReader.GetPeptideHitResultType(strResultType)

        If Not (
          eResultType = clsPHRPReader.ePeptideHitResultType.Sequest OrElse
          eResultType = clsPHRPReader.ePeptideHitResultType.XTandem OrElse
          eResultType = clsPHRPReader.ePeptideHitResultType.Inspect OrElse
          eResultType = clsPHRPReader.ePeptideHitResultType.MSGFDB OrElse
          eResultType = clsPHRPReader.ePeptideHitResultType.MODa OrElse
          eResultType = clsPHRPReader.ePeptideHitResultType.MODPlus) Then
            m_message = "Invalid tool result type (not supported by IDPicker): " & strResultType
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
            eReturnCode = IJobParams.CloseOutType.CLOSEOUT_FAILED
            Return False
        End If

        mInputFileRenames = New Dictionary(Of String, String)

        lstFileNamesToGet = GetPHRPFileNames(eResultType, strDatasetName)
        mSynopsisFileIsEmpty = False

        If m_DebugLevel >= 2 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Retrieving the " & eResultType.ToString & " files")
        End If

        Dim synFileName As String
        synFileName = clsPHRPReader.GetPHRPSynopsisFileName(eResultType, strDatasetName)

        For Each kvEntry As KeyValuePair(Of String, Boolean) In lstFileNamesToGet

            If Not FindAndRetrieveMiscFiles(kvEntry.Key, False) Then
                ' File not found; is it required?
                If kvEntry.Value Then
                    'Errors were reported in function call, so just return
                    eReturnCode = IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
                    Return False
                End If
            End If

            m_jobParams.AddResultFileToSkip(kvEntry.Key)

            If kvEntry.Key = synFileName Then
                ' Check whether the synopsis file is empty
                Dim strSynFilePath As String
                strSynFilePath = Path.Combine(m_WorkingDir, synFileName)

                Dim strErrorMessage As String = String.Empty
                If Not ValidateFileHasData(strSynFilePath, "Synopsis file", strErrorMessage) Then
                    ' The synopsis file is empty
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, strErrorMessage)

                    ' We don't want to fail the job out yet; instead, we'll exit now, then let the ToolRunner exit with a Completion message of "Synopsis file is empty"
                    mSynopsisFileIsEmpty = True
                    Return True
                End If
            End If

        Next

        If Not m_MyEMSLUtilities.ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders) Then
            Return False
        End If

        If eResultType = clsPHRPReader.ePeptideHitResultType.XTandem Then
            ' X!Tandem requires a few additional parameter files
            lstExtraFilesToGet = clsPHRPParserXTandem.GetAdditionalSearchEngineParamFileNames(Path.Combine(m_WorkingDir, strSearchEngineParamFileName))
            For Each strFileName As String In lstExtraFilesToGet

                If Not FindAndRetrieveMiscFiles(strFileName, False) Then
                    ' File not found
                    eReturnCode = IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
                    Return False
                End If

                m_jobParams.AddResultFileToSkip(strFileName)
            Next
        End If

        If Not m_MyEMSLUtilities.ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders) Then
            Return False
        End If

        For Each item As KeyValuePair(Of String, String) In mInputFileRenames
            Dim fiFile As FileInfo
            fiFile = New FileInfo(Path.Combine(m_WorkingDir, item.Key))
            If Not fiFile.Exists Then
                m_message = "File " & item.Key & " not found; unable to rename to " & item.Value
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
                eReturnCode = IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
                Return False
            Else
                Try
                    fiFile.MoveTo(Path.Combine(m_WorkingDir, item.Value))
                Catch ex As Exception
                    m_message = "Error renaming file " & item.Key & " to " & item.Value
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & "; " & ex.Message)
                    eReturnCode = IJobParams.CloseOutType.CLOSEOUT_FAILED
                    Return False
                End Try

                m_jobParams.AddResultFileToSkip(item.Value)
            End If
        Next

        Return True

    End Function

    ''' <summary>
    ''' Retrieve the ID Picker parameter file
    ''' </summary>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Protected Function RetrieveIDPickerParamFile() As Boolean

        Dim strIDPickerParamFileName As String = m_jobParams.GetParam("IDPickerParamFile")
        Dim strIDPickerParamFilePath As String
        Dim strParamFileStoragePathKeyName As String

        If String.IsNullOrEmpty(strIDPickerParamFileName) Then
            strIDPickerParamFileName = DEFAULT_IDPICKER_PARAM_FILE_NAME
        End If

        strParamFileStoragePathKeyName = clsGlobal.STEPTOOL_PARAMFILESTORAGEPATH_PREFIX & "IDPicker"
        strIDPickerParamFilePath = m_mgrParams.GetParam(strParamFileStoragePathKeyName)
        If String.IsNullOrEmpty(strIDPickerParamFilePath) Then
            strIDPickerParamFilePath = "\\gigasax\dms_parameter_Files\IDPicker"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Parameter '" & strParamFileStoragePathKeyName & "' is not defined (obtained using V_Pipeline_Step_Tools_Detail_Report in the Broker DB); will assume: " & strIDPickerParamFilePath)
        End If

        If Not CopyFileToWorkDir(strIDPickerParamFileName, strIDPickerParamFilePath, m_WorkingDir) Then
            'Errors were reported in function call, so just return
            Return False
        End If

        ' Store the param file name so that we can load later
        m_jobParams.AddAdditionalParameter("JobParameters", IDPICKER_PARAM_FILENAME_LOCAL, strIDPickerParamFileName)

        Return True

    End Function

    Protected Function RetrieveMASICFilesWrapper() As IJobParams.CloseOutType

        Dim retrievalAttempts = 0

        While retrievalAttempts < 2

            retrievalAttempts += 1
            If Not RetrieveMASICFiles(m_DatasetName) Then
                Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
            End If

            If m_MyEMSLUtilities.FilesToDownload.Count = 0 Then
                Exit While
            Else
                If m_MyEMSLUtilities.ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders) Then
                    Exit While
                Else
                    ' Look for the MASIC files on the Samba share
                    MyBase.DisableMyEMSLSearch()
                End If
            End If

        End While

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    ''' <summary>
    ''' Retrieve the MASIC ScanStats.txt and ScanStatsEx.txt files
    ''' </summary>
    ''' <param name="strDatasetName"></param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Protected Function RetrieveMASICFiles(strDatasetName As String) As Boolean

        If Not RetrieveScanStatsFiles(False) Then
            ' _ScanStats.txt file not found
            ' If processing a .Raw file or .UIMF file then we can create the file using the MSFileInfoScanner
            If Not GenerateScanStatsFile() Then
                ' Error message should already have been logged and stored in m_message
                Return False
            End If
        Else
            If m_DebugLevel >= 1 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Retrieved MASIC ScanStats and ScanStatsEx files")
            End If
        End If

        m_jobParams.AddResultFileToSkip(strDatasetName & SCAN_STATS_FILE_SUFFIX)
        m_jobParams.AddResultFileToSkip(strDatasetName & SCAN_STATS_EX_FILE_SUFFIX)
        Return True

    End Function

    ''' <summary>
    ''' Determines the files that need to be copied to the work directory, based on the result type
    ''' </summary>
    ''' <param name="eResultType">PHRP result type (Seqest, X!Tandem, etc.)</param>
    ''' <param name="strDatasetName">Dataset name</param>
    ''' <returns>A generic list with the filenames to find.  The Boolean value is True if the file is Required, false if not required</returns>
    ''' <remarks></remarks>
    Protected Function GetPHRPFileNames(eResultType As clsPHRPReader.ePeptideHitResultType, strDatasetName As String) As SortedList(Of String, Boolean)

        Dim lstFileNamesToGet As SortedList(Of String, Boolean)
        lstFileNamesToGet = New SortedList(Of String, Boolean)

        Dim synFileName As String
        synFileName = clsPHRPReader.GetPHRPSynopsisFileName(eResultType, strDatasetName)

        lstFileNamesToGet.Add(synFileName, True)
        lstFileNamesToGet.Add(clsPHRPReader.GetPHRPModSummaryFileName(eResultType, strDatasetName), False)
        lstFileNamesToGet.Add(clsPHRPReader.GetPHRPResultToSeqMapFileName(eResultType, strDatasetName), True)
        lstFileNamesToGet.Add(clsPHRPReader.GetPHRPSeqInfoFileName(eResultType, strDatasetName), True)
        lstFileNamesToGet.Add(clsPHRPReader.GetPHRPSeqToProteinMapFileName(eResultType, strDatasetName), True)
        lstFileNamesToGet.Add(clsPHRPReader.GetPHRPPepToProteinMapFileName(eResultType, strDatasetName), False)

        If eResultType <> clsPHRPReader.ePeptideHitResultType.MODa And
           eResultType <> clsPHRPReader.ePeptideHitResultType.MODPlus Then
            lstFileNamesToGet.Add(clsPHRPReader.GetMSGFFileName(synFileName), True)
        End If

        Dim strToolVersionFile As String = clsPHRPReader.GetToolVersionInfoFilename(eResultType)
        Dim strToolNameForScript As String = m_jobParams.GetJobParameter("ToolName", "")
        If eResultType = clsPHRPReader.ePeptideHitResultType.MSGFDB And strToolNameForScript = "MSGFPlus_IMS" Then
            ' PeptideListToXML expects the ToolVersion file to be named "Tool_Version_Info_MSGFDB.txt"
            ' However, this is the MSGFPlus_IMS script, so the file is currently "Tool_Version_Info_MSGFPlus_IMS.txt"
            ' We'll copy the current file locally, then rename it to the expected name
            Const strOriginalName = "Tool_Version_Info_MSGFPlus_IMS.txt"
            mInputFileRenames.Add(strOriginalName, strToolVersionFile)
            strToolVersionFile = String.Copy(strOriginalName)
        End If

        lstFileNamesToGet.Add(strToolVersionFile, True)

        Return lstFileNamesToGet

    End Function

End Class
