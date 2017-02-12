Option Strict On

Imports AnalysisManagerBase
Imports System.IO

Public Class clsAnalysisResourcesMSAlignQuant
    Inherits clsAnalysisResources

    Public Const MSALIGN_RESULT_TABLE_SUFFIX As String = "_MSAlign_ResultTable.txt"

    Public Overrides Function GetResources() As CloseOutType

        ' Retrieve shared resources, including the JobParameters file from the previous job step
        Dim result = GetSharedResources()
        If result <> CloseOutType.CLOSEOUT_SUCCESS Then
            Return result
        End If

        ' Retrieve the MSAlign_Quant parameter file
        ' For example, MSAlign_Quant_Workflow_2012-07-25

        Dim strParamFileStoragePathKeyName = clsGlobal.STEPTOOL_PARAMFILESTORAGEPATH_PREFIX & "MSAlign_Quant"
        Dim strParamFileStoragePath = m_mgrParams.GetParam(strParamFileStoragePathKeyName)
        If strParamFileStoragePath Is Nothing OrElse strParamFileStoragePath.Length = 0 Then
            strParamFileStoragePath = "\\gigasax\DMS_Parameter_Files\DeconToolsWorkflows"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Parameter '" & strParamFileStoragePathKeyName & "' is not defined (obtained using V_Pipeline_Step_Tools_Detail_Report in the Broker DB); will assume: " & strParamFileStoragePath)
        End If

        Dim strParamFileName As String = m_jobParams.GetJobParameter("MSAlignQuantParamFile", String.Empty)
        If String.IsNullOrEmpty(strParamFileName) Then
            m_message = clsAnalysisToolRunnerBase.NotifyMissingParameter(m_jobParams, "MSAlignQuantParamFile")
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
            Return CloseOutType.CLOSEOUT_NO_PARAM_FILE
        End If

        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Getting data files")

        If Not RetrieveFile(strParamFileName, strParamFileStoragePath) Then
            Return CloseOutType.CLOSEOUT_NO_PARAM_FILE
        End If

        ' Retrieve the MSAlign results for this job
        Dim strMSAlignResultsTable As String
        strMSAlignResultsTable = m_DatasetName & MSALIGN_RESULT_TABLE_SUFFIX
        If Not FindAndRetrieveMiscFiles(strMSAlignResultsTable, False) Then
            'Errors were reported in function call, so just return
            Return CloseOutType.CLOSEOUT_FILE_NOT_FOUND
        End If
        m_jobParams.AddResultFileToSkip(strMSAlignResultsTable)


        ' Get the instrument data file
        Dim strRawDataType As String = m_jobParams.GetParam("RawDataType")

        Select Case strRawDataType.ToLower
            Case RAW_DATA_TYPE_DOT_RAW_FILES, RAW_DATA_TYPE_BRUKER_FT_FOLDER, RAW_DATA_TYPE_DOT_D_FOLDERS
                If RetrieveSpectra(strRawDataType) Then

                    If Not MyBase.ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders) Then
                        Return CloseOutType.CLOSEOUT_FAILED
                    End If

                    ' Confirm that the .Raw or .D folder was actually copied locally
                    If strRawDataType.ToLower() = RAW_DATA_TYPE_DOT_RAW_FILES Then
                        If Not File.Exists(Path.Combine(m_WorkingDir, DatasetName & DOT_RAW_EXTENSION)) Then
                            m_message = "Thermo .Raw file not successfully copied to WorkDir; likely a timeout error"
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsDtaGenResources.GetResources: " & m_message)
                            Return CloseOutType.CLOSEOUT_FAILED
                        End If
                        m_jobParams.AddResultFileExtensionToSkip(DOT_RAW_EXTENSION)  'Raw file

                    ElseIf strRawDataType.ToLower() = RAW_DATA_TYPE_BRUKER_FT_FOLDER Then
                        If Not Directory.Exists(Path.Combine(m_WorkingDir, DatasetName & DOT_D_EXTENSION)) Then
                            m_message = "Bruker .D folder not successfully copied to WorkDir; likely a timeout error"
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsDtaGenResources.GetResources: " & m_message)
                            Return CloseOutType.CLOSEOUT_FAILED
                        End If
                    End If

                Else
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsDtaGenResources.GetResources: Error occurred retrieving spectra.")
                    Return CloseOutType.CLOSEOUT_FAILED
                End If
            Case Else
                m_message = "Dataset type " & strRawDataType & " is not supported"
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsDtaGenResources.GetResources: " & m_message & "; must be " & RAW_DATA_TYPE_DOT_RAW_FILES & " or " & RAW_DATA_TYPE_BRUKER_FT_FOLDER)
                Return CloseOutType.CLOSEOUT_FAILED
        End Select

        If Not MyBase.ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders) Then
            Return CloseOutType.CLOSEOUT_FAILED
        End If

        Return CloseOutType.CLOSEOUT_SUCCESS

    End Function


End Class
