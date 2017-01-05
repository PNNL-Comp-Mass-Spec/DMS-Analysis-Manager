'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Created 10/12/2011
'
'*********************************************************************************************************

Option Strict On

Imports AnalysisManagerBase

Public Class clsAnalysisResourcesMSAlignHistone
    Inherits clsAnalysisResources

    Public Const MSDECONV_MSALIGN_FILE_SUFFIX As String = "_msdeconv.msalign"

    Public Overrides Sub Setup(mgrParams As IMgrParams, jobParams As IJobParams, statusTools As IStatusFile, myEMSLUtilities As clsMyEMSLUtilities)
        MyBase.Setup(mgrParams, jobParams, statusTools, myEmslUtilities)
        SetOption(clsGlobal.eAnalysisResourceOptions.OrgDbRequired, True)
    End Sub

    Public Overrides Function GetResources() As IJobParams.CloseOutType

        ' Retrieve shared resources, including the JobParameters file from the previous job step
        Dim result = GetSharedResources()
        If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            Return result
        End If

        ' Make sure the machine has enough free memory to run MSAlign
        If Not ValidateFreeMemorySize("MSAlignJavaMemorySize", "MSAlign") Then
            m_message = "Not enough free memory to run MSAlign"
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        ' Retrieve param file
        If Not RetrieveFile( _
           m_jobParams.GetParam("ParmFileName"), _
           m_jobParams.GetParam("ParmFileStoragePath")) _
        Then Return IJobParams.CloseOutType.CLOSEOUT_FAILED

        ' Retrieve Fasta file
        If Not RetrieveOrgDB(m_mgrParams.GetParam("orgdbdir")) Then Return IJobParams.CloseOutType.CLOSEOUT_FAILED

        ' Retrieve the MSAlign file
        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Getting data files")
        dim fileToGet = m_DatasetName & MSDECONV_MSALIGN_FILE_SUFFIX
        If Not FindAndRetrieveMiscFiles(FileToGet, False) Then
            'Errors were reported in function call, so just return
            Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
        End If
        m_jobParams.AddResultFileToSkip(fileToGet)

        If Not MyBase.ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders) Then
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

End Class
