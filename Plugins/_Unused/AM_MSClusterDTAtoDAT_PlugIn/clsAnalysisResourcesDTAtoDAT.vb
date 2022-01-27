Option Strict On

Imports AnalysisManagerBase

Public Class clsAnalysisResourcesDTAtoDAT
    Inherits clsAnalysisResources

    Public Overrides Function GetResources() As IJobParams.CloseOutType


        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Getting resources")

        ' Retrieve the _DTA.txt file
        ' Note that if the file was found in MyEMSL then RetrieveDtaFiles will auto-call ProcessMyEMSLDownloadQueue to download the file
        If Not RetrieveDtaFiles() Then
            'Errors were reported in function call, so just return
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        If Not MyBase.ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders) Then
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        'Add all the extensions of the files to delete after run
        m_JobParams.AddResultFileExtensionToSkip("_dta.zip") 'Zipped DTA
        m_JobParams.AddResultFileExtensionToSkip("_dta.txt") 'Unzipped, concatenated DTA

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

End Class
