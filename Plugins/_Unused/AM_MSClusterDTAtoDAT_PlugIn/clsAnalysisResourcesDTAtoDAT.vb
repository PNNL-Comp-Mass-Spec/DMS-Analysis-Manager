Option Strict On

Imports AnalysisManagerBase
Imports AnalysisManagerBase.AnalysisTool
Imports AnalysisManagerBase.JobConfig
Imports PRISM.Logging

Public Class clsAnalysisResourcesDTAtoDAT
    Inherits AnalysisResources

    Public Overrides Function GetResources() As CloseOutType


        LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.INFO, "Getting resources")

        ' Retrieve the _DTA.txt file
        ' Note that if the file was found in MyEMSL then RetrieveDtaFiles will auto-call ProcessMyEMSLDownloadQueue to download the file
        If Not FileSearchTool.RetrieveDtaFiles() Then
            'Errors were reported in function call, so just return
            Return CloseOutType.CLOSEOUT_FAILED
        End If

        If Not MyBase.ProcessMyEMSLDownloadQueue(mWorkDir, MyEMSLReader.Downloader.DownloadLayout.FlatNoSubdirectories) Then
            Return CloseOutType.CLOSEOUT_FAILED
        End If

        'Add all the extensions of the files to delete after run
        mJobParams.AddResultFileExtensionToSkip("_dta.zip") 'Zipped DTA
        mJobParams.AddResultFileExtensionToSkip("_dta.txt") 'Unzipped, concatenated DTA

        Return CloseOutType.CLOSEOUT_SUCCESS

    End Function

End Class
