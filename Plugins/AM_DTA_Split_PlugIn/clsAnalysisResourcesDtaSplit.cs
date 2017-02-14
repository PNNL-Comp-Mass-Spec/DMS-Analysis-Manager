Option Strict On

Imports AnalysisManagerBase
Imports MyEMSLReader

Public Class clsAnalysisResourcesDtaSplit
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

        ' Retrieve the _DTA.txt file
        ' Note that if the file was found in MyEMSL then RetrieveDtaFiles will auto-call ProcessMyEMSLDownloadQueue to download the file
        If Not FileSearch.RetrieveDtaFiles() Then
            'Errors were reported in function call, so just return
            Return CloseOutType.CLOSEOUT_FAILED
        End If

        If Not MyBase.ProcessMyEMSLDownloadQueue(m_WorkingDir, Downloader.DownloadFolderLayout.FlatNoSubfolders) Then
            Return CloseOutType.CLOSEOUT_FAILED
        End If

        'Add all the extensions of the files to delete after run
        m_jobParams.AddResultFileExtensionToSkip("_dta.zip") 'Zipped DTA
        m_JobParams.AddResultFileExtensionToSkip("_dta.txt") 'Unzipped, concatenated DTA

        'All finished
        Return CloseOutType.CLOSEOUT_SUCCESS

    End Function
#End Region

End Class
