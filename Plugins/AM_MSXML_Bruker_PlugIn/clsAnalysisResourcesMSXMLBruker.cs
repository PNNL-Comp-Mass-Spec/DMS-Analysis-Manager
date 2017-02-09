'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Created 03/30/2011
'
'*********************************************************************************************************

Option Strict On

Imports AnalysisManagerBase

Public Class clsAnalysisResourcesMSXMLBruker
    Inherits clsAnalysisResources


#Region "Methods"
    ''' <summary>
    ''' Retrieves files necessary for creating the .mzXML file
    ''' </summary>
    ''' <returns>CloseOutType indicating success or failure</returns>
    ''' <remarks></remarks>
    Public Overrides Function GetResources() As CloseOutType

        ' Retrieve shared resources, including the JobParameters file from the previous job step
        Dim result = GetSharedResources()
        If result <> CloseOutType.CLOSEOUT_SUCCESS Then
            Return result
        End If

        'Get input data file
        Dim strRawDataType As String = m_jobParams.GetParam("RawDataType")
        Dim eRawDataType = GetRawDataType(strRawDataType)

        Select Case eRawDataType
            Case eRawDataTypeConstants.BrukerFTFolder, eRawDataTypeConstants.BrukerTOFBaf
                ' This dataset type is acceptable
            Case Else
                m_message = "Dataset type " & strRawDataType & " is not supported"
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsDtaGenResources.GetResources: " & m_message & "; must be " & RAW_DATA_TYPE_BRUKER_FT_FOLDER & " or " & RAW_DATA_TYPE_BRUKER_TOF_BAF_FOLDER)
                Return CloseOutType.CLOSEOUT_FAILED

        End Select

        If Not RetrieveSpectra(strRawDataType) Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsDtaGenResources.GetResources: Error occurred retrieving spectra.")
            Return CloseOutType.CLOSEOUT_FAILED
        End If

        If Not MyBase.ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.SingleDataset) Then
            Return CloseOutType.CLOSEOUT_FAILED
        End If

        Return CloseOutType.CLOSEOUT_SUCCESS

    End Function
#End Region

End Class
