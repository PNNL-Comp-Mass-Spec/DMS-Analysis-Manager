'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Created 07/16/2014
'
'*********************************************************************************************************

Option Strict On

Imports AnalysisManagerBase

Public Class clsAnalysisResourcesPBFGenerator
    Inherits clsAnalysisResources
    
    Public Overrides Function GetResources() As AnalysisManagerBase.IJobParams.CloseOutType

        ' Retrieve shared resources, including the JobParameters file from the previous job step
        Dim result = GetSharedResources()
        If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            Return result
        End If

        If Not RetrieveInstrumentData() Then
            Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
        End If

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    Protected Function RetrieveInstrumentData() As Boolean

        Dim currentTask As String = "Initializing"

        Try

            Dim rawDataType As String = m_jobParams.GetJobParameter("RawDataType", "")
            Dim eRawDataType = GetRawDataType(rawDataType)

            If eRawDataType = eRawDataTypeConstants.ThermoRawFile Then
                m_jobParams.AddResultFileExtensionToSkip(DOT_RAW_EXTENSION)
            Else
                m_message = "PbfGen presently only supports Thermo .Raw files"
                Return False
            End If

            currentTask = "Retrieve intrument data"

            ' Retrieve the instrument data file
            If Not RetrieveSpectra(rawDataType) Then
                If String.IsNullOrEmpty(m_message) Then
                    m_message = "Error retrieving instrument data file"
                End If

                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisResourcesPBFGenerator.GetResources: " & m_message)
                Return False
            End If

            currentTask = "Process MyEMSL Download Queue"

            If Not MyBase.ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders) Then
                Return False
            End If

            Threading.Thread.Sleep(500)

            Return True

        Catch ex As Exception
            m_message = "Exception in RetrieveInstrumentData: " & ex.Message
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & "; task = " & currentTask & "; " & clsGlobal.GetExceptionStackTrace(ex))
            Return False
        End Try

    End Function

End Class
