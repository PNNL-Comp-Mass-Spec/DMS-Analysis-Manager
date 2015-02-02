'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Created 01/30/2015
'
'*********************************************************************************************************

Option Strict On

Imports AnalysisManagerBase

Public Class clsAnalysisResourcesProMex
    Inherits clsAnalysisResources

    Public Overrides Sub Setup(ByRef mgrParams As IMgrParams, ByRef jobParams As IJobParams)
        MyBase.Setup(mgrParams, jobParams)
        SetOption(clsGlobal.eAnalysisResourceOptions.OrgDbRequired, True)
    End Sub

    Public Overrides Sub Setup(mgrParams As IMgrParams, jobParams As IJobParams, statusTools As IStatusFile)
        MyBase.Setup(mgrParams, jobParams, statusTools)
        SetOption(clsGlobal.eAnalysisResourceOptions.OrgDbRequired, True)
    End Sub

    Public Overrides Function GetResources() As IJobParams.CloseOutType

        ' Get the ProMex parameter file

        Dim paramFileStoragePathKeyName As String
        Dim proMexParmFileStoragePath As String
        paramFileStoragePathKeyName = clsGlobal.STEPTOOL_PARAMFILESTORAGEPATH_PREFIX & "ProMex"

        proMexParmFileStoragePath = m_mgrParams.GetParam(paramFileStoragePathKeyName)
        If String.IsNullOrEmpty(proMexParmFileStoragePath) Then
            proMexParmFileStoragePath = "C:\DMS_Programs\ProMex"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN, "Parameter '" & paramFileStoragePathKeyName & "' is not defined (obtained using V_Pipeline_Step_Tools_Detail_Report in the Broker DB); will assume: " & proMexParmFileStoragePath)
        End If

        Dim paramFileName = m_jobParams.GetParam("ProMexParamFile")
        If String.IsNullOrEmpty(paramFileName) Then
            m_message = "Parameter 'ProMexParamFile' is not defined; see the settings file for this job"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        If Not RetrieveFile(paramFileName, proMexParmFileStoragePath) Then
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        ' Retrieve the PBF file
        Dim eResult = RetrievePBFFile()
        If eResult <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            Return eResult
        End If

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    Protected Function RetrievePBFFile() As IJobParams.CloseOutType

        Dim currentTask As String = "Initializing"

        Try
            ' Retrieve the .pbf file from the MSXml cache folder

            currentTask = "RetrievePBFFile"

            Dim eResult = GetPBFFile()
            If eResult <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                Return eResult
            End If

            m_jobParams.AddResultFileExtensionToSkip(DOT_PBF_EXTENSION)

            Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

        Catch ex As Exception
            m_message = "Exception in RetrievePBFFile: " & ex.Message
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & "; task = " & currentTask & "; " & clsGlobal.GetExceptionStackTrace(ex))
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

    End Function

End Class
