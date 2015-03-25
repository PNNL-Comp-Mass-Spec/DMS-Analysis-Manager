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

        Dim paramFileName As String

        ' If this is a ProMex script, then the ProMex parameter file name is tracked as the job's parameter file
        ' Otherwise, for MSPathFinder scripts, the ProMex parameter file is defined in the Job's settings file, and is thus accessible as job parameter ProMexParamFile

        Dim toolName = m_jobParams.GetParam("ToolName")
        Dim proMexScript = toolName.StartsWith("ProMex", StringComparison.CurrentCultureIgnoreCase)
        Dim proMexBruker = IsProMexBrukerJob(m_jobParams)

        If proMexScript Then

            paramFileName = m_jobParams.GetJobParameter("ParmFileName", "")

            If String.IsNullOrEmpty(paramFileName) Then
                m_message = "Job Parameter File name is empty"
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            m_jobParams.AddAdditionalParameter("StepParameters", "ProMexParamFile", paramFileName)

        Else
            paramFileName = m_jobParams.GetParam("ProMexParamFile")

            If String.IsNullOrEmpty(paramFileName) Then
                ' Settings file does not contain parameter ProMexParamFile
                m_message = "Parameter 'ProMexParamFile' is not defined in the settings file for this job"
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

        End If

        If Not RetrieveFile(paramFileName, proMexParmFileStoragePath) Then
            If proMexScript Then
                m_message = clsGlobal.AppendToComment(m_message, "see the parameter file name defined for this Analysis Job")
            Else
                m_message = clsGlobal.AppendToComment(m_message, "see the Analysis Job's settings file, entry ProMexParamFile")
            End If
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        Dim eResult As IJobParams.CloseOutType

        If proMexBruker Then
            ' Retrieve the mzML file
            ' Note that ProMex will create a PBF file using the .mzML file
            eResult = RetrieveMzMLFile()
        Else
            ' Retrieve the PBF file
            eResult = RetrievePBFFile()
        End If

        If eResult <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            Return eResult
        End If

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    ''' <summary>
    ''' Returns True if this is a ProMex_Bruker job
    ''' </summary>
    ''' <param name="jobParams"></param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public Shared Function IsProMexBrukerJob(ByVal jobParams As IJobParams) As Boolean
        Dim toolName = jobParams.GetParam("ToolName")
        Dim proMexBruker = toolName.StartsWith("ProMex_Bruker", StringComparison.CurrentCultureIgnoreCase)

        Return proMexBruker

    End Function

    Protected Function RetrieveMzMLFile() As IJobParams.CloseOutType

        Dim currentTask As String = "Initializing"

        Try
            ' Retrieve the .mzML file from the MSXml cache folder

            currentTask = "RetrieveMzMLFile"

            Dim eResult = GetMzMLFile()
            If eResult <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
                Return eResult
            End If

            m_jobParams.AddResultFileExtensionToSkip(DOT_MZML_EXTENSION)

            Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

        Catch ex As Exception
            m_message = "Exception in RetrieveMzMLFile: " & ex.Message
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & "; task = " & currentTask & "; " & clsGlobal.GetExceptionStackTrace(ex))
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

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
