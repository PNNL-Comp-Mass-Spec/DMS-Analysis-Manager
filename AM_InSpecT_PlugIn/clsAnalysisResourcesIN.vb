Imports AnalysisManagerBase
Imports PRISM.Logging
Imports System.IO
Imports System
Imports ParamFileGenerator.MakeParams
Imports AnalysisManagerMSMSResourceBase

Public Class clsAnalysisResourcesIN
    Inherits clsAnalysisResourcesMSMS

    Protected Overrides Function RetrieveParamFile(ByVal ParamFileName As String, _
                    ByVal ParamFilePath As String, ByVal WorkDir As String) As Boolean

        Dim result As Boolean = True

        m_logger.PostEntry("Getting param file", ILogger.logMsgType.logNormal, True)

        'Uses ParamFileGenerator dll provided by Ken Auberry to get mod_defs and mass correction files
        'NOTE: ParamFilePath isn't used in this override, but is needed in parameter list for compatability
        Dim ParFileGen As IGenerateFile = New clsMakeParameterFile

        Try
            result = ParFileGen.MakeFile(ParamFileName, SetBioworksVersion("inspect"), _
            Path.Combine(m_mgrParams.GetParam("orgdbdir"), m_jobParams.GetParam("generatedFastaName")), _
            WorkDir, m_mgrParams.GetParam("connectionstring"))
        Catch Ex As Exception
            Dim Msg As String = "clsAnalysisResourcesIN.RetrieveParamFile(), exception generating param file: " & Ex.Message
            m_logger.PostEntry(Msg, ILogger.logMsgType.logError, True)
            Return False
        End Try

        If Not result Then
            m_logger.PostEntry("Error converting param file: " & ParFileGen.LastError, ILogger.logMsgType.logError, True)
            Return False
        End If

        RetrieveParamFile = result

    End Function

End Class
