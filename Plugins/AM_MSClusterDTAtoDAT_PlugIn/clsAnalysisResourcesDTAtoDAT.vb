' Last modified 06/15/2009 JDS - Added logging using log4net
Option Strict On

Imports AnalysisManagerBase

Public Class clsAnalysisResourcesDTAtoDAT
    Inherits clsAnalysisResources

    Public Overrides Function GetResources() As AnalysisManagerBase.IJobParams.CloseOutType


        'Clear out list of files to delete or keep when packaging the results
        clsGlobal.ResetFilesToDeleteOrKeep()

        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Getting resources")

        ' Note: DTAtoDAT does not use a parameter file

        ''Dim strParamFileName As String
        ''strParamFileName = m_jobParams.GetParam("ParmFileName")

        ''If Not (strParamFileName = "na" OrElse strParamFileName = "(na)" OrElse strParamFileName = "") Then
        ''    'Retrieve param file
        ''    If Not RetrieveFile( _
        ''     m_jobParams.GetParam("ParmFileName"), _
        ''     m_jobParams.GetParam("ParmFileStoragePath"), _
        ''     m_mgrParams.GetParam("workdir")) _
        ''    Then Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        ''End If

        'Retrieve unzipped dta files (do not unconcatenate)
        If Not RetrieveDtaFiles(False) Then
            'Errors were reported in function call, so just return
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        'Add all the extensions of the files to delete after run
        clsGlobal.m_FilesToDeleteExt.Add("_dta.zip") 'Zipped DTA
        clsGlobal.m_FilesToDeleteExt.Add("_dta.txt") 'Unzipped, concatenated DTA

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

End Class
