' Last modified 06/15/2009 JDS - Added logging using log4net
Option Strict On

Imports AnalysisManagerBase

Public Class clsAnalysisResourcesDTAtoDAT
    Inherits clsAnalysisResources

    Public Overrides Function GetResources() As IJobParams.CloseOutType


        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Getting resources")

        'Retrieve unzipped dta files (do not unconcatenate)
        If Not RetrieveDtaFiles(False) Then
            'Errors were reported in function call, so just return
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        'Add all the extensions of the files to delete after run
        m_JobParams.AddResultFileExtensionToSkip("_dta.zip") 'Zipped DTA
        m_JobParams.AddResultFileExtensionToSkip("_dta.txt") 'Unzipped, concatenated DTA

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

End Class
