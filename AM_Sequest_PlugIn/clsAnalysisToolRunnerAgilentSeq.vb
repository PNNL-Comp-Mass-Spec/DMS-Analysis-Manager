' Last modified 06/15/2009 JDS - Added logging using log4net
Imports System.IO
Imports AnalysisManagerBase

Public Class clsAnalysisToolRunnerAgilentSeq
	Inherits clsAnalysisToolRunnerSeqBase

	Public Sub New()
	End Sub

    Public Overrides Sub Setup(ByVal mgrParams As IMgrParams, ByVal jobParams As IJobParams, _
     ByVal StatusTools As IStatusFile)

        MyBase.Setup(mgrParams, jobParams, StatusTools)
        If m_DebugLevel > 3 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerSeq.Setup()")
        End If

    End Sub

	Protected Function DeleteDataFile() As IJobParams.CloseOutType

		'Deletes the data files (.mgf and .cdf) from the working directory
		Dim FoundFiles() As String
		Dim MyFile As String

		Try
			'Delete the .mgf file
			FoundFiles = Directory.GetFiles(m_WorkDir, "*.mgf")
			For Each MyFile In FoundFiles
				DeleteFileWithRetries(MyFile)
			Next
			'Delete the .cdf file, if present
			FoundFiles = Directory.GetFiles(m_WorkDir, "*.cdf")
			For Each MyFile In FoundFiles
				DeleteFileWithRetries(MyFile)
			Next
		Catch Err As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error deleting raw data file(s), job " & m_JobNum & ", step " & m_jobParams.GetParam("Step") & Err.Message)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

	End Function

End Class
