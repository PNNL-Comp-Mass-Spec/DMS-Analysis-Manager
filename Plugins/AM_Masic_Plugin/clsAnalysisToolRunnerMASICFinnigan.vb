Option Strict On

'*********************************************************************************************************
' Written by Matt Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2006, Battelle Memorial Institute
' Created 06/07/2006
'
' Last modified 01/15/2008
' Last modified 06/15/2009 JDS - Added logging using log4net
'*********************************************************************************************************

Imports AnalysisManagerBase
Imports AnalysisManagerBase.clsGlobal

Public Class clsAnalysisToolRunnerMASICFinnigan
	Inherits clsAnalysisToolRunnerMASICBase

	'*********************************************************************************************************
	'Derived class for performing MASIC analysis on Finnigan datasets
	'*********************************************************************************************************

	Public Sub New()
	End Sub

	Protected Overrides Function RunMASIC() As IJobParams.CloseOutType

		Dim strParameterFileName As String
        Dim strParameterFilePath As String

        Dim strRawFileName As String
        Dim strInputFilePath As String

		strParameterFileName = m_JobParams.GetParam("parmFileName")

		If Not strParameterFileName Is Nothing AndAlso strParameterFileName.Trim.ToLower <> "na" Then
            strParameterFilePath = System.IO.Path.Combine(m_WorkDir, m_jobParams.GetParam("parmFileName"))
		Else
			strParameterFilePath = String.Empty
		End If

        ' Determine the path to the .Raw file
        strRawFileName = m_Dataset & ".raw"
        strInputFilePath = AnalysisManagerBase.clsAnalysisResources.ResolveStoragePath(m_WorkDir, strRawFileName)

        If strInputFilePath Is Nothing OrElse strInputFilePath.Length = 0 Then
            ' Unable to resolve the file path
            m_ErrorMessage = "Could not find " & strRawFileName & " or " & strRawFileName & AnalysisManagerBase.clsAnalysisResources.STORAGE_PATH_INFO_FILE_SUFFIX & " in the working folder; unable to run MASIC"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_ErrorMessage)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        Return MyBase.StartMASICAndWait(strInputFilePath, m_WorkDir, strParameterFilePath)

	End Function

	Protected Overrides Function DeleteDataFile() As IJobParams.CloseOutType

		'Deletes the .raw file from the working directory
		Dim FoundFiles() As String
		Dim MyFile As String

		'Delete the .raw file
		Try
            FoundFiles = System.IO.Directory.GetFiles(m_WorkDir, "*.raw")
			For Each MyFile In FoundFiles
				DeleteFileWithRetries(MyFile)
			Next MyFile
			Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
		Catch Err As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error finding .raw files to delete, job " & m_JobNum)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

	End Function

End Class
