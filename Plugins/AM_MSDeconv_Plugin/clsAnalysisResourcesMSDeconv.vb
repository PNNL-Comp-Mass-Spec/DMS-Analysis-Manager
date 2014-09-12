'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Created 10/12/2011
'
'*********************************************************************************************************

Option Strict On

Imports AnalysisManagerBase

Public Class clsAnalysisResourcesMSDeconv
	Inherits clsAnalysisResources

	Public Overrides Function GetResources() As IJobParams.CloseOutType


		' Make sure the machine has enough free memory to run MSDeconv
		If Not ValidateFreeMemorySize("MSDeconvJavaMemorySize", "MSDeconv") Then
			m_message = "Not enough free memory to run MSDeconv"
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Getting mzXML file")

		'Dim eResult = GetMzXMLFile()
		'If eResult <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
		'	Return eResult
		'End If

		Dim errorMessage = String.Empty
		Dim fileMissingFromCache = False
		Const unzipFile = True

		Dim success = RetrieveCachedMzXMLFile(unzipFile, errorMessage, fileMissingFromCache)
		If Not success Then
			Return HandleMsXmlRetrieveFailure(fileMissingFromCache, errorMessage, DOT_MZXML_EXTENSION)
		End If

		' Make sure we don't move the .mzXML file into the results folder
		m_jobParams.AddResultFileExtensionToSkip(DOT_MZXML_EXTENSION)

		If Not MyBase.ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders) Then
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

End Class
