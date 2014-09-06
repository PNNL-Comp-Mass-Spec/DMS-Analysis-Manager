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

		' Retrieve the .mzXML file for this dataset
		' Do not use RetrieveMZXmlFile since that function looks for any valid MSXML_Gen folder for this dataset
		' Instead, use RetrieveCachedMzXMLFile 

		Dim errorMessage = String.Empty
		Dim fileMissingFromCache = False
		Const unzipFile = True

		Dim success = RetrieveCachedMzXMLFile(unzipFile, errorMessage, fileMissingFromCache)
		If Not success Then
			If fileMissingFromCache Then
				If String.IsNullOrEmpty(errorMessage) Then
					errorMessage = "Cached .mzXML file does not exist; will re-generate it"
				End If

				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, errorMessage)
				Return IJobParams.CloseOutType.CLOSEOUT_MZML_FILE_NOT_IN_CACHE
			End If

			If String.IsNullOrEmpty(errorMessage) Then
				errorMessage = "Unknown error in RetrieveCachedMzXMLFile"
			End If
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, errorMessage)
			Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
		End If

		' Make sure we don't move the .mzXML file into the results folder
		m_jobParams.AddResultFileExtensionToSkip(DOT_MZXML_EXTENSION)

		If Not MyBase.ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders) Then
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

End Class
