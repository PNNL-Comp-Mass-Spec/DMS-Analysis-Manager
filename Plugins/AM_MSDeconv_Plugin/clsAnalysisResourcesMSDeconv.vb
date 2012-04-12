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

		Dim FileToGet As String

		' Make sure the machine has enough free memory to run MSDeconv
		If Not ValidateFreeMemorySize("MSDeconvJavaMemorySize", "MSDeconv") Then
			m_message = "Not enough free memory to run MSDeconv"
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Getting mzXML file")

		' Retrieve the .mzXML file for this dataset
		' Do not use RetrieveMZXmlFile since that function looks for any valid MSXML_Gen folder for this dataset
		' Instead, use FindAndRetrieveMiscFiles 

		' Note that capitalization matters for the extension; it must be .mzXML
		FileToGet = m_jobParams.GetParam("DatasetNum") & ".mzXML"
		If Not FindAndRetrieveMiscFiles(FileToGet, False) Then
			'Errors were reported in function call, so just return
			Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
		End If
		m_jobParams.AddResultFileToSkip(FileToGet)

		' Make sure we don't move the .mzXML file into the results folder
		m_JobParams.AddResultFileExtensionToSkip(".mzXML")

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

End Class
