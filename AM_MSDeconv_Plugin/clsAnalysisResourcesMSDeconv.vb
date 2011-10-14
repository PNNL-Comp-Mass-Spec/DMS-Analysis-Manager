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

	Public Overrides Function GetResources() As AnalysisManagerBase.IJobParams.CloseOutType

		Dim FileToGet As String

		'Clear out list of files to delete or keep when packaging the results
		clsGlobal.ResetFilesToDeleteOrKeep()

		' Make sure the machine has enough free memory to run MSDeconv
		If Not ValidateFreeMemorySize("MSDeconvJavaMemorySize", "MSDeconv") Then
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Getting mzXML file")

		' Retrieve the .mzXML file for this dataset
		' Do not use RetrieveMZXmlFile since that function looks for folder any valid MSXML_Gen folder for this dataset
		' Instead, use FindAndRetrieveMiscFiles 

		' Note that capitalization matters for the extension; it must be .mzXML
		FileToGet = m_jobParams.GetParam("DatasetNum") & ".mzXML"
		If Not FindAndRetrieveMiscFiles(FileToGet, False) Then
			'Errors were reported in function call, so just return
			Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
		End If
		clsGlobal.FilesToDelete.Add(FileToGet)

		' Make sure we don't move the .mzXML file into the results folder
		clsGlobal.m_FilesToDeleteExt.Add(".mzXML")

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

End Class
