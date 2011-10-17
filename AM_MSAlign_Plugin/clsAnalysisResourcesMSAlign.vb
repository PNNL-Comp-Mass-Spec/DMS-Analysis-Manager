'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Created 10/12/2011
'
'*********************************************************************************************************

Option Strict On

Imports AnalysisManagerBase

Public Class clsAnalysisResourcesMSAlign
	Inherits clsAnalysisResources

	Public Const MSDECONV_MSALIGN_FILE_SUFFIX As String = "_msdeconv.msalign"

	Public Overrides Function GetResources() As AnalysisManagerBase.IJobParams.CloseOutType

		Dim FileToGet As String

		' Clear out list of files to delete or keep when packaging the results
		clsGlobal.ResetFilesToDeleteOrKeep()

		' Make sure the machine has enough free memory to run MSAlign
		If Not ValidateFreeMemorySize("MSAlignJavaMemorySize", "MSAlign") Then
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		' Retrieve param file
		If Not RetrieveFile( _
		   m_jobParams.GetParam("ParmFileName"), _
		   m_jobParams.GetParam("ParmFileStoragePath"), _
		   m_mgrParams.GetParam("workdir")) _
		Then Return IJobParams.CloseOutType.CLOSEOUT_FAILED

		' Retrieve Fasta file
		If Not RetrieveOrgDB(m_mgrParams.GetParam("orgdbdir")) Then Return IJobParams.CloseOutType.CLOSEOUT_FAILED

		' Retrieve the MSAlign file
		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Getting data files")
		FileToGet = m_jobParams.GetParam("DatasetNum") & MSDECONV_MSALIGN_FILE_SUFFIX
		If Not FindAndRetrieveMiscFiles(FileToGet, False) Then
			'Errors were reported in function call, so just return
			Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
		End If
		clsGlobal.FilesToDelete.Add(FileToGet)

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

End Class
