Option Strict On

Imports AnalysisManagerBase
Imports System.IO

Public Class clsAnalysisResourcesMzRefinery
	Inherits clsAnalysisResources

#Region "Methods"

	Public Overrides Sub Setup(ByRef mgrParams As IMgrParams, ByRef jobParams As IJobParams)
		MyBase.Setup(mgrParams, jobParams)
		SetOption(clsGlobal.eAnalysisResourceOptions.OrgDbRequired, True)
	End Sub

	Public Overrides Sub Setup(mgrParams As IMgrParams, jobParams As IJobParams, statusTools As IStatusFile)
		MyBase.Setup(mgrParams, jobParams, statusTools)
		SetOption(clsGlobal.eAnalysisResourceOptions.OrgDbRequired, True)
	End Sub

	''' <summary>
	''' Retrieves files necessary for running MzRefinery
	''' </summary>
	''' <returns>IJobParams.CloseOutType indicating success or failure</returns>
	''' <remarks></remarks>
	Public Overrides Function GetResources() As IJobParams.CloseOutType

		Dim currentTask As String = "Initializing"

		Try

			Dim mzRefParamFile = m_jobParams.GetJobParameter("MzRefParamFile", String.Empty)
			If String.IsNullOrEmpty(mzRefParamFile) Then
				LogError("MzRefParamFile parameter is empty")
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			currentTask = "Get Input file"

			Dim eResult = GetMzMLFile()
			If eResult <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
				Return eResult
			End If

			' Retrieve the Fasta file
			Dim localOrgDbFolder = m_mgrParams.GetParam("orgdbdir")

			currentTask = "RetrieveOrgDB to " & localOrgDbFolder

			If Not RetrieveOrgDB(localOrgDbFolder) Then
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			' Retrieve the Mz Refinery parameter file
			currentTask = "Retrieve the Mz Refinery parameter file " & mzRefParamFile

			Const paramFileStoragePathKeyName As String = clsGlobal.STEPTOOL_PARAMFILESTORAGEPATH_PREFIX & "Mz_Refinery"

			Dim mzRefineryParmFileStoragePath = m_mgrParams.GetParam(paramFileStoragePathKeyName)
			If String.IsNullOrWhiteSpace(mzRefineryParmFileStoragePath) Then
				mzRefineryParmFileStoragePath = "\\gigasax\dms_parameter_Files\MzRefinery"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Parameter '" & paramFileStoragePathKeyName & "' is not defined (obtained using V_Pipeline_Step_Tools_Detail_Report in the Broker DB); will assume: " & mzRefineryParmFileStoragePath)
			End If

			' Retrieve param file
			If Not RetrieveFile(mzRefParamFile, mzRefineryParmFileStoragePath) Then
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

		Catch ex As Exception
			m_message = "Exception in GetResources: " & ex.Message
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & "; task = " & currentTask & "; " & clsGlobal.GetExceptionStackTrace(ex))
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

#End Region

End Class
