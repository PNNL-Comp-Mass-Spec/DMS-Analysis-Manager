Option Strict On

Imports AnalysisManagerBase

Public Class clsAnalysisResourcesMSAlignQuant
	Inherits clsAnalysisResources

	Public Const MSALIGN_RESULT_TABLE_SUFFIX As String = "_MSAlign_ResultTable.txt"

	Public Overrides Function GetResources() As IJobParams.CloseOutType

		' Retrieve the MSAlign_Quant parameter file
		' For example, MSAlign_Quant_Workflow_2012-07-25

		Dim strParamFileStoragePathKeyName As String
		Dim strParamFileStoragePath As String
		strParamFileStoragePathKeyName = clsGlobal.STEPTOOL_PARAMFILESTORAGEPATH_PREFIX & "MSAlign_Quant"

		strParamFileStoragePath = m_mgrParams.GetParam(strParamFileStoragePathKeyName)
		If strParamFileStoragePath Is Nothing OrElse strParamFileStoragePath.Length = 0 Then
			strParamFileStoragePath = "\\gigasax\DMS_Parameter_Files\DeconToolsWorkflows"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Parameter '" & strParamFileStoragePathKeyName & "' is not defined (obtained using V_Pipeline_Step_Tools_Detail_Report in the Broker DB); will assume: " & strParamFileStoragePath)
		End If

		Dim strParamFileName As String = m_jobParams.GetParam("MSAlignQuantParamFile")
		If String.IsNullOrEmpty(strParamFileName) Then
			m_message = "MSAlignQuantParamFile param file not defined in the settings file for this analysis job (" & m_jobParams.GetJobParameter("SettingsFileName", "??") & ")"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
			Return IJobParams.CloseOutType.CLOSEOUT_NO_PARAM_FILE
		End If

		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Getting data files")

		If Not RetrieveFile(strParamFileName, strParamFileStoragePath, m_WorkingDir) Then
			Return IJobParams.CloseOutType.CLOSEOUT_NO_PARAM_FILE
		End If

		' Retrieve the MSAlign results for this job
		Dim strMSAlignResultsTable As String
		strMSAlignResultsTable = m_jobParams.GetParam("DatasetNum") & MSALIGN_RESULT_TABLE_SUFFIX
		If Not FindAndRetrieveMiscFiles(strMSAlignResultsTable, False) Then
			'Errors were reported in function call, so just return
			Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
		End If
		m_jobParams.AddResultFileToSkip(strMSAlignResultsTable)


		' Get the instrument data file
		Dim strRawDataType As String = m_jobParams.GetParam("RawDataType")

		Select Case strRawDataType.ToLower
			Case RAW_DATA_TYPE_DOT_RAW_FILES, RAW_DATA_TYPE_BRUKER_FT_FOLDER
				If RetrieveSpectra(strRawDataType, m_mgrParams.GetParam("workdir")) Then
					m_jobParams.AddResultFileExtensionToSkip(clsAnalysisResources.DOT_RAW_EXTENSION)  'Raw file
				Else
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsDtaGenResources.GetResources: Error occurred retrieving spectra.")
					Return IJobParams.CloseOutType.CLOSEOUT_FAILED
				End If
			Case Else
				m_message = "Dataset type " & strRawDataType & " is not supported"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsDtaGenResources.GetResources: " & m_message & "; must be " & RAW_DATA_TYPE_DOT_RAW_FILES & " or " & RAW_DATA_TYPE_BRUKER_FT_FOLDER)
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Select

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function


End Class
