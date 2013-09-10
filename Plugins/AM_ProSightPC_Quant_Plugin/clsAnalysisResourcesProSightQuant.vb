Option Strict On

Imports AnalysisManagerBase

Public Class clsAnalysisResourcesProSightQuant
	Inherits clsAnalysisResources

	Public Const PROSIGHT_PC_RESULT_FILE As String = "ProSightPC_Results.xls"
	Public Const TOOL_DISABLED As Boolean = True

	Public Overrides Function GetResources() As IJobParams.CloseOutType

		If TOOL_DISABLED Then
			' This tool is currently disabled, so just return Success
			Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
		End If

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

		Dim strParamFileName As String = m_jobParams.GetParam("ProSightQuantParamFile")
		If String.IsNullOrEmpty(strParamFileName) Then
			m_message = clsAnalysisToolRunnerBase.NotifyMissingParameter(m_jobParams, "ProSightQuantParamFile")
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
			Return IJobParams.CloseOutType.CLOSEOUT_NO_PARAM_FILE
		End If

		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Getting data files")

		If Not RetrieveFile(strParamFileName, strParamFileStoragePath) Then
			Return IJobParams.CloseOutType.CLOSEOUT_NO_PARAM_FILE
		End If

		' Retrieve the ProSightPC results for this job
		Dim strProSightPCResultsFile As String
		strProSightPCResultsFile = PROSIGHT_PC_RESULT_FILE
		If Not FindAndRetrieveMiscFiles(strProSightPCResultsFile, False) Then
			'Errors were reported in function call, so just return
			Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
		End If
		m_jobParams.AddResultFileToSkip(strProSightPCResultsFile)


		' Get the instrument data file
		Dim strRawDataType As String = m_jobParams.GetParam("RawDataType")

		Select Case strRawDataType.ToLower
			Case RAW_DATA_TYPE_DOT_RAW_FILES, RAW_DATA_TYPE_BRUKER_FT_FOLDER
				If RetrieveSpectra(strRawDataType) Then

					If Not MyBase.ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders) Then
						Return IJobParams.CloseOutType.CLOSEOUT_FAILED
					End If

					' Confirm that the .Raw or .D folder was actually copied locally
					If strRawDataType.ToLower() = RAW_DATA_TYPE_DOT_RAW_FILES Then
						If Not System.IO.File.Exists(System.IO.Path.Combine(m_WorkingDir, m_DatasetName & DOT_RAW_EXTENSION)) Then
							m_message = "Thermo .Raw file not successfully copied to WorkDir; likely a timeout error"
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsDtaGenResources.GetResources: " & m_message)
							Return IJobParams.CloseOutType.CLOSEOUT_FAILED
						End If
						m_jobParams.AddResultFileExtensionToSkip(clsAnalysisResources.DOT_RAW_EXTENSION)  'Raw file

					ElseIf strRawDataType.ToLower() = RAW_DATA_TYPE_BRUKER_FT_FOLDER Then
						If Not System.IO.Directory.Exists(System.IO.Path.Combine(m_WorkingDir, m_DatasetName & DOT_D_EXTENSION)) Then
							m_message = "Bruker .D folder not successfully copied to WorkDir; likely a timeout error"
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsDtaGenResources.GetResources: " & m_message)
							Return IJobParams.CloseOutType.CLOSEOUT_FAILED
						End If
					End If

				Else
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsDtaGenResources.GetResources: Error occurred retrieving spectra.")
					Return IJobParams.CloseOutType.CLOSEOUT_FAILED
				End If
			Case Else
				m_message = "Dataset type " & strRawDataType & " is not supported"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsDtaGenResources.GetResources: " & m_message & "; must be " & RAW_DATA_TYPE_DOT_RAW_FILES & " or " & RAW_DATA_TYPE_BRUKER_FT_FOLDER)
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Select

		If Not MyBase.ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders) Then
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function


End Class
