'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2008, Battelle Memorial Institute
' Created 07/08/2008
'
' Last modified 06/11/2009 JDS - Added logging using log4net
'*********************************************************************************************************

Imports AnalysisManagerBase

Public Class clsDtaGenResources
	Inherits clsAnalysisResources

	'*********************************************************************************************************
	'Gets resources necessary for DTA creation
	'*********************************************************************************************************

#Region "Methods"
	Public Overrides Function GetResources() As IJobParams.CloseOutType

		Dim strRawDataType As String = m_jobParams.GetJobParameter("RawDataType", "")
		Dim blnMGFInstrumentData As Boolean = m_jobParams.GetJobParameter("MGFInstrumentData", False)

		Dim eDtaGeneratorType As clsDtaGenToolRunner.eDTAGeneratorConstants
		Dim strErrorMessage As String = String.Empty

		eDtaGeneratorType = clsDtaGenToolRunner.GetDTAGeneratorInfo(m_jobParams, strErrorMessage)
		If eDtaGeneratorType = clsDtaGenToolRunner.eDTAGeneratorConstants.Unknown Then
			If String.IsNullOrEmpty(strErrorMessage) Then
				m_message = "GetDTAGeneratorInfo reported an Unknown DTAGenerator type"
			Else
				m_message = strErrorMessage
			End If

			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
			Return IJobParams.CloseOutType.CLOSEOUT_NO_SETTINGS_FILE
		End If

		If Not GetParameterFiles(eDtaGeneratorType) Then
			Return IJobParams.CloseOutType.CLOSEOUT_NO_PARAM_FILE
		End If


		If blnMGFInstrumentData Then
			Dim strFileToFind As String = m_DatasetName & DOT_MGF_EXTENSION
			If Not FindAndRetrieveMiscFiles(strFileToFind, False) Then
				m_message = "Instrument data not found: " & strFileToFind
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsDtaGenResources.GetResources: " & m_message)
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			Else
				m_jobParams.AddResultFileExtensionToSkip(clsAnalysisResources.DOT_MGF_EXTENSION)
			End If
		Else
			'Get input data file
			If Not RetrieveSpectra(strRawDataType) Then
				If String.IsNullOrEmpty(m_message) Then
					m_message = "Error retrieving instrument data file"
				End If

				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsDtaGenResources.GetResources: " & m_message)
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If
		End If

		If Not MyBase.ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders) Then
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

	Protected Function GetParameterFiles(eDtaGeneratorType As clsDtaGenToolRunner.eDTAGeneratorConstants) As Boolean

		If eDtaGeneratorType = clsDtaGenToolRunner.eDTAGeneratorConstants.DeconConsole Then

			Dim strParamFileStoragePathKeyName As String
			Dim strParamFileStoragePath As String
			strParamFileStoragePathKeyName = clsGlobal.STEPTOOL_PARAMFILESTORAGEPATH_PREFIX & "DTA_Gen"

			strParamFileStoragePath = m_mgrParams.GetParam(strParamFileStoragePathKeyName)
			If strParamFileStoragePath Is Nothing OrElse strParamFileStoragePath.Length = 0 Then
				strParamFileStoragePath = "\\gigasax\DMS_Parameter_Files\DTA_Gen"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Parameter '" & strParamFileStoragePathKeyName & "' is not defined (obtained using V_Pipeline_Step_Tools_Detail_Report in the Broker DB); will assume: " & strParamFileStoragePath)
			End If

			Dim strParamFileName As String
			strParamFileName = m_jobParams.GetJobParameter("DtaGenerator", "DeconMSn_ParamFile", String.Empty)

			If String.IsNullOrEmpty(strParamFileName) Then
				m_message = clsAnalysisToolRunnerBase.NotifyMissingParameter(m_jobParams, "DeconMSn_ParamFile")
				Return False
			End If

			If Not RetrieveFile(strParamFileName, strParamFileStoragePath) Then
				Return False
			End If

		End If

		Return True

	End Function

#End Region

End Class

