'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2008, Battelle Memorial Institute
' Created 07/08/2008
'
'*********************************************************************************************************

Imports AnalysisManagerBase
Imports System.IO

''' <summary>
''' Gets resources necessary for DTA creation
''' </summary>
''' <remarks></remarks>
Public Class clsDtaGenResources
	Inherits clsAnalysisResources

#Region "Constants"
    Public Const USING_EXISTING_DECONMSN_RESULTS As String = "Using_existing_DeconMSn_Results"
#End Region

#Region "Methods"
	Public Overrides Function GetResources() As IJobParams.CloseOutType

		Dim strRawDataType As String = m_jobParams.GetJobParameter("RawDataType", "")
		Dim blnMGFInstrumentData As Boolean = m_jobParams.GetJobParameter("MGFInstrumentData", False)

		Dim eDtaGeneratorType As clsDtaGenToolRunner.eDTAGeneratorConstants
		Dim strErrorMessage As String = String.Empty

		Dim zippedDTAFilePath As String = String.Empty

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

			Dim blnCentroidDTAs = False
			If eDtaGeneratorType = clsDtaGenToolRunner.eDTAGeneratorConstants.DeconConsole Then
				blnCentroidDTAs = False
			Else
				blnCentroidDTAs = m_jobParams.GetJobParameter("CentroidDTAs", False)
			End If

			If blnCentroidDTAs Then
				' Look for a DTA_Gen_1_26_ folder for this dataset
				' If it exists, and if we can find a valid _dta.zip file, then use that file instead of re-running DeconMSn (since DeconMSn can take some time to run)

				Dim datasetID = m_jobParams.GetJobParameter("DatasetID", 0)
				Dim folderNameToFind = "DTA_Gen_1_26_" & datasetID
				Dim fileToFind = m_DatasetName & "_dta.zip"
				Dim validFolderFound As Boolean

				Dim existingDtaFolder = FindValidFolder(m_DatasetName, fileToFind, folderNameToFind, MaxRetryCount:=1, LogFolderNotFound:=False, RetrievingInstrumentDataFolder:=False, validFolderFound:=validFolderFound)

				If validFolderFound Then
					' Copy the file locally (or queue it for download from MyEMSL)

					Dim blnFileCopiedOrQueued = CopyFileToWorkDir(fileToFind, existingDtaFolder, m_WorkingDir)

					If blnFileCopiedOrQueued Then
						zippedDTAFilePath = Path.Combine(m_WorkingDir, fileToFind)

						m_jobParams.AddAdditionalParameter("JobParameters", USING_EXISTING_DECONMSN_RESULTS, "True")

						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Found pre-existing DeconMSn results; will not re-run DeconMSn if they are valid")

						fileToFind = m_DatasetName & "_profile.txt"
						blnFileCopiedOrQueued = CopyFileToWorkDir(fileToFind, existingDtaFolder, m_WorkingDir)

						fileToFind = m_DatasetName & "_DeconMSn_log.txt"
						blnFileCopiedOrQueued = CopyFileToWorkDir(fileToFind, existingDtaFolder, m_WorkingDir)
					End If

				End If

			End If
		End If

		If Not MyBase.ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders) Then
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		If Not String.IsNullOrEmpty(zippedDTAFilePath) Then

			Threading.Thread.Sleep(150)

			Dim fiZippedDtaFile = New FileInfo(zippedDTAFilePath)
			Dim tempZipFilePath = Path.Combine(m_WorkingDir, Path.GetFileNameWithoutExtension(fiZippedDtaFile.Name) & "_PreExisting.zip")

			fiZippedDtaFile.MoveTo(tempZipFilePath)

			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Unzipping file " + Path.GetFileName(zippedDTAFilePath))
			If UnzipFileStart(tempZipFilePath, m_WorkingDir, "clsDtaGenResources", False) Then
				If m_DebugLevel >= 1 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Unzipped file " + Path.GetFileName(zippedDTAFilePath))
				End If

				m_jobParams.AddResultFileToSkip(Path.GetFileName(tempZipFilePath))

			End If
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

