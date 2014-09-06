Option Strict On

Imports AnalysisManagerBase

Public Class clsAnalysisResourcesMSXMLGen
    Inherits clsAnalysisResources

#Region "Methods"
    ''' <summary>
	''' Retrieves files necessary for creating the .mzXML file
    ''' </summary>
    ''' <returns>IJobParams.CloseOutType indicating success or failure</returns>
    ''' <remarks></remarks>
    Public Overrides Function GetResources() As IJobParams.CloseOutType

		Dim currentTask As String = "Initializing"

		Try

			currentTask = "Determine RawDataType"

			' Get input data file
			Dim strRawDataType As String = m_jobParams.GetParam("RawDataType")

			Select Case strRawDataType.ToLower
				Case RAW_DATA_TYPE_DOT_RAW_FILES, RAW_DATA_TYPE_DOT_D_FOLDERS
					currentTask = "Retrieve spectra: " & strRawDataType

					If RetrieveSpectra(strRawDataType) Then
						m_jobParams.AddResultFileExtensionToSkip(DOT_RAW_EXTENSION)	 'Raw file
					Else
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsDtaGenResources.GetResources: Error occurred retrieving spectra.")
						Return IJobParams.CloseOutType.CLOSEOUT_FAILED
					End If
				Case Else
					m_message = "Dataset type " & strRawDataType & " is not supported"
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsDtaGenResources.GetResources: " & m_message & "; must be " & RAW_DATA_TYPE_DOT_RAW_FILES)
					Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End Select

			currentTask = "ProcessMyEMSLDownloadQueue"
			If Not ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders) Then
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			Dim mzMLRefParamFile = m_jobParams.GetJobParameter("MzMLRefParamFile", String.Empty)
			If Not String.IsNullOrEmpty(mzMLRefParamFile) Then

				' Retrieve the Fasta file
				Dim localOrgDbFolder = m_mgrParams.GetParam("orgdbdir")

				currentTask = "RetrieveOrgDB to " & localOrgDbFolder

				If Not RetrieveOrgDB(localOrgDbFolder) Then Return IJobParams.CloseOutType.CLOSEOUT_FAILED

				currentTask = "Retrieve the MzML Refinery parameter file " & mzMLRefParamFile

				Const paramFileStoragePathKeyName As String = clsGlobal.STEPTOOL_PARAMFILESTORAGEPATH_PREFIX & "MzML_Refinery"

				Dim mzMLRefineryParmFileStoragePath = m_mgrParams.GetParam(paramFileStoragePathKeyName)
				If String.IsNullOrWhiteSpace(mzMLRefineryParmFileStoragePath) Then
					mzMLRefineryParmFileStoragePath = "\\gigasax\dms_parameter_Files\MzMLRefinery"
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Parameter '" & paramFileStoragePathKeyName & "' is not defined (obtained using V_Pipeline_Step_Tools_Detail_Report in the Broker DB); will assume: " & mzMLRefineryParmFileStoragePath)

				End If
				'Retrieve param file
				If Not RetrieveFile( _
				   mzMLRefParamFile, _
				   m_jobParams.GetParam("ParmFileStoragePath")) _
				Then Return IJobParams.CloseOutType.CLOSEOUT_FAILED



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
