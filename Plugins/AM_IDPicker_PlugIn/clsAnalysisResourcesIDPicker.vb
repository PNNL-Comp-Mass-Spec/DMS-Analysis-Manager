Option Strict On

Imports AnalysisManagerBase
Imports PHRPReader

Public Class clsAnalysisResourcesIDPicker
	Inherits clsAnalysisResources

	Public Const IDPICKER_PARAM_FILENAME_LOCAL As String = "IDPickerParamFileLocal"
	Public Const DEFAULT_IDPICKER_PARAM_FILE_NAME As String = "IDPicker_Defaults.txt"

	Protected mSynopsisFileIsEmpty As Boolean

	Public Overrides Function GetResources() As IJobParams.CloseOutType

		Dim strDatasetName As String
		Dim RawDataType As String
		Dim eRawDataType As eRawDataTypeConstants
		Dim blnMGFInstrumentData As Boolean
		Dim eReturnCode As IJobParams.CloseOutType = IJobParams.CloseOutType.CLOSEOUT_SUCCESS

		' Retrieve the parameter file for the associated peptide search tool (Sequest, XTandem, MSGFDB, etc.)
		Dim strParamFileName As String = m_jobParams.GetParam("ParmFileName")

		If Not FindAndRetrieveMiscFiles(strParamFileName, False) Then
			Return IJobParams.CloseOutType.CLOSEOUT_NO_PARAM_FILE
		End If
		m_jobParams.AddResultFileToSkip(strParamFileName)

		' Retrieve the IDPicker parameter file specified for this job
		If Not RetrieveIDPickerParamFile() Then
			Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
		End If

		strDatasetName = m_jobParams.GetParam("DatasetNum")
		RawDataType = m_jobParams.GetParam("RawDataType")
		eRawDataType = clsAnalysisResources.GetRawDataType(RawDataType)
		blnMGFInstrumentData = m_jobParams.GetJobParameter("MGFInstrumentData", False)

		' Retrieve the PSM result files, PHRP files, and MSGF file
		If Not GetInputFiles(strDatasetName, strParamFileName, eReturnCode) Then
			If eReturnCode = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then eReturnCode = IJobParams.CloseOutType.CLOSEOUT_FAILED
			Return eReturnCode
		End If

		If mSynopsisFileIsEmpty Then
			' Don't retrieve anymore files
			Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
		End If

		If Not blnMGFInstrumentData Then
			' Retrieve the MASIC ScanStats.txt and ScanStatsEx.txt files
			If eRawDataType = eRawDataTypeConstants.ThermoRawFile Or eRawDataType = eRawDataTypeConstants.UIMF Then
				If Not RetrieveMASICFiles(strDatasetName) Then
					Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
				End If
			Else
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Not retrieving MASIC files since unsupported data type: " & RawDataType)
			End If
		End If

		'Retrieve the Fasta file
		If Not RetrieveOrgDB(m_mgrParams.GetParam("orgdbdir")) Then Return IJobParams.CloseOutType.CLOSEOUT_FAILED

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

	Protected Function GetInputFiles(ByVal strDatasetName As String, ByVal strSearchEngineParamFileName As String, ByRef eReturnCode As IJobParams.CloseOutType) As Boolean
		' This tracks the filenames to find and whether or not they are required
		Dim lstFileNamesToGet As System.Collections.Generic.SortedList(Of String, Boolean)
		Dim lstExtraFilesToGet As System.Collections.Generic.List(Of String)

		Dim strResultType As String

		Dim eResultType As PHRPReader.clsPHRPReader.ePeptideHitResultType
		eReturnCode = IJobParams.CloseOutType.CLOSEOUT_SUCCESS

		strResultType = m_jobParams.GetParam("ResultType")

		' Make sure the ResultType is valid
		eResultType = clsPHRPReader.GetPeptideHitResultType(strResultType)

		If eResultType = clsPHRPReader.ePeptideHitResultType.Sequest OrElse _
		   eResultType = clsPHRPReader.ePeptideHitResultType.XTandem OrElse _
		   eResultType = clsPHRPReader.ePeptideHitResultType.Inspect OrElse _
		   eResultType = clsPHRPReader.ePeptideHitResultType.MSGFDB Then
		Else
			m_message = "Invalid tool result type (not supported by IDPicker): " & strResultType
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
			eReturnCode = IJobParams.CloseOutType.CLOSEOUT_FAILED
			Return False
		End If

		lstFileNamesToGet = GetPHRPFileNames(eResultType, strDatasetName)
		mSynopsisFileIsEmpty = False

		If m_DebugLevel >= 2 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Retrieving the " & eResultType.ToString & " files")
		End If

		Dim synFileName As String
		synFileName = clsPHRPReader.GetPHRPSynopsisFileName(eResultType, strDatasetName)

		For Each kvEntry As System.Collections.Generic.KeyValuePair(Of String, Boolean) In lstFileNamesToGet

			If Not FindAndRetrieveMiscFiles(kvEntry.Key, False) Then
				' File not found; is it required?
				If kvEntry.Value Then
					'Errors were reported in function call, so just return
					eReturnCode = IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
					Return False
				End If		
			End If

			m_jobParams.AddResultFileToSkip(kvEntry.Key)

			If kvEntry.Key = synFileName Then
				' Check whether the synopsis file is empty
				Dim strSynFilePath As String
				strSynFilePath = System.IO.Path.Combine(m_WorkingDir, synFileName)

				Dim strErrorMessage As String = String.Empty
				If Not ValidateFileHasData(strSynFilePath, "Synopsis file", strErrorMessage) Then
					' The synopsis file is empty
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, strErrorMessage)

					' We don't want to fail the job out yet; instead, we'll exit now, then let the ToolRunner exit with a Completion message of "Synopsis file is empty"
					mSynopsisFileIsEmpty = True
					Return True
				End If
			End If

		Next


		If eResultType = clsPHRPReader.ePeptideHitResultType.XTandem Then
			' X!Tandem requires a few additional parameter files
			lstExtraFilesToGet = clsPHRPParserXTandem.GetAdditionalSearchEngineParamFileNames(System.IO.Path.Combine(m_WorkingDir, strSearchEngineParamFileName))
			For Each strFileName As String In lstExtraFilesToGet

				If Not FindAndRetrieveMiscFiles(strFileName, False) Then
					' File not found
					eReturnCode = IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
					Return False
				End If

				m_jobParams.AddResultFileToSkip(strFileName)
			Next
		End If

		Return True

	End Function

	Protected Function RetrieveIDPickerParamFile() As Boolean

		Dim strIDPickerParamFileName As String = m_jobParams.GetParam("IDPickerParamFile")
		Dim strIDPickerParamFilePath As String
		Dim strParamFileStoragePathKeyName As String

		If String.IsNullOrEmpty(strIDPickerParamFileName) Then
			strIDPickerParamFileName = DEFAULT_IDPICKER_PARAM_FILE_NAME
		End If

		strParamFileStoragePathKeyName = clsGlobal.STEPTOOL_PARAMFILESTORAGEPATH_PREFIX & "IDPicker"
		strIDPickerParamFilePath = m_mgrParams.GetParam(strParamFileStoragePathKeyName)
		If String.IsNullOrEmpty(strIDPickerParamFilePath) Then
			strIDPickerParamFilePath = "\\gigasax\dms_parameter_Files\IDPicker"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Parameter '" & strParamFileStoragePathKeyName & "' is not defined (obtained using V_Pipeline_Step_Tools_Detail_Report in the Broker DB); will assume: " & strIDPickerParamFilePath)
		End If

		If Not CopyFileToWorkDir(strIDPickerParamFileName, strIDPickerParamFilePath, m_WorkingDir) Then
			'Errors were reported in function call, so just return
			Return False
		End If

		' Store the param file name so that we can load later
		m_jobParams.AddAdditionalParameter("JobParameters", IDPICKER_PARAM_FILENAME_LOCAL, strIDPickerParamFileName)

		Return True

	End Function

	Protected Function RetrieveMASICFiles(ByVal strDatasetName As String) As Boolean

		' Retrieve the MASIC ScanStats.txt and ScanStatsEx.txt files
		If Not RetrieveScanStatsFiles(m_WorkingDir, False) Then
			' _ScanStats.txt file not found
			' If processing a .Raw file or .UIMF file then we can create the file using the MSFileInfoScanner
			If Not GenerateScanStatsFile() Then
				' Error message should already have been logged and stored in m_message
				Return False
			End If
		Else
			If m_DebugLevel >= 1 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Retrieved MASIC ScanStats and ScanStatsEx files")
			End If
		End If

		m_jobParams.AddResultFileToSkip(strDatasetName & SCAN_STATS_FILE_SUFFIX)
		m_jobParams.AddResultFileToSkip(strDatasetName & SCAN_STATS_EX_FILE_SUFFIX)
		Return True

	End Function

	Protected Function GetPHRPFileNames(ByVal eResultType As clsPHRPReader.ePeptideHitResultType, ByVal strDatasetName As String) As System.Collections.Generic.SortedList(Of String, Boolean)

		Dim lstFileNamesToGet As System.Collections.Generic.SortedList(Of String, Boolean)
		lstFileNamesToGet = New System.Collections.Generic.SortedList(Of String, Boolean)

		Dim synFileName As String
		synFileName = clsPHRPReader.GetPHRPSynopsisFileName(eResultType, strDatasetName)

		lstFileNamesToGet.Add(synFileName, True)
		lstFileNamesToGet.Add(clsPHRPReader.GetPHRPModSummaryFileName(eResultType, strDatasetName), False)
		lstFileNamesToGet.Add(clsPHRPReader.GetPHRPResultToSeqMapFileName(eResultType, strDatasetName), True)
		lstFileNamesToGet.Add(clsPHRPReader.GetPHRPSeqInfoFileName(eResultType, strDatasetName), True)
		lstFileNamesToGet.Add(clsPHRPReader.GetPHRPSeqToProteinMapFileName(eResultType, strDatasetName), True)

		lstFileNamesToGet.Add(clsPHRPReader.GetMSGFFileName(synFileName), True)

		Return lstFileNamesToGet

	End Function

End Class
