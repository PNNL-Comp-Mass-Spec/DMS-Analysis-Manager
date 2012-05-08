Option Strict On

Imports AnalysisManagerBase
Imports PHRPReader

Public Class clsAnalysisResourcesIDPicker
	Inherits clsAnalysisResources

	Public Overrides Function GetResources() As IJobParams.CloseOutType

		Dim strDatasetName As String
		strDatasetName = m_jobParams.GetParam("DatasetNum")

		' Retrieve the parameter file associated peptide search tool (Sequest, XTandem, MSGFDB, etc.)
		Dim strParamFileName As String = m_jobParams.GetParam("ParmFileName")
		Dim strParamFileStoragePath As String = m_jobParams.GetParam("ParmFileStoragePath")

		If Not RetrieveFile(strParamFileName, strParamFileStoragePath, m_WorkingDir) Then
			Return IJobParams.CloseOutType.CLOSEOUT_NO_PARAM_FILE
		End If

		' Retrieve the PSM result files, PHRP files, and MSGF file
		If Not GetInputFiles(strDatasetName, strParamFileName) Then
			Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
		End If

		' Retrieve the MASIC ScanStats.txt and ScanStatsEx.txt files
		If Not RetrieveMASICFiles(strDatasetName) Then
			Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
		End If

        'Retrieve the Fasta file
        If Not RetrieveOrgDB(m_mgrParams.GetParam("orgdbdir")) Then Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        
		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function


	Protected Function GetInputFiles(ByVal strDatasetName As String, ByVal strSearchEngineParamFileName As String) As Boolean
		' This tracks the filenames to find and whether or not they are required
		Dim lstFileNamesToGet As System.Collections.Generic.SortedList(Of String, Boolean)
		Dim lstExtraFilesToGet As System.Collections.Generic.List(Of String)

		Dim strResultType As String

		Dim eResultType As PHRPReader.clsPHRPReader.ePeptideHitResultType
		Dim blnSuccess As Boolean

		strResultType = m_jobParams.GetParam("ResultType")

		' Make sure the ResultType is valid
		eResultType = clsPHRPReader.GetPeptideHitResultType(strResultType)

		If eResultType = clsPHRPReader.ePeptideHitResultType.Sequest OrElse _
		   eResultType = clsPHRPReader.ePeptideHitResultType.XTandem OrElse _
		   eResultType = clsPHRPReader.ePeptideHitResultType.Inspect OrElse _
		   eResultType = clsPHRPReader.ePeptideHitResultType.MSGFDB Then
			blnSuccess = True
		Else
			m_message = "Invalid tool result type (not supported by IDPicker): " & strResultType
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
			blnSuccess = False
		End If

		If Not blnSuccess Then
			Return False
		End If

		lstFileNamesToGet = GetPHRPFileNames(eResultType, strDatasetName)

		If m_DebugLevel >= 2 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Retrieving the " & eResultType.ToString & " files")
		End If

		For Each kvEntry As System.Collections.Generic.KeyValuePair(Of String, Boolean) In lstFileNamesToGet

			If Not FindAndRetrieveMiscFiles(kvEntry.Key, False) Then
				' File not found; is it required?
				If kvEntry.Value Then
					'Errors were reported in function call, so just return
					Return False
				End If
			End If

			m_jobParams.AddResultFileToSkip(kvEntry.Key)
		Next

		' Verify that the synopsis file was copied, is not 0-bytes, and contains more than just a header row
		Dim strSynFilePath As String
		strSynFilePath = System.IO.Path.Combine(m_WorkingDir, clsPHRPReader.GetPHRPSynopsisFileName(eResultType, strDatasetName))

		If Not ValidateFileHasData(strSynFilePath, "Synopsis") Then
			' Errors were already logged (including file not found)
			Return False
		End If

		If eResultType = clsPHRPReader.ePeptideHitResultType.XTandem Then
			' X!Tandem requires a few additional parameter files
			lstExtraFilesToGet = clsPHRPParserXTandem.GetAdditionalSearchEngineParamFileNames(System.IO.Path.Combine(m_WorkingDir, strSearchEngineParamFileName))
			For Each strFileName As String In lstExtraFilesToGet

				If Not FindAndRetrieveMiscFiles(strFileName, False) Then
					' File not found
					Return False
				End If

				m_jobParams.AddResultFileToSkip(strFileName)
			Next
		End If

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
