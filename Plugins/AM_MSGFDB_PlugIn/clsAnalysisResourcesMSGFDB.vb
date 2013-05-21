'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Created 07/29/2011
'
'*********************************************************************************************************

Option Strict On

Imports AnalysisManagerBase

Public Class clsAnalysisResourcesMSGFDB
	Inherits clsAnalysisResources

	Public Overrides Function GetResources() As IJobParams.CloseOutType

		' Make sure the machine has enough free memory to run MSGFDB
		If Not ValidateFreeMemorySize("MSGFDBJavaMemorySize", "MSGFDB", False) Then
			m_message = "Not enough free memory to run MSGFDB"
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		'Retrieve Fasta file
		If Not RetrieveOrgDB(m_mgrParams.GetParam("orgdbdir")) Then Return IJobParams.CloseOutType.CLOSEOUT_FAILED

		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Getting param file")

		' Retrieve param file
		' This will also obtain the _ModDefs.txt file using query 
		'  SELECT Local_Symbol, Monoisotopic_Mass_Correction, Residue_Symbol, Mod_Type_Symbol, Mass_Correction_Tag
		'  FROM V_Param_File_Mass_Mod_Info 
		'  WHERE Param_File_Name = 'ParamFileName'
		If Not RetrieveGeneratedParamFile( _
		   m_jobParams.GetParam("ParmFileName"), _
		   m_jobParams.GetParam("ParmFileStoragePath"), _
		   m_WorkingDir) _
		Then
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		' The ToolName job parameter holds the name of the job script we are executing
		Dim strScriptName As String = m_jobParams.GetParam("ToolName")

		If strScriptName.ToLower().Contains("mzxml") OrElse strScriptName.ToLower().Contains("msgfdb_bruker") Then

			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Getting mzXML file")

			' Retrieve the .mzXML file for this dataset
			' Do not use RetrieveMZXmlFile since that function looks for any valid MSXML_Gen folder for this dataset
			' Instead, use FindAndRetrieveMiscFiles 

			' Note that capitalization matters for the extension; it must be .mzXML
			Dim FileToGet As String = m_DatasetName & ".mzXML"
			If Not FindAndRetrieveMiscFiles(FileToGet, False) Then
				'Errors were reported in function call, so just return
				Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
			End If
			m_jobParams.AddResultFileToSkip(FileToGet)

		Else
			' Retrieve the _DTA.txt file
			' Retrieve unzipped dta files (do not de-concatenate since MSGFDB uses the _Dta.txt file directly)
			If Not RetrieveDtaFiles(False) Then
				'Errors were reported in function call, so just return
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			Dim strAssumedScanType As String
			strAssumedScanType = m_jobParams.GetParam("AssumedScanType")

			If Not String.IsNullOrWhiteSpace(strAssumedScanType) Then
				' Scan type is assumed; we don't need the Masic ScanStats.txt files or the .Raw file
				Select Case strAssumedScanType.ToUpper()
					Case "CID", "ETD", "HCD"
						If m_DebugLevel >= 1 Then
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Assuming scan type is '" & strAssumedScanType & "'")
						End If
					Case Else
						m_message = "Invalid assumed scan type '" & strAssumedScanType & "'; must be CID, ETD, or HCD"
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
						Return IJobParams.CloseOutType.CLOSEOUT_FAILED
				End Select
			Else
				' Retrieve the MASIC ScanStats.txt and ScanStatsEx.txt files
				Dim blnSuccess As Boolean
				blnSuccess = RetrieveScanStatsFiles(m_WorkingDir, CreateStoragePathInfoOnly:=False, RetrieveScanStatsFile:=True, RetrieveScanStatsExFile:=False)

				If blnSuccess Then
					' Open the ScanStats file and read the header line to see if column ScanTypeName is present
					Dim blnScanTypeColumnFound As Boolean
					Dim strScanStatsFilePath As String = System.IO.Path.Combine(m_WorkingDir, m_DatasetName & "_ScanStats.txt")
					blnScanTypeColumnFound = ValidateScanStatsFileHasScanTypeNameColumn(strScanStatsFilePath)

					If Not blnScanTypeColumnFound Then
						' We also have to retrieve the _ScanStatsEx.txt file
						blnSuccess = RetrieveScanStatsFiles(m_WorkingDir, CreateStoragePathInfoOnly:=False, RetrieveScanStatsFile:=False, RetrieveScanStatsExFile:=True)
					End If

				End If

				If Not blnSuccess Then
					' _ScanStats.txt file not found
					' If processing a .Raw file or .UIMF file then we can create the file using the MSFileInfoScanner
					If Not GenerateScanStatsFile() Then
						' Error message should already have been logged and stored in m_message
						Return IJobParams.CloseOutType.CLOSEOUT_FAILED
					End If
				Else
					If m_DebugLevel >= 1 Then
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Retrieved MASIC ScanStats and ScanStatsEx files")
					End If
				End If
			End If

			' If the _dta.txt file is over 2 GB in size, then condense it
			If Not ValidateCDTAFileSize(m_WorkingDir, m_DatasetName & "_dta.txt") Then
				'Errors were reported in function call, so just return
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			' Remove any spectra from the _DTA.txt file with fewer than 3 ions
			If Not ValidateCDTAFileRemoveSparseSpectra(m_WorkingDir, m_DatasetName & "_dta.txt") Then
				'Errors were reported in function call, so just return
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If
		End If


		'Add all the extensions of the files to delete after run
		m_jobParams.AddResultFileExtensionToSkip(".mzXML")
		m_jobParams.AddResultFileExtensionToSkip("_dta.zip") 'Zipped DTA
		m_jobParams.AddResultFileExtensionToSkip("_dta.txt") 'Unzipped, concatenated DTA
		m_jobParams.AddResultFileExtensionToSkip("temp.tsv") ' MSGFDB creates .txt.temp.tsv files, which we don't need

		m_jobParams.AddResultFileExtensionToSkip(SCAN_STATS_FILE_SUFFIX)
		m_jobParams.AddResultFileExtensionToSkip("_ScanStatsEx.txt")

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

	Protected Function ValidateScanStatsFileHasScanTypeNameColumn(ByVal strScanStatsFilePath As String) As Boolean

		Dim blnScanTypeColumnFound As Boolean = False

		Using srScanStatsFile As System.IO.StreamReader = New System.IO.StreamReader(New System.IO.FileStream(strScanStatsFilePath, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.ReadWrite))

			If srScanStatsFile.Peek > -1 Then
				Dim lstColumns As Generic.List(Of String)
				lstColumns = srScanStatsFile.ReadLine().Split(ControlChars.Tab).ToList()

				If lstColumns.Contains("ScanTypeName") Then
					blnScanTypeColumnFound = True
				End If
			End If

		End Using

		Return blnScanTypeColumnFound

	End Function

End Class
