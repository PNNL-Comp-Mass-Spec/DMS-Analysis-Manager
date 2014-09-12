'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Created 07/29/2011
'
'*********************************************************************************************************

Option Strict On

Imports AnalysisManagerBase
Imports System.IO

Public Class clsAnalysisResourcesMSGFDB
	Inherits clsAnalysisResources

	Public Overrides Sub Setup(ByRef mgrParams As IMgrParams, ByRef jobParams As IJobParams)
		MyBase.Setup(mgrParams, jobParams)
		SetOption(clsGlobal.eAnalysisResourceOptions.OrgDbRequired, True)
	End Sub

	Public Overrides Sub Setup(mgrParams As IMgrParams, jobParams As IJobParams, statusTools As IStatusFile)
		MyBase.Setup(mgrParams, jobParams, statusTools)
		SetOption(clsGlobal.eAnalysisResourceOptions.OrgDbRequired, True)
	End Sub

	''' <summary>
	''' Retrieves files necessary for running MSGF+
	''' </summary>
	''' <returns>IJobParams.CloseOutType indicating success or failure</returns>
	''' <remarks></remarks>
	Public Overrides Function GetResources() As IJobParams.CloseOutType

		Dim currentTask As String = "Initializing"

		Try

			currentTask = "GetHPCOptions"

			' Determine whether or not we'll be running MSGF+ in HPC (high performance computing) mode
			Dim udtHPCOptions As udtHPCOptionsType = GetHPCOptions(m_jobParams, m_MgrName)

			If udtHPCOptions.UsingHPC Then
				' Make sure the HPC working directory exists and that it is empty
				currentTask = "Verify " & udtHPCOptions.WorkDirPath

				Dim diPicFsWorkDir = New DirectoryInfo(udtHPCOptions.WorkDirPath)
				If diPicFsWorkDir.Exists Then
					Const blnDeleteFolderIfEmpty = False
					m_FileTools.DeleteDirectoryFiles(diPicFsWorkDir.FullName, blnDeleteFolderIfEmpty)
				Else
					currentTask = "Create " & udtHPCOptions.WorkDirPath

					Try
						diPicFsWorkDir.Create()

					Catch ex As Exception
						m_message = "Unable to create folder " & udtHPCOptions.WorkDirPath & ": " & ex.Message
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & "; " & clsGlobal.GetExceptionStackTrace(ex))

						CheckParentFolder(diPicFsWorkDir)

						Return IJobParams.CloseOutType.CLOSEOUT_FAILED
					End Try

				End If
			Else
				' Make sure the machine has enough free memory to run MSGFDB
				currentTask = "ValidateFreeMemorySize"
				If Not ValidateFreeMemorySize("MSGFDBJavaMemorySize", "MSGFDB", False) Then
					m_message = "Not enough free memory to run MSGFDB"
					Return IJobParams.CloseOutType.CLOSEOUT_FAILED
				End If
			End If

			' Retrieve the Fasta file
			Dim localOrgDbFolder = m_mgrParams.GetParam("orgdbdir")

			If udtHPCOptions.UsingHPC Then
				' Override the OrgDbDir to point to Picfs, specifically \\winhpcfs\projects\DMS\DMS_Temp_Org
				localOrgDbFolder = Path.Combine(udtHPCOptions.SharePath, "DMS_Temp_Org")
			End If

			currentTask = "RetrieveOrgDB to " & localOrgDbFolder

			If Not RetrieveOrgDB(localOrgDbFolder, udtHPCOptions) Then Return IJobParams.CloseOutType.CLOSEOUT_FAILED

			If m_DebugLevel >= 2 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Getting param file")
			End If

			' Retrieve the parameter file
			' This will also obtain the _ModDefs.txt file using query 
			'  SELECT Local_Symbol, Monoisotopic_Mass_Correction, Residue_Symbol, Mod_Type_Symbol, Mass_Correction_Tag
			'  FROM V_Param_File_Mass_Mod_Info 
			'  WHERE Param_File_Name = 'ParamFileName'

			Dim paramFileName = m_jobParams.GetParam("ParmFileName")
			currentTask = "RetrieveGeneratedParamFile " & paramFileName

			If Not RetrieveGeneratedParamFile( _
			   paramFileName, _
			   m_jobParams.GetParam("ParmFileStoragePath")) _
			Then
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			' The ToolName job parameter holds the name of the job script we are executing
			Dim strScriptName As String = m_jobParams.GetParam("ToolName")
			Dim eResult As IJobParams.CloseOutType

			If strScriptName.ToLower().Contains("mzxml") OrElse strScriptName.ToLower().Contains("msgfplus_bruker") Then
				currentTask = "Get mzXML file"
				eResult = GetMzXMLFile()

			ElseIf strScriptName.ToLower().Contains("mzml") Then
				currentTask = "Get mzML file"
				eResult = GetMzMLFile()

			Else
				currentTask = "RetrieveDtaFiles"
				eResult = GetCDTAFile()

				If eResult = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
					currentTask = "GetMasicFiles"
					eResult = GetMasicFiles()
				End If

				If eResult = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
					currentTask = "ValidateCDTAFile"
					eResult = ValidateCDTAFile()
				End If

			End If

			If eResult <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
				Return eResult
			End If

			currentTask = "Add extensions to skip"

			'Add all the extensions of the files to delete after run
			m_jobParams.AddResultFileExtensionToSkip(DOT_MZXML_EXTENSION)
			m_jobParams.AddResultFileExtensionToSkip("_dta.zip") 'Zipped DTA
			m_jobParams.AddResultFileExtensionToSkip("_dta.txt") 'Unzipped, concatenated DTA
			m_jobParams.AddResultFileExtensionToSkip("temp.tsv") ' MSGFDB creates .txt.temp.tsv files, which we don't need

			m_jobParams.AddResultFileExtensionToSkip(SCAN_STATS_FILE_SUFFIX)
			m_jobParams.AddResultFileExtensionToSkip(SCAN_STATS_EX_FILE_SUFFIX)

			Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

		Catch ex As Exception
			m_message = "Exception in GetResources: " & ex.Message
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & "; task = " & currentTask & "; " & clsGlobal.GetExceptionStackTrace(ex))
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

	End Function

	Private Sub CheckParentFolder(ByVal diPicFsWorkDir As DirectoryInfo)

		Try
			Dim lstParentDirectories = diPicFsWorkDir.Parent.GetDirectories().ToList()

			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Found " & lstParentDirectories.Count & " subdirectories in " & diPicFsWorkDir.FullName)

		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception checking " & diPicFsWorkDir.Parent.FullName & ": " & ex.Message)
		End Try

	End Sub

	Private Function GetCDTAFile() As IJobParams.CloseOutType

		' Retrieve the _DTA.txt file
		' Note that if the file was found in MyEMSL then RetrieveDtaFiles will auto-call ProcessMyEMSLDownloadQueue to download the file

		If Not RetrieveDtaFiles() Then
			Dim sharedResultsFolder = m_jobParams.GetParam("SharedResultsFolders")
			If Not String.IsNullOrEmpty(sharedResultsFolder) Then
				m_message &= "; shared results folder is " & sharedResultsFolder
			End If

			' Errors were reported in function call, so just return
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

	Private Function GetMasicFiles() As IJobParams.CloseOutType

		Dim strAssumedScanType  = m_jobParams.GetParam("AssumedScanType")

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

			Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
		End If

		' Retrieve the MASIC ScanStats.txt file (and possibly the ScanStatsEx.txt file)

		Dim blnSuccess As Boolean
		blnSuccess = RetrieveScanStatsFiles(CreateStoragePathInfoOnly:=False, RetrieveScanStatsFile:=True, RetrieveScanStatsExFile:=False)

		If Not MyBase.ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders) Then
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		If blnSuccess Then
			' Open the ScanStats file and read the header line to see if column ScanTypeName is present
			' Also confirm that there are MSn spectra labeled as HCD, CID, or ETD
			Dim strScanStatsOrExFilePath As String = Path.Combine(m_WorkingDir, m_DatasetName & "_ScanStats.txt")

			Dim blnScanTypeColumnFound = ValidateScanStatsFileHasScanTypeNameColumn(strScanStatsOrExFilePath)

			If Not blnScanTypeColumnFound Then
				' We also have to retrieve the _ScanStatsEx.txt file
				blnSuccess = RetrieveScanStatsFiles(CreateStoragePathInfoOnly:=False, RetrieveScanStatsFile:=False, RetrieveScanStatsExFile:=True)

				If blnSuccess Then
					strScanStatsOrExFilePath = Path.Combine(m_WorkingDir, m_DatasetName & "_ScanStatsEx.txt")
				End If
			End If

			If blnScanTypeColumnFound OrElse blnSuccess Then
				Dim detailedScanTypesDefined = ValidateScanStatsFileHasDetailedScanTypes(strScanStatsOrExFilePath)

				If Not detailedScanTypesDefined Then
					If blnScanTypeColumnFound Then
						m_message = "ScanTypes defined in the ScanTypeName column"
					Else
						m_message = "ScanTypes defined in the ""Collision Mode"" column or ""Scan Filter Text"" column"
					End If

					m_message &= " do not contain detailed CID, ETD, or HCD information; MSGF+ could use the wrong scoring model; fix this problem before running MSGF+"

					Return IJobParams.CloseOutType.CLOSEOUT_FAILED
				End If

			End If
		End If

		If blnSuccess Then
			If m_DebugLevel >= 1 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Retrieved MASIC ScanStats and ScanStatsEx files")
			End If
			Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
		End If

		' _ScanStats.txt file not found
		' If processing a .Raw file or .UIMF file then we can create the file using the MSFileInfoScanner
		If Not GenerateScanStatsFile() Then
			' Error message should already have been logged and stored in m_message
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		Dim strScanStatsFilePath As String = Path.Combine(m_WorkingDir, m_DatasetName & "_ScanStats.txt")
		Dim detailedScanTypesDefinedNewFile = ValidateScanStatsFileHasDetailedScanTypes(strScanStatsFilePath)

		If Not detailedScanTypesDefinedNewFile Then
			m_message = "ScanTypes defined in the ScanTypeName column do not contain detailed CID, ETD, or HCD information; MSGF+ could use the wrong scoring model; fix this problem before running MSGF+"
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		If Not MyBase.ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders) Then
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

	Private Function ValidateCDTAFile() As IJobParams.CloseOutType

		' If the _dta.txt file is over 2 GB in size, then condense it
		If Not ValidateCDTAFileSize(m_WorkingDir, m_DatasetName & "_dta.txt") Then
			' Errors were reported in function call, so just return
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		' Remove any spectra from the _DTA.txt file with fewer than 3 ions
		If Not ValidateCDTAFileRemoveSparseSpectra(m_WorkingDir, m_DatasetName & "_dta.txt") Then
			' Errors were reported in function call, so just return
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		' Make sure that the spectra are centroided
		Dim strCDTAPath = Path.Combine(m_WorkingDir, m_DatasetName & "_dta.txt")

		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Validating that the _dta.txt file has centroided spectra")

		If Not ValidateCDTAFileIsCentroided(strCDTAPath) Then
			' m_message is already updated
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

	Public Shared Function ValidateScanStatsFileHasDetailedScanTypes(ByVal strScanStatsFilePath As String) As Boolean

		Dim lstColumnNameWithScanType = New List(Of String) From {"ScanTypeName", "Collision Mode", "Scan Filter Text"}
		Dim lstColumnIndicesToCheck = New List(Of Integer)

		Dim blnDetailedScanTypesDefined As Boolean = False

		Using srScanStatsFile As StreamReader = New StreamReader(New FileStream(strScanStatsFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))

			If srScanStatsFile.Peek > -1 Then
				' Parse the scan headers

				Dim lstColumns As List(Of String)
				lstColumns = srScanStatsFile.ReadLine().Split(ControlChars.Tab).ToList()

				For Each columnName In lstColumnNameWithScanType
					Dim intScanTypeIndex = lstColumns.IndexOf(columnName)
					If intScanTypeIndex >= 0 Then
						lstColumnIndicesToCheck.Add(intScanTypeIndex)
					End If
				Next

				If lstColumnIndicesToCheck.Count = 0 Then
					Dim sngValue As Single
					If Single.TryParse(lstColumns(0), sngValue) OrElse Single.TryParse(lstColumns(1), sngValue) Then
						' This file does not have a header line
						If lstColumns.Count >= 11 Then
							' Check whether column 11 has ScanTypeName info
							If lstColumns(10).IndexOf("MS", StringComparison.CurrentCultureIgnoreCase) >= 0 OrElse
							   lstColumns(10).IndexOf("SRM", StringComparison.CurrentCultureIgnoreCase) >= 0 OrElse
							   lstColumns(10).IndexOf("MRM", StringComparison.CurrentCultureIgnoreCase) >= 0 Then
								Return True
							End If
						End If

						If lstColumns.Count >= 16 Then
							' Check whether column 15 has "Collision Mode" values
							If lstColumns(15).IndexOf("HCD", StringComparison.CurrentCultureIgnoreCase) >= 0 OrElse
							   lstColumns(15).IndexOf("CID", StringComparison.CurrentCultureIgnoreCase) >= 0 OrElse
							   lstColumns(15).IndexOf("ETD", StringComparison.CurrentCultureIgnoreCase) >= 0 Then
								Return True
							End If
						End If

						If lstColumns.Count >= 17 Then
							' Check whether column 15 has "Collision Mode" values
							If lstColumns(16).IndexOf("HCD", StringComparison.CurrentCultureIgnoreCase) >= 0 OrElse
							   lstColumns(16).IndexOf("CID", StringComparison.CurrentCultureIgnoreCase) >= 0 OrElse
							   lstColumns(16).IndexOf("ETD", StringComparison.CurrentCultureIgnoreCase) >= 0 Then
								Return True
							End If
						End If

					End If
				End If
			End If

			If lstColumnIndicesToCheck.Count > 0 Then
				Do While srScanStatsFile.Peek > -1 And Not blnDetailedScanTypesDefined
					Dim lstColumns As List(Of String)
					lstColumns = srScanStatsFile.ReadLine().Split(ControlChars.Tab).ToList()

					For Each columnIndex In lstColumnIndicesToCheck
						Dim strScanType = lstColumns(columnIndex)

						If strScanType.IndexOf("HCD", StringComparison.CurrentCultureIgnoreCase) >= 0 Then
							blnDetailedScanTypesDefined = True
						ElseIf strScanType.IndexOf("CID", StringComparison.CurrentCultureIgnoreCase) >= 0 Then
							blnDetailedScanTypesDefined = True
						ElseIf strScanType.IndexOf("ETD", StringComparison.CurrentCultureIgnoreCase) >= 0 Then
							blnDetailedScanTypesDefined = True
						End If
					Next

				Loop
			End If

		End Using

		Return blnDetailedScanTypesDefined

	End Function

	Protected Function ValidateScanStatsFileHasScanTypeNameColumn(ByVal strScanStatsFilePath As String) As Boolean

		Dim blnScanTypeColumnFound As Boolean = False

		Using srScanStatsFile As StreamReader = New StreamReader(New FileStream(strScanStatsFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))

			If srScanStatsFile.Peek > -1 Then
				' Parse the scan headers to look for ScanTypeName

				Dim lstColumns As List(Of String)
				Dim sngValue As Single
				lstColumns = srScanStatsFile.ReadLine().Split(ControlChars.Tab).ToList()

				If lstColumns.Contains("ScanTypeName") Then
					blnScanTypeColumnFound = True
				ElseIf Single.TryParse(lstColumns(0), sngValue) OrElse Single.TryParse(lstColumns(1), sngValue) Then
					' This file does not have a header line
					If lstColumns.Count >= 11 Then
						' Assume column 11 is the ScanTypeName column
						If lstColumns(10).IndexOf("MS", StringComparison.CurrentCultureIgnoreCase) >= 0 OrElse
						 lstColumns(10).IndexOf("SRM", StringComparison.CurrentCultureIgnoreCase) >= 0 OrElse
						 lstColumns(10).IndexOf("MRM", StringComparison.CurrentCultureIgnoreCase) >= 0 Then
							blnScanTypeColumnFound = True
						End If
					End If
				End If
			End If

		End Using

		Return blnScanTypeColumnFound

	End Function

End Class
