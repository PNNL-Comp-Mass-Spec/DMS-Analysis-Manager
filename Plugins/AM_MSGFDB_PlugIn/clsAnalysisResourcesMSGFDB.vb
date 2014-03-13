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

	Public Overrides Function GetResources() As IJobParams.CloseOutType

		Dim currentTask As String = "Initializing"

		Try

			currentTask = "GetHPCOptions"

			' Determine whether or not we'll be running MSGF+ in HPC (high performance computing) mode
			Dim udtHPCOptions As udtHPCOptionsType = GetHPCOptions(m_jobParams, m_MgrName)

			' Make sure the machine has enough free memory to run MSGFDB
			If udtHPCOptions.UsingHPC Then
				' Make sure the working directory exists and that it is empty
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
				currentTask = "ValidateFreeMemorySize"
				If Not ValidateFreeMemorySize("MSGFDBJavaMemorySize", "MSGFDB", False) Then
					m_message = "Not enough free memory to run MSGFDB"
					Return IJobParams.CloseOutType.CLOSEOUT_FAILED
				End If
			End If

			' Retrieve the Fasta file
			Dim localOrgDbFolder = m_mgrParams.GetParam("orgdbdir")

			If udtHPCOptions.UsingHPC Then
				' Override the OrgDbDir to point to Picfs, specifically \\picfs\projects\DMS\DMS_Temp_Org
				localOrgDbFolder = Path.Combine(udtHPCOptions.SharePath, "DMS_Temp_Org")
			End If

			currentTask = "RetrieveOrgDB to " & localOrgDbFolder

			If Not RetrieveOrgDB(localOrgDbFolder, udtHPCOptions) Then Return IJobParams.CloseOutType.CLOSEOUT_FAILED

			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Getting param file")

			' Retrieve param file
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

			If strScriptName.ToLower().Contains("mzxml") OrElse strScriptName.ToLower().Contains("msgfplus_bruker") Then

				' Retrieve the .mzXML file for this dataset
				' Do not use RetrieveMZXmlFile since that function looks for any valid MSXML_Gen folder for this dataset
				' Instead, use FindAndRetrieveMiscFiles 

				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Getting mzXML file")
				currentTask = "Get mzXML file"

				' Note that capitalization matters for the extension; it must be .mzXML
				Dim FileToGet As String = m_DatasetName & ".mzXML"
				If Not FindAndRetrieveMiscFiles(FileToGet, False) Then
					'Errors were reported in function call, so just return
					Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
				End If
				m_jobParams.AddResultFileToSkip(FileToGet)

				If Not MyBase.ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders) Then
					Return IJobParams.CloseOutType.CLOSEOUT_FAILED
				End If

			Else
				' Retrieve the _DTA.txt file
				' Note that if the file was found in MyEMSL then RetrieveDtaFiles will auto-call ProcessMyEMSLDownloadQueue to download the file

				currentTask = "RetrieveDtaFiles"

				If Not RetrieveDtaFiles() Then
					Dim sharedResultsFolder = m_jobParams.GetParam("SharedResultsFolders")
					If Not String.IsNullOrEmpty(sharedResultsFolder) Then
						m_message &= "; shared results folder is " & sharedResultsFolder
					End If

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

					currentTask = "RetrieveScanStatsFiles"

					Dim blnSuccess As Boolean
					blnSuccess = RetrieveScanStatsFiles(CreateStoragePathInfoOnly:=False, RetrieveScanStatsFile:=True, RetrieveScanStatsExFile:=False)

					If Not MyBase.ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders) Then
						Return IJobParams.CloseOutType.CLOSEOUT_FAILED
					End If

					If blnSuccess Then
						' Open the ScanStats file and read the header line to see if column ScanTypeName is present
						Dim blnScanTypeColumnFound As Boolean
						Dim strScanStatsFilePath As String = Path.Combine(m_WorkingDir, m_DatasetName & "_ScanStats.txt")
						blnScanTypeColumnFound = ValidateScanStatsFileHasScanTypeNameColumn(strScanStatsFilePath)

						If Not blnScanTypeColumnFound Then
							' We also have to retrieve the _ScanStatsEx.txt file
							blnSuccess = RetrieveScanStatsFiles(CreateStoragePathInfoOnly:=False, RetrieveScanStatsFile:=False, RetrieveScanStatsExFile:=True)
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

				currentTask = "ProcessMyEMSLDownloadQueue"

				If Not MyBase.ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders) Then
					Return IJobParams.CloseOutType.CLOSEOUT_FAILED
				End If

				' If the _dta.txt file is over 2 GB in size, then condense it
				currentTask = "ValidateCDTAFileSize"

				If Not ValidateCDTAFileSize(m_WorkingDir, m_DatasetName & "_dta.txt") Then
					'Errors were reported in function call, so just return
					Return IJobParams.CloseOutType.CLOSEOUT_FAILED
				End If

				' Remove any spectra from the _DTA.txt file with fewer than 3 ions
				currentTask = "ValidateCDTAFileRemoveSparseSpectra"

				If Not ValidateCDTAFileRemoveSparseSpectra(m_WorkingDir, m_DatasetName & "_dta.txt") Then
					'Errors were reported in function call, so just return
					Return IJobParams.CloseOutType.CLOSEOUT_FAILED
				End If
			End If

			currentTask = "Add extensions to skip"

			'Add all the extensions of the files to delete after run
			m_jobParams.AddResultFileExtensionToSkip(".mzXML")
			m_jobParams.AddResultFileExtensionToSkip("_dta.zip") 'Zipped DTA
			m_jobParams.AddResultFileExtensionToSkip("_dta.txt") 'Unzipped, concatenated DTA
			m_jobParams.AddResultFileExtensionToSkip("temp.tsv") ' MSGFDB creates .txt.temp.tsv files, which we don't need

			m_jobParams.AddResultFileExtensionToSkip(SCAN_STATS_FILE_SUFFIX)
			m_jobParams.AddResultFileExtensionToSkip("_ScanStatsEx.txt")

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

	Protected Function ValidateScanStatsFileHasScanTypeNameColumn(ByVal strScanStatsFilePath As String) As Boolean

		Dim blnScanTypeColumnFound As Boolean = False

		Using srScanStatsFile As StreamReader = New StreamReader(New FileStream(strScanStatsFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))

			If srScanStatsFile.Peek > -1 Then
				Dim lstColumns As List(Of String)
				lstColumns = srScanStatsFile.ReadLine().Split(ControlChars.Tab).ToList()

				If lstColumns.Contains("ScanTypeName") Then
					blnScanTypeColumnFound = True
				End If
			End If

		End Using

		Return blnScanTypeColumnFound

	End Function

End Class
