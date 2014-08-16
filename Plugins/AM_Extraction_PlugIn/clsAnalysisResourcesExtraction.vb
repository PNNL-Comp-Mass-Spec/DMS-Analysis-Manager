'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2006, Battelle Memorial Institute
' Created 009/22/2008
'
'*********************************************************************************************************

Imports AnalysisManagerBase
Imports System.IO

Public Class clsAnalysisResourcesExtraction
	Inherits clsAnalysisResources

	'*********************************************************************************************************
	'Manages retrieval of all files needed for data extraction
	'*********************************************************************************************************

#Region "Constants"
	Public Const MOD_DEFS_FILE_SUFFIX As String = "_ModDefs.txt"
	Public Const MASS_CORRECTION_TAGS_FILENAME As String = "Mass_Correction_Tags.txt"
#End Region

#Region "Module variables"
	Protected mRetrieveOrganismDB As Boolean

	' Keys are the original file name, values are the new name
	Protected m_PendingFileRenames As Dictionary(Of String, String)
#End Region

#Region "Methods"

	Public Overrides Sub Setup(ByRef mgrParams As IMgrParams, ByRef jobParams As IJobParams)
		Dim statusTools As IStatusFile = Nothing
		Setup(mgrParams, jobParams, statusTools)		
	End Sub

	Public Overrides Sub Setup(mgrParams As IMgrParams, jobParams As IJobParams, statusTools As IStatusFile)
		MyBase.Setup(mgrParams, jobParams, statusTools)

		Dim orgDbRequired As Boolean = True
		Dim strResultType As String = m_jobParams.GetParam("ResultType")

		If strResultType = RESULT_TYPE_MSGFDB Then
			' Extraction of MSGF+ results typically does not require a fasta file
			orgDbRequired = False
		End If

		SetOption(clsGlobal.eAnalysisResourceOptions.OrgDbRequired, orgDbRequired)
	End Sub

	''' <summary>
	''' Gets all files needed to perform data extraction
	''' </summary>
	''' <returns>IJobParams.CloseOutType specifying results</returns>
	''' <remarks></remarks>
	Public Overrides Function GetResources() As IJobParams.CloseOutType

		' Set this to true for now
		' It will be changed to False if processing MSGFDB results and the _PepToProtMap.txt file is successfully retrieved
		mRetrieveOrganismDB = True
		m_PendingFileRenames = New Dictionary(Of String, String)

		Dim strResultType As String = m_jobParams.GetParam("ResultType")

		'Get analysis results files
		If GetInputFiles(strResultType) <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		'Get misc files
		If RetrieveMiscFiles(strResultType) <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		If Not MyBase.ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders) Then
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		For Each entry In m_PendingFileRenames
			Dim sourceFile As New FileInfo(Path.Combine(m_WorkingDir, entry.Key))
			If sourceFile.Exists Then
				sourceFile.MoveTo(Path.Combine(m_WorkingDir, entry.Value))
			End If
		Next

		If mRetrieveOrganismDB Then
			Dim blnSkipProteinMods = m_jobParams.GetJobParameter("SkipProteinMods", False)
			If Not blnSkipProteinMods Then
				' Retrieve the Fasta file; required to create the _ProteinMods.txt file
				If Not RetrieveOrgDB(m_mgrParams.GetParam("orgdbdir")) Then
					Return IJobParams.CloseOutType.CLOSEOUT_FAILED
				End If
			End If
		End If

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

	''' <summary>
	''' Retrieves input files (ie, .out files) needed for extraction
	''' </summary>
	''' <param name="strResultType">String specifying type of analysis results input to extraction process</param>
	''' <returns>IJobParams.CloseOutType specifying results</returns>
	''' <remarks></remarks>
	Private Function GetInputFiles(ByVal strResultType As String) As IJobParams.CloseOutType

		Dim eResult As IJobParams.CloseOutType

		Try

			Select Case strResultType
				Case RESULT_TYPE_SEQUEST
					eResult = GetSequestFiles()

				Case RESULT_TYPE_XTANDEM
					eResult = GetXTandemFiles()

				Case RESULT_TYPE_INSPECT
					eResult = GetInspectFiles()

				Case RESULT_TYPE_MSGFDB
					eResult = GetMSGFPlusFiles()

				Case RESULT_TYPE_MSALIGN
					eResult = GetMSAlignFiles()

				Case RESULT_TYPE_MODA
					eResult = GetMODaFiles()

				Case RESULT_TYPE_MSPATHFINDER
					eResult = GetMSPathFinderFiles()

				Case Else
					m_message = "Invalid tool result type: " & strResultType
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
					Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End Select

			If eResult <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
				Return eResult
			End If

			RetrieveToolVersionFile(strResultType)

		Catch ex As Exception
			m_message = "Error retrieving input files"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message)
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

	Private Function GetSequestFiles() As IJobParams.CloseOutType

		'Get the concatenated .out file
		If Not RetrieveOutFiles(False) Then
			'Errors were reported in function call, so just return
			Return IJobParams.CloseOutType.CLOSEOUT_NO_OUT_FILES
		End If

		' Note that we'll obtain the Sequest parameter file in RetrieveMiscFiles

		'Add all the extensions of the files to delete after run
		m_jobParams.AddResultFileExtensionToSkip("_dta.zip") 'Zipped DTA
		m_jobParams.AddResultFileExtensionToSkip("_dta.txt") 'Unzipped, concatenated DTA
		m_jobParams.AddResultFileExtensionToSkip("_out.zip") 'Zipped OUT
		m_jobParams.AddResultFileExtensionToSkip("_out.txt") 'Unzipped, concatenated OUT
		m_jobParams.AddResultFileExtensionToSkip(".dta")  'DTA files
		m_jobParams.AddResultFileExtensionToSkip(".out")  'DTA files

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

	Private Function GetXTandemFiles() As IJobParams.CloseOutType

		Dim FileToGet As String

		FileToGet = m_DatasetName & "_xt.zip"
		If Not FindAndRetrieveMiscFiles(FileToGet, True) Then
			'Errors were reported in function call, so just return
			Return IJobParams.CloseOutType.CLOSEOUT_NO_XT_FILES
		End If
		m_jobParams.AddResultFileToSkip(FileToGet)

		'Manually adding this file to FilesToDelete; we don't want the unzipped .xml file to be copied to the server
		m_jobParams.AddResultFileToSkip(m_DatasetName & "_xt.xml")

		' Note that we'll obtain the X!Tandem parameter file in RetrieveMiscFiles

		' However, we need to obtain the "input.xml" file and "default_input.xml" files now
		FileToGet = "input.xml"
		If Not FindAndRetrieveMiscFiles(FileToGet, False) Then
			'Errors were reported in function call, so just return
			Return IJobParams.CloseOutType.CLOSEOUT_NO_PARAM_FILE
		End If
		m_jobParams.AddResultFileToSkip(FileToGet)

		If Not CopyFileToWorkDir("default_input.xml", m_jobParams.GetParam("ParmFileStoragePath"), m_WorkingDir) Then
			Const Msg As String = "Failed retrieving default_input.xml file."
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg)
			Return IJobParams.CloseOutType.CLOSEOUT_NO_PARAM_FILE
		End If

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

	Private Function GetInspectFiles() As IJobParams.CloseOutType

		Dim FileToGet As String

		' Get the zipped Inspect results files

		' This file contains the p-value filtered results
		FileToGet = m_DatasetName & "_inspect.zip"
		If Not FindAndRetrieveMiscFiles(FileToGet, False) Then
			'Errors were reported in function call, so just return
			Return IJobParams.CloseOutType.CLOSEOUT_NO_INSP_FILES
		End If
		m_jobParams.AddResultFileToSkip(FileToGet)

		' This file contains top hit for each scan (no filters)
		FileToGet = m_DatasetName & "_inspect_fht.zip"
		If Not FindAndRetrieveMiscFiles(FileToGet, False) Then
			'Errors were reported in function call, so just return
			Return IJobParams.CloseOutType.CLOSEOUT_NO_INSP_FILES
		End If
		m_jobParams.AddResultFileToSkip(FileToGet)

		' Get the peptide to protein mapping file
		FileToGet = m_DatasetName & "_inspect_PepToProtMap.txt"
		If Not FindAndRetrieveMiscFiles(FileToGet, False) Then
			'Errors were reported in function call

			' See if IgnorePeptideToProteinMapError=True
			If m_jobParams.GetJobParameter("IgnorePeptideToProteinMapError", False) Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Ignoring missing _PepToProtMap.txt file since 'IgnorePeptideToProteinMapError' = True")
			Else
				Return IJobParams.CloseOutType.CLOSEOUT_NO_INSP_FILES
			End If
		Else
			' The OrgDB (aka fasta file) is not required
			mRetrieveOrganismDB = False
		End If
		m_jobParams.AddResultFileToSkip(FileToGet)

		' Note that we'll obtain the Inspect parameter file in RetrieveMiscFiles

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

	Private Function GetMODaFiles() As IJobParams.CloseOutType

		Dim FileToGet As String

		FileToGet = m_DatasetName & "_moda.zip"
		If Not FindAndRetrieveMiscFiles(FileToGet, True) Then
			'Errors were reported in function call, so just return
			Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
		End If
		m_jobParams.AddResultFileToSkip(FileToGet)
		m_jobParams.AddResultFileExtensionToSkip("_moda.txt")

		FileToGet = m_DatasetName & "_mgf_IndexToScanMap.txt"
		If Not FindAndRetrieveMiscFiles(FileToGet, False) Then
			Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
		End If
		m_jobParams.AddResultFileToSkip(FileToGet)

		' Note that we'll obtain the MODa parameter file in RetrieveMiscFiles

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

	Private Function GetMSGFPlusFiles() As IJobParams.CloseOutType

		Dim currentStep As String = "Initializing"

		Dim blnUseLegacyMSGFDB As Boolean
		Dim splitFastaEnabled = m_jobParams.GetJobParameter("SplitFasta", False)
		Dim suffixToAdd As String
		Dim mzidSuffix As String

		Dim numberOfClonedSteps = 1
		Dim pepToProtMapRetrievalError As Boolean = False

		Try

			If splitFastaEnabled Then
				numberOfClonedSteps = m_jobParams.GetJobParameter("NumberOfClonedSteps", 0)
				If numberOfClonedSteps = 0 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Settings file is missing parameter NumberOfClonedSteps; cannot retrieve MSGFPlus results")
					Return IJobParams.CloseOutType.CLOSEOUT_FAILED
				End If

				suffixToAdd = "_Part1"
			Else
				suffixToAdd = String.Empty
			End If

			Dim SourceFolderPath As String
			currentStep = "Determining results file type based on the results file name"
			blnUseLegacyMSGFDB = False

			SourceFolderPath = FindDataFile(m_DatasetName & "_msgfplus" & suffixToAdd & ".mzid.gz", True, False)
			If String.IsNullOrEmpty(SourceFolderPath) Then
				' File not found
				SourceFolderPath = FindDataFile(m_DatasetName & "_msgfplus" & suffixToAdd & ".zip", True, False)
				If String.IsNullOrEmpty(SourceFolderPath) Then
					' File not found
					SourceFolderPath = FindDataFile(m_DatasetName & "_msgfdb" & suffixToAdd & ".zip", True, False)
					If String.IsNullOrEmpty(SourceFolderPath) Then
						' File not found; log a warning
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Could not find the _msgfplus.mzid.gz, _msgfplus.zip file, or the _msgfdb.zip file; assuming we're running MSGF+")
						mzidSuffix = ".mzid.gz"
					Else
						' File Found
						blnUseLegacyMSGFDB = True
						mzidSuffix = ".zip"
					End If
				Else
					' Running MSGF+ with zipped results
					mzidSuffix = ".zip"
				End If
			Else
				' Running MSGF+ with gzipped results
				mzidSuffix = ".mzid.gz"
			End If

			For iteration As Integer = 1 To numberOfClonedSteps

				Dim blnSkipMSGFResultsZipFileCopy As Boolean = False
				Dim FileToGet As String
				Dim strBaseName As String

				If splitFastaEnabled Then
					suffixToAdd = "_Part" & iteration
				Else
					suffixToAdd = String.Empty
				End If

				If blnUseLegacyMSGFDB Then
					strBaseName = m_DatasetName & "_msgfdb"

					If splitFastaEnabled Then
						m_message = "GetMSGFPlusFiles does not support SplitFasta mode for legacy MSGF-DB results"
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
						Return IJobParams.CloseOutType.CLOSEOUT_FAILED
					End If
				Else
					strBaseName = m_DatasetName & "_msgfplus" & suffixToAdd

					FileToGet = m_DatasetName & "_msgfdb" & suffixToAdd & ".tsv"
					currentStep = "Retrieving " & FileToGet

					SourceFolderPath = FindDataFile(FileToGet, False, False)

					If Not String.IsNullOrEmpty(SourceFolderPath) Then

						If Not SourceFolderPath.StartsWith(MYEMSL_PATH_FLAG) Then
							' Examine the date of the TSV file
							' If less than 4 hours old, then retrieve it; otherwise, grab the _msgfplus.zip file and re-generate the .tsv file

							Dim fiTSVFile As FileInfo
							fiTSVFile = New FileInfo(Path.Combine(SourceFolderPath, FileToGet))
							If DateTime.UtcNow.Subtract(fiTSVFile.LastWriteTimeUtc).TotalHours < 4 Then
								' File is recent; grab it
								If Not CopyFileToWorkDir(FileToGet, SourceFolderPath, m_WorkingDir) Then
									' File copy failed; that's OK; we'll grab the _msgfplus.mzid.gz file
								Else
									blnSkipMSGFResultsZipFileCopy = True
									m_jobParams.AddResultFileToSkip(FileToGet)
								End If
							End If

							m_jobParams.AddServerFileToDelete(fiTSVFile.FullName)
						End If

					End If
				End If

				If Not blnSkipMSGFResultsZipFileCopy Then
					FileToGet = strBaseName & mzidSuffix
					currentStep = "Retrieving " & FileToGet

					If Not FindAndRetrieveMiscFiles(FileToGet, True) Then
						'Errors were reported in function call, so just return
						Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
					End If
					m_jobParams.AddResultFileToSkip(FileToGet)
				End If

				' Manually add several files to skip
				If splitFastaEnabled Then
					m_jobParams.AddResultFileToSkip(m_DatasetName & "_msgfdb_Part" & iteration & ".txt")
					m_jobParams.AddResultFileToSkip(m_DatasetName & "_msgfplus_Part" & iteration & ".mzid")
					m_jobParams.AddResultFileToSkip(m_DatasetName & "_msgfdb_Part" & iteration & ".tsv")
				Else
					m_jobParams.AddResultFileToSkip(m_DatasetName & "_msgfdb.txt")
					m_jobParams.AddResultFileToSkip(m_DatasetName & "_msgfplus.mzid")
					m_jobParams.AddResultFileToSkip(m_DatasetName & "_msgfdb.tsv")
				End If

				' Get the peptide to protein mapping file
				FileToGet = m_DatasetName & "_msgfdb" & suffixToAdd & "_PepToProtMap.txt"
				currentStep = "Retrieving " & FileToGet

				If Not FindAndRetrieveMiscFiles(FileToGet, False) Then
					'Errors were reported in function call

					' See if IgnorePeptideToProteinMapError=True
					If m_jobParams.GetJobParameter("IgnorePeptideToProteinMapError", False) Then
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Ignoring missing _PepToProtMap.txt file since 'IgnorePeptideToProteinMapError' = True")
					Else
						Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
					End If

					pepToProtMapRetrievalError = True
				Else
					If splitFastaEnabled Then
						Dim fiPepToProtMapFile As FileInfo
						fiPepToProtMapFile = New FileInfo(Path.Combine(SourceFolderPath, FileToGet))
						m_jobParams.AddServerFileToDelete(fiPepToProtMapFile.FullName)
					End If
				End If

				m_jobParams.AddResultFileToSkip(FileToGet)

			Next

			If Not pepToProtMapRetrievalError Then
				' The OrgDB (aka fasta file) is not required
				mRetrieveOrganismDB = False
			End If

		Catch ex As Exception
			m_message = "Error in GetMSGFPlusFiles at step " & currentStep
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex)
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

		' Note that we'll obtain the MSGF-DB parameter file in RetrieveMiscFiles

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

	Private Function GetMSAlignFiles() As IJobParams.CloseOutType

		Dim FileToGet As String

		FileToGet = m_DatasetName & "_MSAlign_ResultTable.txt"
		If Not FindAndRetrieveMiscFiles(FileToGet, False) Then
			'Errors were reported in function call, so just return
			Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
		End If
		m_jobParams.AddResultFileToSkip(FileToGet)

		' Note that we'll obtain the MSAlign parameter file in RetrieveMiscFiles

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function


	Private Function GetMSPathFinderFiles() As IJobParams.CloseOutType

		Dim FileToGet As String

		FileToGet = m_DatasetName & "_IcTsv.zip"

		If Not FindAndRetrieveMiscFiles(FileToGet, True) Then
			'Errors were reported in function call, so just return
			Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
		End If
		m_jobParams.AddResultFileToSkip(FileToGet)
		m_jobParams.AddResultFileExtensionToSkip(".tsv")

		' Note that we'll obtain the MSPathFinder parameter file in RetrieveMiscFiles

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

	' Deprecated function
	'
	' <summary>
	' Copies the default Mass Correction Tags file to the working directory
	' </summary>
	' <returns>True if success, otherwise false</returns>
	' <remarks></remarks>
	'Protected Function RetrieveDefaultMassCorrectionTagsFile() As Boolean

	'	Dim strParamFileStoragePath As String
	'	Dim ioFolderInfo As System.DirectoryInfo
	'	Dim ioSubfolders() As System.DirectoryInfo
	'	Dim ioFiles() As System.FileInfo

	'	Try
	'		strParamFileStoragePath = m_jobParams.GetParam("ParmFileStoragePath")
	'		ioFolderInfo = New System.DirectoryInfo(strParamFileStoragePath).Parent

	'		ioSubfolders = ioFolderInfo.GetDirectories("MassCorrectionTags")

	'		If ioSubfolders.Length = 0 Then
	'			m_message = "MassCorrectionTags folder not found at " & ioFolderInfo.FullName
	'			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
	'			Return False
	'		End If

	'		ioFiles = ioSubfolders(0).GetFiles(MASS_CORRECTION_TAGS_FILENAME)
	'		If ioFiles.Length = 0 Then
	'			m_message = MASS_CORRECTION_TAGS_FILENAME & " file not found at " & ioSubfolders(0).FullName
	'			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
	'			Return False
	'		End If

	'		If m_DebugLevel >= 1 Then
	'			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Retrieving default Mass Correction Tags file from " & ioFiles(0).FullName)
	'		End If

	'		ioFiles(0).CopyTo(System.Path.Combine(m_WorkingDir, ioFiles(0).Name))

	'	Catch ex As Exception
	'		m_message = "Error retrieving " & MASS_CORRECTION_TAGS_FILENAME
	'		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message)
	'		Return False
	'	End Try

	'	Return True

	'End Function


	''' <summary>
	''' Retrieves misc files (i.e., ModDefs) needed for extraction
	''' </summary>
	''' <returns>IJobParams.CloseOutType specifying results</returns>
	''' <remarks></remarks>
	Protected Friend Function RetrieveMiscFiles(ByVal ResultType As String) As IJobParams.CloseOutType

		Dim strParamFileName As String = m_jobParams.GetParam("ParmFileName")
		Dim ModDefsFilename As String = Path.GetFileNameWithoutExtension(strParamFileName) & MOD_DEFS_FILE_SUFFIX

		Dim blnSuccess As Boolean

		Try

			' Call RetrieveGeneratedParamFile() now to re-create the parameter file, retrieve the _ModDefs.txt file, and retrieve the MassCorrectionTags.txt file
			' Although the ModDefs file should have been created when Sequest, X!Tandem, Inspect, MSGFDB, or MSAlign ran, we re-generate it here just in case T_Param_File_Mass_Mods had missing information
			' Furthermore, we need the search engine parameter file for the PHRPReader

			' Note that the _ModDefs.txt file is obtained using this query:
			'  SELECT Local_Symbol, Monoisotopic_Mass_Correction, Residue_Symbol, Mod_Type_Symbol, Mass_Correction_Tag
			'  FROM V_Param_File_Mass_Mod_Info 
			'  WHERE Param_File_Name = 'ParamFileName'

			blnSuccess = RetrieveGeneratedParamFile( _
			 strParamFileName, _
			 m_jobParams.GetParam("ParmFileStoragePath"))

			If Not blnSuccess Then
				m_message = "Error retrieving parameter file and ModDefs.txt file"
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			' Confirm that the file was actually created
			Dim fiModDefsFile = New FileInfo(Path.Combine(m_WorkingDir, ModDefsFilename))

			If Not fiModDefsFile.Exists And ResultType <> RESULT_TYPE_MSALIGN Then
				m_message = "Unable to create the ModDefs.txt file; update T_Param_File_Mass_Mods"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Unable to create the ModDefs.txt file; define the modifications in table T_Param_File_Mass_Mods for parameter file " & strParamFileName)
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			m_jobParams.AddResultFileToSkip(strParamFileName)
			m_jobParams.AddResultFileToSkip(MASS_CORRECTION_TAGS_FILENAME)

			Dim logModFilesFileNotFound = Not ResultType <> RESULT_TYPE_MSALIGN

			' Check whether the newly generated ModDefs file matches the existing one
			' If it doesn't match, or if the existing one is missing, then we need to keep the file
			' Otherwise, we can skip it
			Dim remoteModDefsFolder = FindDataFile(ModDefsFilename, SearchArchivedDatasetFolder:=True, LogFileNotFound:=logModFilesFileNotFound)
			If String.IsNullOrEmpty(remoteModDefsFolder) Then
				' ModDefs file not found on the server
				If fiModDefsFile.Length = 0 Then
					m_jobParams.AddResultFileToSkip(ModDefsFilename)
				End If
			ElseIf remoteModDefsFolder.ToLower().StartsWith("\\proto") Then
				If clsGlobal.FilesMatch(fiModDefsFile.FullName, Path.Combine(remoteModDefsFolder, ModDefsFilename)) Then
					m_jobParams.AddResultFileToSkip(ModDefsFilename)
				End If
			End If


		Catch ex As Exception
			m_message = "Error retrieving miscellaneous files"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message)
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

	Protected Function RetrieveToolVersionFile(ByVal strResultType As String) As Boolean

		Dim eResultType As PHRPReader.clsPHRPReader.ePeptideHitResultType
		Dim blnSuccess As Boolean

		Try
			' Make sure the ResultType is valid
			eResultType = PHRPReader.clsPHRPReader.GetPeptideHitResultType(strResultType)

			Dim strToolVersionFile As String = PHRPReader.clsPHRPReader.GetToolVersionInfoFilename(eResultType)
			Dim strToolVersionFileNewName As String = String.Empty

			Dim strToolNameForScript As String = m_jobParams.GetJobParameter("ToolName", String.Empty)
			If eResultType = PHRPReader.clsPHRPReader.ePeptideHitResultType.MSGFDB And strToolNameForScript = "MSGFPlus_IMS" Then
				' PeptideListToXML expects the ToolVersion file to be named "Tool_Version_Info_MSGFDB.txt"
				' However, this is the MSGFPlus_IMS script, so the file is currently "Tool_Version_Info_MSGFPlus_IMS.txt"
				' We'll copy the current file locally, then rename it to the expected name
				strToolVersionFileNewName = String.Copy(strToolVersionFile)
				strToolVersionFile = "Tool_Version_Info_MSGFPlus_IMS.txt"
			End If

			blnSuccess = FindAndRetrieveMiscFiles(strToolVersionFile, False, False)

			If blnSuccess AndAlso Not String.IsNullOrEmpty(strToolVersionFileNewName) Then
				m_PendingFileRenames.Add(strToolVersionFile, strToolVersionFileNewName)

				strToolVersionFile = strToolVersionFileNewName
			End If

			m_jobParams.AddResultFileToSkip(strToolVersionFile)

		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error in RetrieveToolVersionFile: " & ex.Message)
			Return False
		End Try

		Return blnSuccess

	End Function
#End Region

End Class
