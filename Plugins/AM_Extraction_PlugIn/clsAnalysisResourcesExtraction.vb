'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2006, Battelle Memorial Institute
' Created 009/22/2008
'
' Last modified 09/25/2008
' Last modified 06/15/2009 JDS - Added logging using log4net
'*********************************************************************************************************

Imports System.IO
Imports AnalysisManagerBase

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
#End Region

#Region "Events"
#End Region

#Region "Properties"
#End Region

#Region "Methods"
	''' <summary>
	''' Gets all files needed to perform data extraction
	''' </summary>
	''' <returns>IJobParams.CloseOutType specifying results</returns>
	''' <remarks></remarks>
	Public Overrides Function GetResources() As IJobParams.CloseOutType

		' Set this to true for now
		' It will be changed to False if processing MSGFDB results and the _PepToProtMap.txt file is successfully retrieved
		mRetrieveOrganismDB = True

		Dim strResultType As String = m_jobParams.GetParam("ResultType")

		'Get analysis results files
		If GetInputFiles(strResultType) <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		'Get misc files
		If RetrieveMiscFiles(strResultType) <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		If mRetrieveOrganismDB Then
			' Retrieve the Fasta file; required to create the _ProteinMods.txt file
			If Not RetrieveOrgDB(m_mgrParams.GetParam("orgdbdir")) Then
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
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

				Case Else
					m_message = "Invalid tool result type: " & strResultType
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
					Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End Select

			RetrieveToolVersionFile(strResultType)

		Catch ex As Exception
			m_message = "Error retrieving input files"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message)
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

	Private Function GetSequestFiles() As IJobParams.CloseOutType
		Dim ExtractionSkipsCDTAFile As Boolean

		ExtractionSkipsCDTAFile = m_jobParams.GetJobParameter("ExtractionSkipsCDTAFile", False)

		If ExtractionSkipsCDTAFile Then
			' Do not grab the _Dta.txt file
		Else
			' As of 1/26/2011 the peptide file extractor no longer needs the concatenated .dta file,
			' so we're no longer copying it

			''If Not RetrieveDtaFiles(False) Then
			''	'Errors were reported in function call, so just return
			''	Return IJobParams.CloseOutType.CLOSEOUT_NO_DTA_FILES
			''End If
		End If

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
		m_jobParams.AddResultFileExtensionToSkip("_PepToProtMapMTS.txt") ' Created by the PeptideToProteinMapEngine when creating the _ProteinMods.txt file

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
			Dim Msg As String = "Failed retrieving default_input.xml file."
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

	Private Function GetMSGFPlusFiles() As IJobParams.CloseOutType

		Dim FileToGet As String

		Dim blnUseLegacyMSGFDB As Boolean
		Dim blnSkipMSGFResultsZipFileCopy As Boolean = False
		Dim strBaseName As String

		Dim SourceFolderPath As String
		SourceFolderPath = FindDataFile(m_DatasetName & "_msgfplus.zip", True, False)
		If String.IsNullOrEmpty(SourceFolderPath) Then
			' File not found
			SourceFolderPath = FindDataFile(m_DatasetName & "_msgfdb.zip", True, False)
			If String.IsNullOrEmpty(SourceFolderPath) Then
				' File not found; log a warning
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Could not find either the _msgfplus.zip file or the _msgfdb.zip file; auto-setting blnUseLegacyMSGFDB=False")
				blnUseLegacyMSGFDB = False
			Else
				' File Found
				blnUseLegacyMSGFDB = True
			End If
		Else
			' Running MSGF+
			blnUseLegacyMSGFDB = False
		End If

		If blnUseLegacyMSGFDB Then
			strBaseName = m_DatasetName & "_msgfdb"
		Else
			strBaseName = m_DatasetName & "_msgfplus"

			FileToGet = m_DatasetName & "_msgfdb.tsv"

			SourceFolderPath = FindDataFile(FileToGet, False, False)

			If Not String.IsNullOrEmpty(SourceFolderPath) Then
				' Examine the date of the TSV file
				' If less than 4 hours old, then retrieve it; otherwise, grab the _msgfplus.zip file and re-generate the .tsv file

				Dim fiTSVFile As System.IO.FileInfo
				fiTSVFile = New System.IO.FileInfo(IO.Path.Combine(SourceFolderPath, FileToGet))
				If DateTime.UtcNow.Subtract(fiTSVFile.LastWriteTimeUtc).TotalHours < 4 Then
					' File is recent; grab it
					If Not CopyFileToWorkDir(FileToGet, SourceFolderPath, m_WorkingDir) Then
						' File copy failed; that's OK; we'll grab the _msgfplus.zip file
					Else
						blnSkipMSGFResultsZipFileCopy = True
						m_jobParams.AddResultFileToSkip(FileToGet)
					End If
				End If

				m_jobParams.AddServerFileToDelete(fiTSVFile.FullName)
			End If
		End If

		If Not blnSkipMSGFResultsZipFileCopy Then
			FileToGet = strBaseName & ".zip"
			If Not FindAndRetrieveMiscFiles(FileToGet, True) Then
				'Errors were reported in function call, so just return
				Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
			End If
			m_jobParams.AddResultFileToSkip(FileToGet)
		End If

		'Manually add several files to skip
		m_jobParams.AddResultFileToSkip(m_DatasetName & "_msgfdb.txt")
		m_jobParams.AddResultFileToSkip(m_DatasetName & "_msgfplus.mzid")
		m_jobParams.AddResultFileToSkip(m_DatasetName & "_msgfdb.tsv")

		' Get the peptide to protein mapping file
		FileToGet = m_DatasetName & "_msgfdb_PepToProtMap.txt"
		If Not FindAndRetrieveMiscFiles(FileToGet, False) Then
			'Errors were reported in function call

			' See if IgnorePeptideToProteinMapError=True
			If m_jobParams.GetJobParameter("IgnorePeptideToProteinMapError", False) Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Ignoring missing _PepToProtMap.txt file since 'IgnorePeptideToProteinMapError' = True")
			Else
				Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
			End If
		Else
			' The OrgDB (aka fasta file) is not required
			mRetrieveOrganismDB = False
		End If

		m_jobParams.AddResultFileToSkip(FileToGet)

		' Note that we'll obtain the MSGF-DB parameter file in RetrieveMiscFiles

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

	Private Function GetMSAlignFiles() As IJobParams.CloseOutType

		Dim FileToGet As String

		FileToGet = m_DatasetName & "_MSAlign_ResultTable.txt"
		If Not FindAndRetrieveMiscFiles(FileToGet, True) Then
			'Errors were reported in function call, so just return
			Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
		End If
		m_jobParams.AddResultFileToSkip(FileToGet)

		' Note that we'll obtain the MSAlign parameter file in RetrieveMiscFiles

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
	'	Dim ioFolderInfo As System.IO.DirectoryInfo
	'	Dim ioSubfolders() As System.IO.DirectoryInfo
	'	Dim ioFiles() As System.IO.FileInfo

	'	Try
	'		strParamFileStoragePath = m_jobParams.GetParam("ParmFileStoragePath")
	'		ioFolderInfo = New System.IO.DirectoryInfo(strParamFileStoragePath).Parent

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

	'		ioFiles(0).CopyTo(System.IO.Path.Combine(m_WorkingDir, ioFiles(0).Name))

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
		Dim ModDefsFolder As String

		Dim blnSearchArchivedDatasetFolder As Boolean = False
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
			 m_jobParams.GetParam("ParmFileStoragePath"), _
			 m_WorkingDir)

			If Not blnSuccess Then
				m_message = "Error retrieving parameter file and ModDefs.txt file"
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			' Confirm that the file was actually created
			blnSuccess = System.IO.File.Exists(System.IO.Path.Combine(m_WorkingDir, ModDefsFilename))

			If Not blnSuccess And ResultType <> RESULT_TYPE_MSALIGN Then
				m_message = "Unable to create the ModDefs.txt file; update T_Param_File_Mass_Mods"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Unable to create the ModDefs.txt file; define the modifications in table T_Param_File_Mass_Mods for parameter file " & strParamFileName)
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			m_jobParams.AddResultFileToSkip(strParamFileName)
			m_jobParams.AddResultFileToSkip(MASS_CORRECTION_TAGS_FILENAME)

			' Check whether the newly generated ModDefs file matches the existing one
			' If it doesn't match, or if the existing one is missing, then we need keep the file
			' Otherwise, we can skip it
			ModDefsFolder = FindDataFile(ModDefsFilename)
			If String.IsNullOrEmpty(ModDefsFolder) Then
				m_jobParams.AddResultFileToSkip(ModDefsFilename)
			ElseIf ModDefsFolder.ToLower().StartsWith("\\proto") Then
				If clsGlobal.FilesMatch(IO.Path.Combine(m_WorkingDir, ModDefsFilename), IO.Path.Combine(ModDefsFolder, ModDefsFilename)) Then
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
		Dim blnSuccess As Boolean = False

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
				IO.File.Move(IO.Path.Combine(m_WorkingDir, strToolVersionFile), IO.Path.Combine(m_WorkingDir, strToolVersionFileNewName))

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
