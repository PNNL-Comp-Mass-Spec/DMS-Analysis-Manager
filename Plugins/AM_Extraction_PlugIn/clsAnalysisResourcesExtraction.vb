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

		'Get analysis results files
		If GetInputFiles(m_jobParams.GetParam("ResultType")) <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		'Get misc files
		If RetrieveMiscFiles() <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
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
	''' <param name="ResultType">String specifying type of analysis results input to extraction process</param>
	''' <returns>IJobParams.CloseOutType specifying results</returns>
	''' <remarks></remarks>
	Private Function GetInputFiles(ByVal ResultType As String) As IJobParams.CloseOutType

		Dim ExtractionSkipsCDTAFile As Boolean
		Dim FileToGet As String

		Try
			Dim strDataset As String = m_jobParams.GetParam("DatasetNum")

			Select Case ResultType
				Case "Peptide_Hit"	'Sequest
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

				Case "XT_Peptide_Hit"
					FileToGet = strDataset & "_xt.zip"
					If Not FindAndRetrieveMiscFiles(FileToGet, True) Then
						'Errors were reported in function call, so just return
						Return IJobParams.CloseOutType.CLOSEOUT_NO_XT_FILES
					End If
					m_jobParams.AddResultFileToSkip(FileToGet)

					'Manually adding this file to FilesToDelete; we don't want the unzipped .xml file to be copied to the server
					m_jobParams.AddResultFileToSkip(strDataset & "_xt.xml")

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

				Case "IN_Peptide_Hit"
					' Get the zipped Inspect results files

					' This file contains the p-value filtered results
					FileToGet = strDataset & "_inspect.zip"
					If Not FindAndRetrieveMiscFiles(FileToGet, False) Then
						'Errors were reported in function call, so just return
						Return IJobParams.CloseOutType.CLOSEOUT_NO_INSP_FILES
					End If
					m_jobParams.AddResultFileToSkip(FileToGet)

					' This file contains top hit for each scan (no filters)
					FileToGet = strDataset & "_inspect_fht.zip"
					If Not FindAndRetrieveMiscFiles(FileToGet, False) Then
						'Errors were reported in function call, so just return
						Return IJobParams.CloseOutType.CLOSEOUT_NO_INSP_FILES
					End If
					m_jobParams.AddResultFileToSkip(FileToGet)

					' Get the peptide to protein mapping file
					FileToGet = strDataset & "_inspect_PepToProtMap.txt"
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

				Case "MSG_Peptide_Hit"
					FileToGet = strDataset & "_msgfdb.zip"
					If Not FindAndRetrieveMiscFiles(FileToGet, True) Then
						'Errors were reported in function call, so just return
						Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
					End If
					m_jobParams.AddResultFileToSkip(FileToGet)

					'Manually adding this file to FilesToDelete; we don't want the unzipped .txt file to be copied to the server
					m_jobParams.AddResultFileToSkip(strDataset & "_msgfdb.txt")

					' Get the peptide to protein mapping file
					FileToGet = strDataset & "_msgfdb_PepToProtMap.txt"
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

				Case Else
					m_message = "Invalid tool result type: " & ResultType
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
					Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End Select

		Catch ex As Exception
			m_message = "Error retrieving input files"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message)
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

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
	Protected Friend Function RetrieveMiscFiles() As IJobParams.CloseOutType

		Dim strParamFileName As String = m_jobParams.GetParam("ParmFileName")
		Dim ModDefsFilename As String = Path.GetFileNameWithoutExtension(strParamFileName) & MOD_DEFS_FILE_SUFFIX

		Dim blnSearchArchivedDatasetFolder As Boolean = False
		Dim blnSuccess As Boolean

		Try

			' Call RetrieveGeneratedParamFile() now to re-create the parameter file, retrieve the _ModDefs.txt file, and retrieve the MassCorrectionTags.txt file
			' Although the ModDefs file should have been created when Sequest, X!Tandem, Inspect, or MSGFDB ran, we re-generate it here just in case T_Param_File_Mass_Mods had missing information
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

			If Not blnSuccess Then
				m_message = "Unable to create the ModDefs.txt file; update T_Param_File_Mass_Mods"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Unable to create the ModDefs.txt file; define the modifications in table T_Param_File_Mass_Mods for parameter file " & strParamFileName)
				Return IJobParams.CloseOutType.CLOSEOUT_FAILED
			End If

			m_jobParams.AddResultFileToSkip(strParamFileName)
			m_jobParams.AddResultFileToSkip(ModDefsFilename)
			m_jobParams.AddResultFileToSkip(MASS_CORRECTION_TAGS_FILENAME)

		Catch ex As Exception
			m_message = "Error retrieving miscellaneous files"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message)
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Try

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function
#End Region

End Class
