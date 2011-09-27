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
Imports System.Collections.Specialized
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
	Public Overrides Function GetResources() As AnalysisManagerBase.IJobParams.CloseOutType

        'Clear out list of files to delete or keep when packaging the results
        clsGlobal.ResetFilesToDeleteOrKeep()

		'Get analysis results files
		If GetInputFiles(m_jobParams.GetParam("ResultType")) <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
			'TODO: Handle error
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		'Get misc files
		If RetrieveMiscFiles() <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
			'TODO: Handle error
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

	''' <summary>
	''' Retrieves input files (ie, .out files) needed for extraction
	''' </summary>
	''' <param name="ResultType">String specifying type of analysis results input to extraction process</param>
	''' <returns>IJobParams.CloseOutType specifying results</returns>
	''' <remarks></remarks>
	Private Function GetInputFiles(ByVal ResultType As String) As AnalysisManagerBase.IJobParams.CloseOutType

		Dim ExtractionSkipsCDTAFile As Boolean
        Dim FileToGet As String

        Dim strDataset As String = m_jobParams.GetParam("DatasetNum")

		Select Case ResultType
			Case "Peptide_Hit"	'Sequest
				ExtractionSkipsCDTAFile = clsGlobal.CBoolSafe(m_jobParams.GetParam("ExtractionSkipsCDTAFile"))

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

				' Get the Sequest parameter file
				FileToGet = m_jobParams.GetParam("ParmFileName")
				If Not FindAndRetrieveMiscFiles(FileToGet, False) Then
					'Errors were reported in function call, so just return
					Return IJobParams.CloseOutType.CLOSEOUT_NO_PARAM_FILE
				End If
				clsGlobal.FilesToDelete.Add(FileToGet)

				'Add all the extensions of the files to delete after run
				clsGlobal.m_FilesToDeleteExt.Add("_dta.zip") 'Zipped DTA
                clsGlobal.m_FilesToDeleteExt.Add("_dta.txt") 'Unzipped, concatenated DTA
                clsGlobal.m_FilesToDeleteExt.Add("_out.zip") 'Zipped OUT
				clsGlobal.m_FilesToDeleteExt.Add("_out.txt") 'Unzipped, concatenated OUT
				clsGlobal.m_FilesToDeleteExt.Add(".dta")  'DTA files
				clsGlobal.m_FilesToDeleteExt.Add(".out")  'DTA files

			Case "XT_Peptide_Hit"
                FileToGet = strDataset & "_xt.zip"
                If Not FindAndRetrieveMiscFiles(FileToGet, True) Then
                    'Errors were reported in function call, so just return
                    Return IJobParams.CloseOutType.CLOSEOUT_NO_XT_FILES
                End If
                clsGlobal.FilesToDelete.Add(FileToGet)

                'Manually adding this file to FilesToDelete; we don't want the unzipped .xml file to be copied to the server
                clsGlobal.FilesToDelete.Add(strDataset & "_xt.xml")

                ' Get the X!Tandem parameter file
                FileToGet = m_jobParams.GetParam("ParmFileName")
                If Not FindAndRetrieveMiscFiles(FileToGet, False) Then
                    'Errors were reported in function call, so just return
                    Return IJobParams.CloseOutType.CLOSEOUT_NO_PARAM_FILE
                End If
                clsGlobal.FilesToDelete.Add(FileToGet)

            Case "IN_Peptide_Hit"
                ' Get the zipped Inspect results files

                ' This file contains the p-value filtered results
                FileToGet = strDataset & "_inspect.zip"
                If Not FindAndRetrieveMiscFiles(FileToGet, False) Then
                    'Errors were reported in function call, so just return
                    Return IJobParams.CloseOutType.CLOSEOUT_NO_INSP_FILES
                End If
                clsGlobal.FilesToDelete.Add(FileToGet)

                ' This file contains top hit for each scan (no filters)
                FileToGet = strDataset & "_inspect_fht.zip"
                If Not FindAndRetrieveMiscFiles(FileToGet, False) Then
                    'Errors were reported in function call
                    ' Don't treat this as a critical error
                End If
                clsGlobal.FilesToDelete.Add(FileToGet)

                ' Get the peptide to protein mapping file
                FileToGet = strDataset & "_inspect_PepToProtMap.txt"
                If Not FindAndRetrieveMiscFiles(FileToGet, False) Then
                    'Errors were reported in function call

                    ' See if IgnorePeptideToProteinMapError=True
                    If AnalysisManagerBase.clsGlobal.CBoolSafe(m_jobParams.GetParam("IgnorePeptideToProteinMapError")) Then
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Ignoring missing _PepToProtMap.txt file since 'IgnorePeptideToProteinMapError' = True")
                    Else
                        Return IJobParams.CloseOutType.CLOSEOUT_NO_INSP_FILES
                    End If
                End If
                clsGlobal.FilesToDelete.Add(FileToGet)

                ' Get the Inspect parameter file
                FileToGet = m_jobParams.GetParam("ParmFileName")
                If Not FindAndRetrieveMiscFiles(FileToGet, False) Then
                    'Errors were reported in function call, so just return
                    Return IJobParams.CloseOutType.CLOSEOUT_NO_PARAM_FILE
                End If
                clsGlobal.FilesToDelete.Add(FileToGet)

            Case "MSG_Peptide_Hit"
                FileToGet = strDataset & "_msgfdb.zip"
                If Not FindAndRetrieveMiscFiles(FileToGet, True) Then
                    'Errors were reported in function call, so just return
                    Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
                End If
                clsGlobal.FilesToDelete.Add(FileToGet)

                'Manually adding this file to FilesToDelete; we don't want the unzipped .txt file to be copied to the server
                clsGlobal.FilesToDelete.Add(strDataset & "_msgfdb.txt")

				' Get the peptide to protein mapping file
				FileToGet = strDataset & "_msgfdb_PepToProtMap.txt"
				If Not FindAndRetrieveMiscFiles(FileToGet, False) Then
					'Errors were reported in function call

					' See if IgnorePeptideToProteinMapError=True
					If AnalysisManagerBase.clsGlobal.CBoolSafe(m_jobParams.GetParam("IgnorePeptideToProteinMapError")) Then
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Ignoring missing _PepToProtMap.txt file since 'IgnorePeptideToProteinMapError' = True")
					Else
						Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
					End If
				End If
				clsGlobal.FilesToDelete.Add(FileToGet)

                ' Get the MSGF-DB parameter file
                FileToGet = m_jobParams.GetParam("ParmFileName")
                If Not FindAndRetrieveMiscFiles(FileToGet, False) Then
                    'Errors were reported in function call, so just return
                    Return IJobParams.CloseOutType.CLOSEOUT_NO_PARAM_FILE
                End If
                clsGlobal.FilesToDelete.Add(FileToGet)

			Case Else
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Invalid tool result type: " & ResultType)
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End Select

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

	''' <summary>
	''' Retrieves misc files (ie, ModDefs) needed for extraction
	''' </summary>
	''' <returns>IJobParams.CloseOutType specifying results</returns>
	''' <remarks></remarks>
	Protected Friend Function RetrieveMiscFiles() As IJobParams.CloseOutType

        Dim strParamFileName As String = m_jobParams.GetParam("ParmFileName")
        Dim ModDefsFilename As String = Path.GetFileNameWithoutExtension(strParamFileName) & MOD_DEFS_FILE_SUFFIX

		'Mod Defs file
        If FindAndRetrieveMiscFiles(ModDefsFilename, False) Then
            clsGlobal.FilesToDelete.Add(ModDefsFilename)

            'Mass correction tags file
            If Not FindAndRetrieveMiscFiles(MASS_CORRECTION_TAGS_FILENAME, False) Then
                'Errors were reported in function call, so just return
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            clsGlobal.FilesToDelete.Add(MASS_CORRECTION_TAGS_FILENAME)
        Else

            ' The ModDefs.txt file should have been created when Sequest, X!Tandem, Inspect, or MSGFDB ran
            ' However, if the mods were not defined in T_Param_File_Mass_Mods then the file will not have been created
            '
            ' We will go ahead and call RetrieveGeneratedParamFile() now to take a second shot at creating the _ModDefs.txt file
            ' The file is obtained using this query:
            '  SELECT Local_Symbol, Monoisotopic_Mass_Correction, Residue_Symbol, Mod_Type_Symbol, Mass_Correction_Tag
            '  FROM V_Param_File_Mass_Mod_Info 
            '  WHERE Param_File_Name = 'ParamFileName'

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "ModDefs.txt file not found; will try to generate it using the ParamFileGenerator")

            Dim blnSuccess As Boolean

            blnSuccess = RetrieveGeneratedParamFile(strParamFileName, _
                                                    m_jobParams.GetParam("ParmFileStoragePath"), _
                                                    m_WorkingDir)

            If blnSuccess Then
                ' Confirm that the file was actually created
                blnSuccess = System.IO.File.Exists(System.IO.Path.Combine(m_WorkingDir, ModDefsFilename))
            End If

            If Not blnSuccess Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Unable to create the ModDefs.txt file; define the modifications in table T_Param_File_Mass_Mods for parameter file " & strParamFileName)
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

        End If

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function
#End Region

End Class
