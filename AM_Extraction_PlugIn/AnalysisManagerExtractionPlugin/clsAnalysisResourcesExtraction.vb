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

		Select Case ResultType
			Case "Peptide_Hit"	'Sequest
				ExtractionSkipsCDTAFile = clsGlobal.CBoolSafe(m_jobParams.GetParam("ExtractionSkipsCDTAFile"))

				If ExtractionSkipsCDTAFile Then
					' Do not grab the _Dta.txt file
				Else
					'Get the concatenated .dta file
					If Not RetrieveDtaFiles(False) Then
						'Errors were reported in function call, so just return
						Return IJobParams.CloseOutType.CLOSEOUT_NO_DTA_FILES
					End If
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

				Dim ext As String
				Dim DumFiles() As String

				'update list of files to be deleted after run
				For Each ext In clsGlobal.m_FilesToDeleteExt
					DumFiles = Directory.GetFiles(m_mgrParams.GetParam("workdir"), "*" & ext) 'Zipped DTA (and others)
					For Each FileToDel As String In DumFiles
						clsGlobal.FilesToDelete.Add(FileToDel)
					Next
				Next

			Case "XT_Peptide_Hit"
                FileToGet = m_jobParams.GetParam("DatasetNum") & "_xt.zip"
                If Not FindAndRetrieveMiscFiles(FileToGet, True) Then
                    'Errors were reported in function call, so just return
                    Return IJobParams.CloseOutType.CLOSEOUT_NO_XT_FILES
                End If
                clsGlobal.FilesToDelete.Add(FileToGet)

                'Manually adding this file to FilesToDelete; we don't want the unzipped .txt file to be copied to the server
                clsGlobal.FilesToDelete.Add(m_jobParams.GetParam("DatasetNum") & "_xt.xml")

                ' Get the X!Tandem parameter file
                FileToGet = m_jobParams.GetParam("ParmFileName")
                If Not FindAndRetrieveMiscFiles(FileToGet, False) Then
                    'Errors were reported in function call, so just return
                    Return IJobParams.CloseOutType.CLOSEOUT_NO_PARAM_FILE
                End If
                clsGlobal.FilesToDelete.Add(FileToGet)

            Case "IN_Peptide_Hit"
                ' Get the Inspect results file
                FileToGet = m_jobParams.GetParam("DatasetNum") & "_inspect.zip"
                If Not FindAndRetrieveMiscFiles(FileToGet, True) Then
                    'Errors were reported in function call, so just return
                    Return IJobParams.CloseOutType.CLOSEOUT_NO_INSP_FILES
                End If
                clsGlobal.FilesToDelete.Add(FileToGet)

                'Manually adding this file to FilesToDelete; we don't want the unzipped .txt file to be copied to the server
                clsGlobal.FilesToDelete.Add(m_jobParams.GetParam("DatasetNum") & "_inspect.txt")

                ' Get the peptide to protein mapping file
                FileToGet = m_jobParams.GetParam("DatasetNum") & "_inspect_PepToProtMap.txt"
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

		Dim ModDefsFilename As String = Path.GetFileNameWithoutExtension(m_jobParams.GetParam("ParmFileName")) & MOD_DEFS_FILE_SUFFIX

		'Mod Defs file
		If Not FindAndRetrieveMiscFiles(ModDefsFilename, False) Then
			'Errors were reported in function call, so just return
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If
        clsGlobal.FilesToDelete.Add(ModDefsFilename)

		'Mass correction tags file
		If Not FindAndRetrieveMiscFiles(MASS_CORRECTION_TAGS_FILENAME, False) Then
			'Errors were reported in function call, so just return
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If
        clsGlobal.FilesToDelete.Add(MASS_CORRECTION_TAGS_FILENAME)

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function
#End Region

End Class
