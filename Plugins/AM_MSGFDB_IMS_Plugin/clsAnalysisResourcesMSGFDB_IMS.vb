'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Created 10/12/2011
'
'*********************************************************************************************************

Option Strict On

Imports AnalysisManagerBase

Public Class clsAnalysisResourcesMSGFDB_IMS
	Inherits clsAnalysisResources

	Public Overrides Function GetResources() As IJobParams.CloseOutType

		' Make sure the machine has enough free memory to run MSGFDB_IMS
		If Not ValidateFreeMemorySize("MSGFDBJavaMemorySize", "MSGFDB") Then
			m_message = "Not enough free memory to run MSGFDB_IMS"
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		Dim strRawDataType As String = m_jobParams.GetParam("RawDataType")
		Dim eRawDataType As clsAnalysisResources.eRawDataTypeConstants
		eRawDataType = clsAnalysisResources.GetRawDataType(strRawDataType)

		If eRawDataType <> eRawDataTypeConstants.UIMF Then
			m_message = "Dataset type is not compatible with MSGFDB_IMS: " & strRawDataType
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
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
		   m_mgrParams.GetParam("workdir")) _
		Then
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		' Get the UIMF file for this dataset
		If Not RetrieveSpectra(strRawDataType, m_WorkingDir) Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisResourcesDecon2ls.GetResources: Error occurred retrieving spectra.")
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		If Not RetrieveDeconToolsResults() Then
			Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
		End If

		'Add all the extensions of the files to delete after run
		m_jobParams.AddResultFileExtensionToSkip(DOT_UIMF_EXTENSION)
		m_jobParams.AddResultFileExtensionToSkip("_isos.csv")
		m_jobParams.AddResultFileExtensionToSkip("_scans.csv")
		m_jobParams.AddResultFileExtensionToSkip("_peaks.txt")
		m_jobParams.AddResultFileExtensionToSkip("_peaks.zip")

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

	Protected Function RetrieveDeconToolsResults() As Boolean

		' The Input_Folder for this job step should have been auto-defined by the DMS_Pipeline database using the Special_Processing parameters
		' For example, for dataset BSA_10ugml_IMS6_TOF03_CID_13Aug12_Frodo using Special_Processing of
		'   SourceJob:Auto{Tool = "Decon2LS_V2" AND [Parm File] = "IMS_UIMF_PeakBR2_PeptideBR4_SN3_SumScans4_SumFrames3_noFit_Thrash_WithPeaks_2012-05-09.xml"}
		' Gives these parameters:

		' SourceJob                     = 852150
		' InputFolderName               = "DLS201206180954_Auto852150"
		' DatasetStoragePath            = \\proto-3\LTQ_Orb_3\2011_1\
		' DatasetArchivePath            = \\a2.emsl.pnl.gov\dmsarch\LTQ_Orb_3\2011_1

		Dim strDeconToolsFolderName As String
		strDeconToolsFolderName = m_jobParams.GetParam("StepParameters", "InputFolderName")

		If String.IsNullOrEmpty(strDeconToolsFolderName) Then
			m_message = "InputFolderName step parameter not found; this is auto-determined by the SourceJob SpecialProcessing text"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
			Return False

		ElseIf Not strDeconToolsFolderName.ToUpper().StartsWith("DLS") Then
			m_message = "InputFolderName step parameter is not a DeconTools folder; it should start with DLS and is auto-determined by the SourceJob SpecialProcessing text"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
			Return False
		End If

		Dim intSourceJob As Integer
		intSourceJob = m_jobParams.GetJobParameter("SourceJob", 0)

		If intSourceJob = 0 Then
			m_message = "SourceJob parameter not found; this is auto-defined  by the SourceJob SpecialProcessing text"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
			Return False
		End If

		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Getting DeconTools result files for job " & intSourceJob)

		If Not FindAndRetrieveMiscFiles(m_DatasetName & "_isos.csv", Unzip:=False) Then
			Return False
		End If

		If Not FindAndRetrieveMiscFiles(m_DatasetName & "_scans.csv", Unzip:=False) Then
			Return False
		End If

		Dim strPeaksFileName As String = m_DatasetName & "_peaks.zip"
		Dim strMatchedPath As String

		' First look for the zipped version of the _peaks.txt file
		strMatchedPath = FindDataFile(strPeaksFileName, SearchArchivedDatasetFolder:=True, LogFileNotFound:=False)
		If Not String.IsNullOrEmpty(strMatchedPath) Then
			' Zipped version found; retrieve it
			If Not FindAndRetrieveMiscFiles(strPeaksFileName, Unzip:=True) Then
				Return False
			End If
		Else
			strPeaksFileName = m_DatasetName & "_peaks.txt"
			If Not FindAndRetrieveMiscFiles(strPeaksFileName, Unzip:=False) Then
				Return False
			End If
		End If

		Return True

	End Function


End Class
