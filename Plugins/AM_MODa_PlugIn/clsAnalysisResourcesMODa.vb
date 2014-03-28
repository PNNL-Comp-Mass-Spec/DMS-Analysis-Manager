'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Created 03/26/2014
'
'*********************************************************************************************************

Option Strict On

Imports AnalysisManagerBase

Public Class clsAnalysisResourcesMODa
	Inherits clsAnalysisResources


	Public Overrides Function GetResources() As IJobParams.CloseOutType

		' Make sure the machine has enough free memory to run MODa
		If Not ValidateFreeMemorySize("MODaJavaMemorySize", "MODa") Then
			m_message = "Not enough free memory to run MODa"
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		' Retrieve param file
		If Not RetrieveFile( _
		   m_jobParams.GetParam("ParmFileName"), _
		   m_jobParams.GetParam("ParmFileStoragePath")) _
		Then Return IJobParams.CloseOutType.CLOSEOUT_FAILED

		' Retrieve Fasta file
		If Not RetrieveOrgDB(m_mgrParams.GetParam("orgdbdir")) Then Return IJobParams.CloseOutType.CLOSEOUT_FAILED

		' Retrieve the _DTA.txt file
		' Note that if the file was found in MyEMSL then RetrieveDtaFiles will auto-call ProcessMyEMSLDownloadQueue to download the file

		If Not RetrieveDtaFiles() Then
			Dim sharedResultsFolder = m_jobParams.GetParam("SharedResultsFolders")
			If Not String.IsNullOrEmpty(sharedResultsFolder) Then
				m_message &= "; shared results folder is " & sharedResultsFolder
			End If

			'Errors were reported in function call, so just return
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		If Not MyBase.ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders) Then
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
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

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

End Class
