'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Created 03/26/2014
'
'*********************************************************************************************************

Option Strict On

Imports AnalysisManagerBase
Imports System.IO

Public Class clsAnalysisResourcesMODa
	Inherits clsAnalysisResources

	Protected WithEvents mDTAtoMGF As DTAtoMGF.clsDTAtoMGF

	Public Overrides Sub Setup(ByRef mgrParams As IMgrParams, ByRef jobParams As IJobParams)
		MyBase.Setup(mgrParams, jobParams)
		SetOption(clsGlobal.eAnalysisResourceOptions.OrgDbRequired, True)
	End Sub

	Public Overrides Sub Setup(mgrParams As IMgrParams, jobParams As IJobParams, statusTools As IStatusFile)
		MyBase.Setup(mgrParams, jobParams, statusTools)
		SetOption(clsGlobal.eAnalysisResourceOptions.OrgDbRequired, True)
	End Sub

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

		' Convert the _dta.txt file to a mgf file
		If Not ConvertCDTAToMGF() Then
			Return IJobParams.CloseOutType.CLOSEOUT_FAILED
		End If

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

	''' <summary>
	''' Convert the _dta.txt file to a .mgf file
	''' </summary>
	''' <returns></returns>
	''' <remarks></remarks>
	Protected Function ConvertCDTAToMGF() As Boolean

		Try
			mDTAtoMGF = New DTAtoMGF.clsDTAtoMGF()
			mDTAtoMGF.Combine2And3PlusCharges = False
			mDTAtoMGF.FilterSpectra = False
			mDTAtoMGF.MaximumIonsPer100MzInterval = 0
			mDTAtoMGF.NoMerge = True
			mDTAtoMGF.CreateIndexFile = True

			' Convert the _dta.txt file for this dataset
			Dim fiCDTAFile As FileInfo = New FileInfo(Path.Combine(m_WorkingDir, m_DatasetName & "_dta.txt"))

			If Not fiCDTAFile.Exists Then
				m_message = "_dta.txt file not found; cannot convert to .mgf"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & fiCDTAFile.FullName)
				Return False
			End If

			If Not mDTAtoMGF.ProcessFile(fiCDTAFile.FullName) Then
				m_message = "Error converting " & fiCDTAFile.Name & " to a .mgf file"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & mDTAtoMGF.GetErrorMessage())
				Return False
			Else
				' Delete the _dta.txt file
				Try
					fiCDTAFile.Delete()
				Catch ex As Exception
					' Ignore errors here
				End Try
			End If

			Threading.Thread.Sleep(125)
			PRISM.Processes.clsProgRunner.GarbageCollectNow()

			Dim fiNewMGFFile As FileInfo
			fiNewMGFFile = New FileInfo(Path.Combine(m_WorkingDir, m_DatasetName & ".mgf"))

			If Not fiNewMGFFile.Exists Then
				' MGF file was not created
				m_message = "A .mgf file was not created using the _dta.txt file; unable to run MODa"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & mDTAtoMGF.GetErrorMessage())
				Return False
			End If

			m_jobParams.AddResultFileExtensionToSkip(".mgf")			

		Catch ex As Exception
			m_message = "Exception in ConvertCDTAToMGF"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex)
			Return False
		End Try

		Return True

	End Function

End Class
