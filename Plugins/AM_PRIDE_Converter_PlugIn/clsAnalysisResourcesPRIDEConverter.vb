Option Strict On

Imports AnalysisManagerBase

Public Class clsAnalysisResourcesPRIDEConverter
	Inherits clsAnalysisResources

	Public Const JOB_PARAM_DATASETS_MISSING_MZXML_FILES As String = "PackedParam_DatasetsMissingMzXMLFiles"
	Public Const JOB_PARAM_DATASET_PACKAGE_JOBS_AND_DATASETS As String = "PackedParam_DataPackageJobsAndDatasets"

	Public Overrides Function GetResources() As IJobParams.CloseOutType

		' Retrieve the parameter file
		Dim strParamFileName As String = m_jobParams.GetParam("ParmFileName")
		Dim strParamFileStoragePath As String = m_jobParams.GetParam("ParmFileStoragePath")

		If Not String.IsNullOrEmpty(strParamFileName) AndAlso strParamFileName <> "na" Then
			If Not RetrieveFile(strParamFileName, strParamFileStoragePath, m_WorkingDir) Then
				Return IJobParams.CloseOutType.CLOSEOUT_NO_PARAM_FILE
			End If
		End If

		Dim lstDataPackagePeptideHitJobs As System.Collections.Generic.List(Of udtDataPackageJobInfoType)
		lstDataPackagePeptideHitJobs = New System.Collections.Generic.List(Of udtDataPackageJobInfoType)

		' Obtain the PHRP-related files for the Peptide_Hit jobs defined for the data package associated with this data aggregation job
		' In addition, obtain the .mzXML file or .Raw file for each dataset
		If Not MyBase.RetrieveDataPackagePeptideHitJobPHRPFiles(True, lstDataPackagePeptideHitJobs) Then
			Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
		End If

		' Use lstDataPackagePeptideHitJobs to look for any datasets for which we will need to create a .mzXML file
		FindMissingMzXmlFiles(lstDataPackagePeptideHitJobs)

		StoreDataPackageJobInfo(lstDataPackagePeptideHitJobs)

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

	''' <summary>
	''' Find datasets that do not have a .mzXML file
	''' </summary>
	''' <param name="lstDataPackagePeptideHitJobs"></param>
	''' <remarks></remarks>
	Protected Sub FindMissingMzXmlFiles(ByVal lstDataPackagePeptideHitJobs As System.Collections.Generic.List(Of udtDataPackageJobInfoType))

		Dim lstDatasets As SortedSet(Of String) = New SortedSet(Of String)

		Try
			For Each udtJob As udtDataPackageJobInfoType In lstDataPackagePeptideHitJobs
				Dim strMzXmlFilePath As String
				strMzXmlFilePath = IO.Path.Combine(m_WorkingDir, udtJob.Dataset & DOT_MZXML_EXTENSION)

				If Not IO.File.Exists(strMzXmlFilePath) Then
					If Not lstDatasets.Contains(udtJob.Dataset) Then
						lstDatasets.Add(udtJob.Dataset)
					End If
				End If
			Next

			If lstDatasets.Count > 0 Then
				StorePackedJobParameterList(lstDatasets.ToList(), clsAnalysisResourcesPRIDEConverter.JOB_PARAM_DATASETS_MISSING_MZXML_FILES)
			End If

		Catch ex As Exception
			m_message = "Exception in FindMissingMzXmlFiles"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message)
		End Try

	End Sub

	''' <summary>
	''' Store the datasets and jobs tracked by lstDataPackagePeptideHitJobs into a packed job parameter
	''' </summary>
	''' <param name="lstDataPackagePeptideHitJobs"></param>
	''' <remarks></remarks>
	Protected Sub StoreDataPackageJobInfo(ByVal lstDataPackagePeptideHitJobs As System.Collections.Generic.List(Of udtDataPackageJobInfoType))
		Dim lstDataPackageJobs As Generic.List(Of String) = New Generic.List(Of String)

		For Each udtJob As udtDataPackageJobInfoType In lstDataPackagePeptideHitJobs
			lstDataPackageJobs.Add(udtJob.Job.ToString() & "|" & udtJob.Dataset)
		Next

		If lstDataPackageJobs.Count > 0 Then
			StorePackedJobParameterList(lstDataPackageJobs.ToList(), clsAnalysisResourcesPRIDEConverter.JOB_PARAM_DATASET_PACKAGE_JOBS_AND_DATASETS)

		End If
	End Sub
	
	''' <summary>
	''' Convert a string list to a packed job parameter
	''' </summary>
	''' <param name="lstItems"></param>
	''' <param name="strParameterName"></param>
	''' <remarks></remarks>
	Protected Sub StorePackedJobParameterList(ByVal lstItems As Generic.List(Of String), ByVal strParameterName As String)
		Dim sbPackedList As Text.StringBuilder = New Text.StringBuilder

		For Each strDataset As String In lstItems
			If sbPackedList.Length > 0 Then sbPackedList.Append(ControlChars.Tab)
			sbPackedList.Append(strDataset)
		Next

		m_jobParams.AddAdditionalParameter("JobParameters", strParameterName, sbPackedList.ToString())
	End Sub
End Class
