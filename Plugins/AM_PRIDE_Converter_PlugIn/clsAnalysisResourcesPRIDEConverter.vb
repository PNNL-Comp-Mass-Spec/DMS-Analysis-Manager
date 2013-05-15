Option Strict On

Imports AnalysisManagerBase

Public Class clsAnalysisResourcesPRIDEConverter
	Inherits clsAnalysisResources

	Public Const JOB_PARAM_DATASETS_MISSING_MZXML_FILES As String = "PackedParam_DatasetsMissingMzXMLFiles"
	Public Const JOB_PARAM_DATA_PACKAGE_PEPTIDE_HIT_JOBS As String = "PackedParam_DataPackagePeptideHitJobs"
	Public Const JOB_PARAM_DICTIONARY_DATASET_STORAGE_YEAR_QUARTER As String = "PackedParam_DatasetStorage_YearQuarter"

	Public Const JOB_PARAM_MSGF_REPORT_TEMPLATE_FILENAME As String = "MSGFReportFileTemplate"

	Public Const DEFAULT_MSGF_REPORT_TEMPLATE_FILENAME As String = "Template.msgf-report.xml"
	Public Const MSGF_REPORT_FILE_SUFFIX As String = "msgf-report.xml"

	Public Overrides Function GetResources() As IJobParams.CloseOutType

		Dim lstDataPackagePeptideHitJobs As Generic.List(Of udtDataPackageJobInfoType)
		lstDataPackagePeptideHitJobs = New Generic.List(Of udtDataPackageJobInfoType)

		Dim blnCreatePrideXMLFiles As Boolean = m_jobParams.GetJobParameter("CreatePrideXMLFiles", False)

		' Check whether we are only creating the .msgf files
		Dim blnCreateMSGFReportFilesOnly As Boolean = m_jobParams.GetJobParameter("CreateMSGFReportFilesOnly", False)
		Dim udtOptions As udtDataPackageRetrievalOptionsType

		udtOptions.CreateJobPathFiles = True

		If blnCreatePrideXMLFiles And Not blnCreateMSGFReportFilesOnly Then
			udtOptions.RetrieveMzXMLFile = True
		Else
			udtOptions.RetrieveMzXMLFile = False
		End If

		udtOptions.RetrieveDTAFiles = m_jobParams.GetJobParameter("CreateMGFFiles", True)
		udtOptions.RetrieveMZidFiles = m_jobParams.GetJobParameter("IncludeMZidFiles", True)

		If blnCreateMSGFReportFilesOnly Then
			udtOptions.RetrieveDTAFiles = False
			udtOptions.RetrieveMZidFiles = False
			blnCreatePrideXMLFiles = False
		Else
			If Not RetrieveMSGFReportTemplateFile() Then
				Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
			End If
		End If

		' Obtain the PHRP-related files for the Peptide_Hit jobs defined for the data package associated with this data aggregation job
		' Possibly also obtain the .mzXML file or .Raw file for each dataset
		' The .mzXML file is required if we are creating Pride XML files (which are required for a "complete" submission, though as of May 2013 Attila Csordas no longer wants these files)
		If Not MyBase.RetrieveDataPackagePeptideHitJobPHRPFiles(udtOptions, lstDataPackagePeptideHitJobs) Then
			Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
		End If

		' Obtain the FASTA files (typically generated from protein collections) used for the jobs in lstDataPackagePeptideHitJobs
		If Not RetrieveFastaFiles(lstDataPackagePeptideHitJobs) Then
			Return IJobParams.CloseOutType.CLOSEOUT_NO_FAS_FILES
		End If

		If udtOptions.RetrieveMzXMLFile Then
			' Use lstDataPackagePeptideHitJobs to look for any datasets for which we will need to create a .mzXML file
			FindMissingMzXmlFiles(udtOptions, lstDataPackagePeptideHitJobs)
		End If

		StoreDataPackageJobs(lstDataPackagePeptideHitJobs)

		Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

	End Function

	''' <summary>
	''' Find datasets that do not have a .mzXML file
	''' Datasets that need to have .mzXML files created will be added to the packed job parameters, storing the dataset names in "PackedParam_DatasetsMissingMzXMLFiles"
	''' and the dataset Year_Quarter values in "PackedParam_DatasetStorage_YearQuarter"
	''' </summary>
	''' <param name="udtOptions">File retrieval options</param>
	''' <param name="lstDataPackagePeptideHitJobs"></param>
	''' <remarks></remarks>
	Protected Sub FindMissingMzXmlFiles(ByVal udtOptions As udtDataPackageRetrievalOptionsType, ByVal lstDataPackagePeptideHitJobs As Generic.List(Of udtDataPackageJobInfoType))

		Dim lstDatasets As SortedSet(Of String) = New SortedSet(Of String)
		Dim lstDatasetYearQuarter As SortedSet(Of String) = New SortedSet(Of String)

		Try
			For Each udtJob As udtDataPackageJobInfoType In lstDataPackagePeptideHitJobs
				Dim strMzXmlFilePath As String
				strMzXmlFilePath = IO.Path.Combine(m_WorkingDir, udtJob.Dataset & DOT_MZXML_EXTENSION)

				If Not IO.File.Exists(strMzXmlFilePath) Then

					' Look for a StoragePathInfo file
					strMzXmlFilePath &= STORAGE_PATH_INFO_FILE_SUFFIX
					If Not IO.File.Exists(strMzXmlFilePath) Then
						If Not lstDatasets.Contains(udtJob.Dataset) Then
							lstDatasets.Add(udtJob.Dataset)
							lstDatasetYearQuarter.Add(udtJob.Dataset & "=" & clsAnalysisResources.GetDatasetYearQuarter(udtJob.ServerStoragePath))
						End If
					End If

				End If
			Next

			If lstDatasets.Count > 0 Then
				StorePackedJobParameterList(lstDatasets.ToList(), JOB_PARAM_DATASETS_MISSING_MZXML_FILES)
				StorePackedJobParameterList(lstDatasetYearQuarter.ToList(), JOB_PARAM_DICTIONARY_DATASET_STORAGE_YEAR_QUARTER)
			End If

		Catch ex As Exception
			m_message = "Exception in FindMissingMzXmlFiles"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message)
		End Try

	End Sub

	Public Shared Function GetGeneratedFastaParamNameForJob(ByVal Job As Integer) As String
		Return "Job" & Job.ToString() & "_GeneratedFasta"
	End Function

	Public Shared Function GetMSGFReportTemplateFilename(ByVal JobParams As IJobParams, ByVal WarnIfJobParamMissing As Boolean) As String

		Dim strTemplateFileName As String = JobParams.GetJobParameter(JOB_PARAM_MSGF_REPORT_TEMPLATE_FILENAME, String.Empty)

		If String.IsNullOrEmpty(strTemplateFileName) Then
			If WarnIfJobParamMissing Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Job parameter " & JOB_PARAM_MSGF_REPORT_TEMPLATE_FILENAME & " is empty; will assume " & strTemplateFileName)
			End If
			strTemplateFileName = DEFAULT_MSGF_REPORT_TEMPLATE_FILENAME
		End If

		Return strTemplateFileName

	End Function

	Protected Function RetrieveFastaFiles(ByVal lstDataPackagePeptideHitJobs As Generic.List(Of udtDataPackageJobInfoType)) As Boolean

		Dim udtCurrentDatasetAndJobInfo As udtDataPackageJobInfoType

		Dim strLocalOrgDBFolder As String = m_mgrParams.GetParam("orgdbdir")

		Dim strDictionaryKey As String

		Dim strOrgDBNameGenerated As String = String.Empty

		' This dictionary is used to avoid calling RetrieveOrgDB() for every job
		' The dictionary keys are LegacyFastaFileName, ProteinOptions, and ProteinCollectionList combined with underscores
		' The dictionary values are the name of the generated (or retrieved) fasta file
		Dim dctOrgDBParamsToGeneratedFileNameMap As Generic.Dictionary(Of String, String)

		Try
			dctOrgDBParamsToGeneratedFileNameMap = New Generic.Dictionary(Of String, String)

			' Cache the current dataset and job info
			udtCurrentDatasetAndJobInfo = GetCurrentDatasetAndJobInfo()

			For Each udtJob As udtDataPackageJobInfoType In lstDataPackagePeptideHitJobs

				strDictionaryKey = udtJob.LegacyFastaFileName & "_" & udtJob.ProteinCollectionList & "_" & udtJob.ProteinOptions

				If dctOrgDBParamsToGeneratedFileNameMap.TryGetValue(strDictionaryKey, strOrgDBNameGenerated) Then
					' Organism DB was already generated
				Else
					OverrideCurrentDatasetAndJobInfo(udtJob)

					m_jobParams.AddAdditionalParameter("PeptideSearch", "generatedFastaName", String.Empty)
					If Not RetrieveOrgDB(strLocalOrgDBFolder) Then
						If String.IsNullOrEmpty(m_message) Then m_message = "Call to RetrieveOrgDB returned false in clsAnalysisResourcesPRIDEConverter.RetrieveFastaFiles"
						Return False
					End If

					strOrgDBNameGenerated = m_jobParams.GetJobParameter("PeptideSearch", "generatedFastaName", String.Empty)

					If String.IsNullOrEmpty(strOrgDBNameGenerated) Then
						m_message = "FASTA file was not generated when RetrieveFastaFiles called RetrieveOrgDB"
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & " (class clsAnalysisResourcesPRIDEConverter)")
						Return False
					End If

					If strOrgDBNameGenerated <> udtJob.OrganismDBName Then
						If strOrgDBNameGenerated Is Nothing Then strOrgDBNameGenerated = "??"
						If udtJob.OrganismDBName Is Nothing Then udtJob.OrganismDBName = "??"

						m_message = "Generated FASTA file name (" & strOrgDBNameGenerated & ") does not match expected fasta file name (" & udtJob.OrganismDBName & "); aborting"
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & " (class clsAnalysisResourcesPRIDEConverter)")
						Return False
					End If

					dctOrgDBParamsToGeneratedFileNameMap.Add(strDictionaryKey, strOrgDBNameGenerated)
				End If

				' Add a new job parameter that associates strOrgDBNameGenerated with this job

				m_jobParams.AddAdditionalParameter("PeptideSearch", GetGeneratedFastaParamNameForJob(udtJob.Job), strOrgDBNameGenerated)
			Next

			' Restore the dataset and job info for this aggregation job
			OverrideCurrentDatasetAndJobInfo(udtCurrentDatasetAndJobInfo)

		Catch ex As Exception
			m_message = "Exception in RetrieveFastaFiles"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex)
			Return False
		End Try

		Return True

	End Function

	Protected Function RetrieveMSGFReportTemplateFile() As Boolean

		' Retrieve the template .msgf-pride.xml file
		' Although there is a default in the PRIDE_Converter parameter file folder, it should ideally be customized and placed in the data package folder

		Dim strTemplateFileName As String
		Dim diDataPackageFolder As IO.DirectoryInfo
		Dim fiFiles As Generic.List(Of IO.FileInfo)

		Try
			strTemplateFileName = GetMSGFReportTemplateFilename(m_jobParams, WarnIfJobParamMissing:=True)

			' First look for the template file in the data package folder
			Dim strDataPackagePath As String = m_jobParams.GetJobParameter("JobParameters", "transferFolderPath", String.Empty)
			If String.IsNullOrEmpty(strDataPackagePath) Then
				m_message = "Job parameter transferFolderPath is missing; unable to determine the data package folder path"
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
				Return False
			End If

			diDataPackageFolder = New IO.DirectoryInfo(strDataPackagePath)
			fiFiles = diDataPackageFolder.GetFiles(strTemplateFileName).ToList()

			If fiFiles.Count = 0 Then
				' File not found; see if any files ending in MSGF_REPORT_FILE_SUFFIX exist in the data package folder
				fiFiles = diDataPackageFolder.GetFiles("*" & MSGF_REPORT_FILE_SUFFIX).ToList()

				If fiFiles.Count = 0 Then
					' File not found; see if any files containin MSGF_REPORT_FILE_SUFFIX exist in the data package folder
					fiFiles = diDataPackageFolder.GetFiles("*" & MSGF_REPORT_FILE_SUFFIX & "*").ToList()

				End If
			End If


			If fiFiles.Count > 0 Then
				' Template file found in the data package; copy it locally
				If Not RetrieveFile(fiFiles(0).Name, fiFiles(0).DirectoryName, m_WorkingDir) Then
					Return False
				Else
					strTemplateFileName = fiFiles(0).Name
				End If
			Else
				Dim strParamFileStoragePath As String = m_jobParams.GetParam("ParmFileStoragePath")
				strTemplateFileName = DEFAULT_MSGF_REPORT_TEMPLATE_FILENAME

				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "MSGF Report template file not found in the data package folder; retrieving " & strTemplateFileName & "from " & strParamFileStoragePath)

				If Not RetrieveFile(strTemplateFileName, strParamFileStoragePath, m_WorkingDir) Then
					Return False
				End If
			End If

			' Assure that the MSGF Report Template file job parameter is up-to-date
			m_jobParams.AddAdditionalParameter("JobParameters", JOB_PARAM_MSGF_REPORT_TEMPLATE_FILENAME, strTemplateFileName)

			m_jobParams.AddResultFileToSkip(strTemplateFileName)

		Catch ex As Exception
			m_message = "Exception in RetrieveMSGFReportTemplateFile"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex)
			Return False
		End Try

		Return True

	End Function

	''' <summary>
	''' Store the datasets and jobs tracked by lstDataPackagePeptideHitJobs into a packed job parameter
	''' </summary>
	''' <param name="lstDataPackagePeptideHitJobs"></param>
	''' <remarks></remarks>
	Protected Sub StoreDataPackageJobs(ByVal lstDataPackagePeptideHitJobs As Generic.List(Of udtDataPackageJobInfoType))
		Dim lstDataPackageJobs As Generic.List(Of String) = New Generic.List(Of String)

		For Each udtJob As udtDataPackageJobInfoType In lstDataPackagePeptideHitJobs
			lstDataPackageJobs.Add(udtJob.Job.ToString())
		Next

		If lstDataPackageJobs.Count > 0 Then
			StorePackedJobParameterList(lstDataPackageJobs.ToList(), JOB_PARAM_DATA_PACKAGE_PEPTIDE_HIT_JOBS)
		End If

	End Sub

End Class
