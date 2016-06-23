Option Strict On

Imports AnalysisManagerBase

Public Class clsAnalysisResourcesPRIDEConverter
	Inherits clsAnalysisResources

	Public Const JOB_PARAM_DATASETS_MISSING_MZXML_FILES As String = "PackedParam_DatasetsMissingMzXMLFiles"
	Public Const JOB_PARAM_DATA_PACKAGE_PEPTIDE_HIT_JOBS As String = "PackedParam_DataPackagePeptideHitJobs"
	Public Const JOB_PARAM_DICTIONARY_DATASET_STORAGE_YEAR_QUARTER As String = "PackedParam_DatasetStorage_YearQuarter"

	Public Const JOB_PARAM_MSGF_REPORT_TEMPLATE_FILENAME As String = "MSGFReportFileTemplate"
	Public Const JOB_PARAM_PX_SUBMISSION_TEMPLATE_FILENAME As String = "PXSubmissionTemplate"

	Public Const DEFAULT_MSGF_REPORT_TEMPLATE_FILENAME As String = "Template.msgf-report.xml"
	Public Const MSGF_REPORT_FILE_SUFFIX As String = "msgf-report.xml"

	Public Const DEFAULT_PX_SUBMISSION_TEMPLATE_FILENAME As String = "PX_Submission_Template.px"
	Public Const PX_SUBMISSION_FILE_SUFFIX As String = ".px"

	Public Overrides Function GetResources() As IJobParams.CloseOutType

        Dim lstDataPackagePeptideHitJobs = New List(Of clsDataPackageJobInfo)

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

        If blnCreatePrideXMLFiles Then
            udtOptions.RetrievePHRPFiles = True
        Else
            udtOptions.retrievePHRPFiles = False
        End If

        udtOptions.RetrieveDTAFiles = m_jobParams.GetJobParameter("CreateMGFFiles", True)
        udtOptions.RetrieveMZidFiles = m_jobParams.GetJobParameter("IncludeMZidFiles", True)
        udtOptions.RetrievePepXMLFiles = m_jobParams.GetJobParameter("IncludePepXMLFiles", False)

        Dim disableMyEMSL = m_jobParams.GetJobParameter("DisableMyEMSL", False)
        If disableMyEMSL Then
            DisableMyEMSLSearch()
        End If

        udtOptions.AssumeInstrumentDataUnpurged = m_jobParams.GetJobParameter("AssumeInstrumentDataUnpurged", True)

        If blnCreateMSGFReportFilesOnly Then
            udtOptions.RetrieveDTAFiles = False
            udtOptions.RetrieveMZidFiles = False
        Else
            If blnCreatePrideXMLFiles Then
                If Not RetrieveMSGFReportTemplateFile() Then
                    Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
                End If
            End If

            If Not RetrievePXSubmissionTemplateFile() Then
                Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
            End If
        End If

        ' Obtain the PHRP-related files for the Peptide_Hit jobs defined for the data package associated with this data aggregation job
        ' Possibly also obtain the .mzXML file or .Raw file for each dataset
        ' The .mzXML file is required if we are creating Pride XML files (which were required for a "complete" submission 
        '   prior to May 2013; we now submit .mzid.gz files and instrument binary files and thus don't need the .mzXML file)
        If Not MyBase.RetrieveDataPackagePeptideHitJobPHRPFiles(udtOptions, lstDataPackagePeptideHitJobs, 0, clsAnalysisToolRunnerPRIDEConverter.PROGRESS_PCT_TOOL_RUNNER_STARTING) Then
            Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
        End If

        ' Obtain the FASTA files (typically generated from protein collections) used for the jobs in lstDataPackagePeptideHitJobs
        If Not RetrieveFastaFiles(lstDataPackagePeptideHitJobs) Then
            Return IJobParams.CloseOutType.CLOSEOUT_NO_FAS_FILES
        End If

        If udtOptions.RetrieveMzXMLFile Then
            ' Use lstDataPackagePeptideHitJobs to look for any datasets for which we will need to create a .mzXML file
            FindMissingMzXmlFiles(lstDataPackagePeptideHitJobs)
        End If

        If Not m_MyEMSLUtilities.ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders) Then
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        StoreDataPackageJobs(lstDataPackagePeptideHitJobs)

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    ''' <summary>
    ''' Find datasets that do not have a .mzXML file
    ''' Datasets that need to have .mzXML files created will be added to the packed job parameters, storing the dataset names in "PackedParam_DatasetsMissingMzXMLFiles"
    ''' and the dataset Year_Quarter values in "PackedParam_DatasetStorage_YearQuarter"
    ''' </summary>
    ''' <param name="lstDataPackagePeptideHitJobs"></param>
    ''' <remarks></remarks>
    Protected Sub FindMissingMzXmlFiles(lstDataPackagePeptideHitJobs As IEnumerable(Of clsDataPackageJobInfo))

        Dim lstDatasets = New SortedSet(Of String)
        Dim lstDatasetYearQuarter = New SortedSet(Of String)

        Try
            For Each dataPkgJob As clsDataPackageJobInfo In lstDataPackagePeptideHitJobs
                Dim strMzXmlFilePath As String
                strMzXmlFilePath = IO.Path.Combine(m_WorkingDir, dataPkgJob.Dataset & DOT_MZXML_EXTENSION)

                If Not IO.File.Exists(strMzXmlFilePath) Then

                    ' Look for a StoragePathInfo file
                    strMzXmlFilePath &= STORAGE_PATH_INFO_FILE_SUFFIX
                    If Not IO.File.Exists(strMzXmlFilePath) Then
                        If Not lstDatasets.Contains(dataPkgJob.Dataset) Then
                            lstDatasets.Add(dataPkgJob.Dataset)
                            lstDatasetYearQuarter.Add(dataPkgJob.Dataset & "=" & GetDatasetYearQuarter(dataPkgJob.ServerStoragePath))
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

    Public Shared Function GetGeneratedFastaParamNameForJob(Job As Integer) As String
        Return "Job" & Job.ToString() & "_GeneratedFasta"
    End Function

    Public Shared Function GetMSGFReportTemplateFilename(JobParams As IJobParams, WarnIfJobParamMissing As Boolean) As String

        Dim strTemplateFileName As String = JobParams.GetJobParameter(JOB_PARAM_MSGF_REPORT_TEMPLATE_FILENAME, String.Empty)

        If String.IsNullOrEmpty(strTemplateFileName) Then
            If WarnIfJobParamMissing Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Job parameter " & JOB_PARAM_MSGF_REPORT_TEMPLATE_FILENAME & " is empty; will assume " & strTemplateFileName)
            End If
            strTemplateFileName = DEFAULT_MSGF_REPORT_TEMPLATE_FILENAME
        End If

        Return strTemplateFileName

    End Function

    Public Shared Function GetPXSubmissionTemplateFilename(JobParams As IJobParams, WarnIfJobParamMissing As Boolean) As String

        Dim strTemplateFileName As String = JobParams.GetJobParameter(JOB_PARAM_PX_SUBMISSION_TEMPLATE_FILENAME, String.Empty)

        If String.IsNullOrEmpty(strTemplateFileName) Then
            strTemplateFileName = DEFAULT_PX_SUBMISSION_TEMPLATE_FILENAME
            If WarnIfJobParamMissing Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Job parameter " & JOB_PARAM_PX_SUBMISSION_TEMPLATE_FILENAME & " is empty; will assume " & strTemplateFileName)
            End If
        End If

        Return strTemplateFileName

    End Function

    Protected Function RetrieveFastaFiles(lstDataPackagePeptideHitJobs As IEnumerable(Of clsDataPackageJobInfo)) As Boolean

        Dim strLocalOrgDBFolder As String = m_mgrParams.GetParam("orgdbdir")

        Dim strDictionaryKey As String

        Dim strOrgDBNameGenerated As String = String.Empty

        ' This dictionary is used to avoid calling RetrieveOrgDB() for every job
        ' The dictionary keys are LegacyFastaFileName, ProteinOptions, and ProteinCollectionList combined with underscores
        ' The dictionary values are the name of the generated (or retrieved) fasta file
        Dim dctOrgDBParamsToGeneratedFileNameMap As Dictionary(Of String, String)

        Try
            dctOrgDBParamsToGeneratedFileNameMap = New Dictionary(Of String, String)

            ' Cache the current dataset and job info
            Dim currentDatasetAndJobInfo = GetCurrentDatasetAndJobInfo()

            For Each dataPkgJob As clsDataPackageJobInfo In lstDataPackagePeptideHitJobs

                strDictionaryKey = dataPkgJob.LegacyFastaFileName & "_" & dataPkgJob.ProteinCollectionList & "_" & dataPkgJob.ProteinOptions

                If dctOrgDBParamsToGeneratedFileNameMap.TryGetValue(strDictionaryKey, strOrgDBNameGenerated) Then
                    ' Organism DB was already generated
                Else
                    OverrideCurrentDatasetAndJobInfo(dataPkgJob)

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

                    If strOrgDBNameGenerated <> dataPkgJob.OrganismDBName Then
                        If strOrgDBNameGenerated Is Nothing Then strOrgDBNameGenerated = "??"
                        If dataPkgJob.OrganismDBName Is Nothing Then dataPkgJob.OrganismDBName = "??"

                        m_message = "Generated FASTA file name (" & strOrgDBNameGenerated & ") does not match expected fasta file name (" & dataPkgJob.OrganismDBName & "); aborting"
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & " (class clsAnalysisResourcesPRIDEConverter)")
                        Return False
                    End If

                    dctOrgDBParamsToGeneratedFileNameMap.Add(strDictionaryKey, strOrgDBNameGenerated)
                End If

                ' Add a new job parameter that associates strOrgDBNameGenerated with this job

                m_jobParams.AddAdditionalParameter("PeptideSearch", GetGeneratedFastaParamNameForJob(dataPkgJob.Job), strOrgDBNameGenerated)
            Next

            ' Restore the dataset and job info for this aggregation job
            OverrideCurrentDatasetAndJobInfo(currentDatasetAndJobInfo)

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
        Dim fiFiles As List(Of IO.FileInfo)

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
                If Not RetrieveFile(fiFiles(0).Name, fiFiles(0).DirectoryName) Then
                    Return False
                Else
                    strTemplateFileName = fiFiles(0).Name
                End If
            Else
                Dim strParamFileStoragePath As String = m_jobParams.GetParam("ParmFileStoragePath")
                strTemplateFileName = DEFAULT_MSGF_REPORT_TEMPLATE_FILENAME

                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "MSGF Report template file not found in the data package folder; retrieving " & strTemplateFileName & "from " & strParamFileStoragePath)

                If String.IsNullOrEmpty(strParamFileStoragePath) Then strParamFileStoragePath = "\\gigasax\dms_parameter_Files\PRIDE_Converter"

                If Not RetrieveFile(strTemplateFileName, strParamFileStoragePath) Then
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

    Protected Function RetrievePXSubmissionTemplateFile() As Boolean

        ' Retrieve the template PX Submission file
        ' Although there is a default in the PRIDE_Converter parameter file folder, it should ideally be customized and placed in the data package folder

        Try
            Dim strTemplateFileName = GetPXSubmissionTemplateFilename(m_jobParams, WarnIfJobParamMissing:=True)

            ' First look for the template file in the data package folder
            ' Note that transferFolderPath is likely \\protoapps\PeptideAtlas_Staging and not the real data package path

            Dim transferFolderPath As String = m_jobParams.GetJobParameter("JobParameters", "transferFolderPath", String.Empty)
            If String.IsNullOrEmpty(transferFolderPath) Then
                m_message = "Job parameter transferFolderPath is missing; unable to determine the data package folder path"
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
                Return False
            End If

            Dim ConnectionString As String = m_mgrParams.GetParam("brokerconnectionstring")
            Dim dataPackageID As Integer = m_jobParams.GetJobParameter("DataPackageID", -1)

            Dim matchFound = False
            Dim lstSourceFolders = New List(Of String)

            lstSourceFolders.Add(GetDataPackageStoragePath(ConnectionString, dataPackageID))
            lstSourceFolders.Add(transferFolderPath)

            For Each sourceFolderPath In lstSourceFolders

                If String.IsNullOrEmpty(sourceFolderPath) Then Continue For

                Dim diDataPackageFolder = New IO.DirectoryInfo(sourceFolderPath)
                Dim fiFiles = diDataPackageFolder.GetFiles(strTemplateFileName).ToList()

                If fiFiles.Count = 0 Then
                    ' File not found; see if any files ending in PX_SUBMISSION_FILE_SUFFIX exist in the data package folder
                    fiFiles = diDataPackageFolder.GetFiles("*" & PX_SUBMISSION_FILE_SUFFIX).ToList()
                End If

                If fiFiles.Count > 0 Then
                    ' Template file found in the data package; copy it locally
                    If Not RetrieveFile(fiFiles(0).Name, fiFiles(0).DirectoryName) Then
                        Return False
                    Else
                        strTemplateFileName = fiFiles(0).Name
                        matchFound = True
                        Exit For
                    End If
                End If

            Next

            If Not matchFound Then
                Dim strParamFileStoragePath As String = m_jobParams.GetParam("ParmFileStoragePath")
                If String.IsNullOrEmpty(strParamFileStoragePath) Then
                    strParamFileStoragePath = "\\gigasax\dms_parameter_Files\PRIDE_Converter"
                End If
                strTemplateFileName = DEFAULT_PX_SUBMISSION_TEMPLATE_FILENAME

                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "PX Submission template file not found in the data package folder; retrieving " & strTemplateFileName & " from " & strParamFileStoragePath)

                If Not RetrieveFile(strTemplateFileName, strParamFileStoragePath, 1) Then
                    If String.IsNullOrEmpty(m_message) Then
                        m_message = "Template PX file " & strTemplateFileName & " to found in the data package folder"
                    End If
                    Return False
                End If
            End If

            ' Assure that the PX Submission Template file job parameter is up-to-date
            m_jobParams.AddAdditionalParameter("JobParameters", JOB_PARAM_PX_SUBMISSION_TEMPLATE_FILENAME, strTemplateFileName)

            m_jobParams.AddResultFileToSkip(strTemplateFileName)

        Catch ex As Exception
            m_message = "Exception in RetrievePXSubmissionTemplateFile"
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
    Protected Sub StoreDataPackageJobs(lstDataPackagePeptideHitJobs As IEnumerable(Of clsDataPackageJobInfo))
        Dim lstDataPackageJobs = New List(Of String)

        For Each dataPkgJob As clsDataPackageJobInfo In lstDataPackagePeptideHitJobs
            lstDataPackageJobs.Add(dataPkgJob.Job.ToString())
        Next

        If lstDataPackageJobs.Count > 0 Then
            StorePackedJobParameterList(lstDataPackageJobs, JOB_PARAM_DATA_PACKAGE_PEPTIDE_HIT_JOBS)
        End If

    End Sub

End Class
