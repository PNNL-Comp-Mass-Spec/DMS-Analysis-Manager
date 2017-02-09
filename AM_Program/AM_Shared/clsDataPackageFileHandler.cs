Imports System.Data.SqlClient
Imports System.IO
Imports System.Runtime.InteropServices
Imports System.Threading
Imports System.Xml
Imports PHRPReader

Public Class clsDataPackageFileHandler
    Inherits clsEventNotifier

#Region "Constants"
    Public Const JOB_INFO_FILE_PREFIX As String = "JobInfoFile_Job"

    Private Const SP_NAME_GET_JOB_STEP_INPUT_FOLDER As String = "GetJobStepInputFolder"

#End Region

#Region "Structures"
    Public Structure udtDataPackageRetrievalOptionsType
        ''' <summary>
        ''' Set to true to create a text file for each job listing the full path to the files that would be retrieved for that job
        ''' Example filename: FilePathInfo_Job950000.txt
        ''' </summary>
        ''' <remarks>No files are actually retrieved when this is set to True</remarks>
        Public CreateJobPathFiles As Boolean

        ''' <summary>
        ''' Set to true to obtain the mzXML file for the dataset associated with this job
        ''' </summary>
        ''' <remarks>If the .mzXML file does not exist, then retrieves the instrument data file (e.g. Thermo .raw file)</remarks>
        Public RetrieveMzXMLFile As Boolean

        ''' <summary>
        ''' Set to True to retrieve _DTA.txt files (the PRIDE Converter will convert these to .mgf files)
        ''' </summary>
        ''' <remarks>If the search used a .mzML instead of a _dta.txt file, the .mzML.gz file will be retrieved</remarks>
        Public RetrieveDTAFiles As Boolean

        ''' <summary>
        ''' Set to True to obtain MSGF+ .mzID files
        ''' </summary>
        ''' <remarks></remarks>
        Public RetrieveMZidFiles As Boolean

        ''' <summary>
        ''' Set to True to obtain .pepXML files (typically stored as _pepXML.zip)
        ''' </summary>
        ''' <remarks></remarks>
        Public RetrievePepXMLFiles As Boolean

        ''' <summary>
        ''' Set to True to obtain the _syn.txt file and related PHRP files
        ''' </summary>
        ''' <remarks></remarks>
        Public RetrievePHRPFiles As Boolean

        ''' <summary>
        ''' When True, assume that the instrument file (e.g. .raw file) exists in the dataset storage folder
        ''' and do not search in MyEMSL or in the archive for the file
        ''' </summary>
        ''' <remarks>Even if the instrument file has been purged from the storage folder, still report "success" when searching for the instrument file</remarks>
        Public AssumeInstrumentDataUnpurged As Boolean
    End Structure
#End Region


#Region "Module variables"
    ''' <summary>
    ''' Typically Gigasax.DMS_Pipeline
    ''' </summary>
    Private ReadOnly mBrokerDbConnectionString As String

    Private ReadOnly mDataPackageID As Integer

    Private ReadOnly mAnalysisResources As clsAnalysisResources

    Private ReadOnly mDataPackageInfoLoader As clsDataPackageInfoLoader

    Public ReadOnly mPipelineDBProcedureExecutor As PRISM.DataBase.clsExecuteDatabaseSP
#End Region

    ''' <summary>
    ''' Constructor
    ''' </summary>
    Public Sub New(brokerDbConnectionString As String, dataPackageID As Integer, resourcesClass As clsAnalysisResources)
        mBrokerDbConnectionString = brokerDbConnectionString
        mDataPackageID = dataPackageID
        mAnalysisResources = resourcesClass

        mDataPackageInfoLoader = New clsDataPackageInfoLoader(mBrokerDbConnectionString, mDataPackageID)

        mPipelineDBProcedureExecutor = New PRISM.DataBase.clsExecuteDatabaseSP(mBrokerDbConnectionString)

        AddHandler mPipelineDBProcedureExecutor.DebugEvent, AddressOf ProcedureExecutor_DebugEvent
        AddHandler mPipelineDBProcedureExecutor.DBErrorEvent, AddressOf ProcedureExecutor_DBErrorEvent

    End Sub

    ''' <summary>
    ''' Contact the database to determine the exact folder name of the MSXML_Gen or Mz_Refinery folder
    ''' that has the CacheInfo file for the mzML file used by a given job
    ''' Look for the cache info file in that folder, then use that to find the location of the actual .mzML.gz file
    ''' </summary>
    ''' <param name="job"></param>
    ''' <param name="stepToolFilter">step tool to filter on; if an empty string, returns the input folder for the primary step tool for the job</param>
    ''' <returns>Path to the .mzML or .mzML.gz file; empty string if not found</returns>
    ''' <remarks>Uses the highest job step to determine the input folder, meaning the .mzML.gz file returned will be the one used by MSGF+</remarks>
    Private Function FindMzMLForJob(dataset As String, job As Integer, stepToolFilter As String, workingDir As String) As String

        Try

            ' Set up the command object prior to SP execution
            Dim myCmd = New SqlCommand(SP_NAME_GET_JOB_STEP_INPUT_FOLDER) With {
                .CommandType = CommandType.StoredProcedure
            }

            myCmd.Parameters.Add(New SqlParameter("@Return", SqlDbType.Int)).Direction = ParameterDirection.ReturnValue
            myCmd.Parameters.Add(New SqlParameter("@job", SqlDbType.Int)).Value = job

            Dim stepToolFilterParam = myCmd.Parameters.Add(New SqlParameter("@stepToolFilter", SqlDbType.VarChar, 8000))
            stepToolFilterParam.Value = stepToolFilter

            myCmd.Parameters.Add(New SqlParameter("@inputFolderName", SqlDbType.VarChar, 128)).Direction = ParameterDirection.Output
            myCmd.Parameters.Add(New SqlParameter("@stepToolMatch", SqlDbType.VarChar, 64)).Direction = ParameterDirection.Output

            Dim matchFound = False
            Dim inputFolderName As String = String.Empty
            Dim stepToolMatch As String = String.Empty

            Do While Not matchFound

                ' Execute the SP
                mPipelineDBProcedureExecutor.ExecuteSP(myCmd, 1)

                inputFolderName = CStr(myCmd.Parameters("@inputFolderName").Value)
                stepToolMatch = CStr(myCmd.Parameters("@stepToolMatch").Value)

                If String.IsNullOrWhiteSpace(inputFolderName) Then
                    If String.IsNullOrEmpty(stepToolFilter) Then
                        OnErrorEvent(String.Format("Unable to determine the input folder for job {0}", job))
                        Return String.Empty
                    Else
                        OnStatusEvent(String.Format("Unable to determine the input folder for job {0} and step tool {1}; will try again without a step tool filter",
                                                    job, stepToolFilter))
                        stepToolFilter = String.Empty
                        stepToolFilterParam.Value = stepToolFilter
                    End If
                Else
                    matchFound = True
                End If
            Loop

            OnStatusEvent(String.Format("Determined the input folder for job {0} is {1}, step tool {2}", job, inputFolderName, stepToolMatch))

            ' Look for a CacheInfo.txt file in the matched input folder
            ' Note that FindValidFolder will search both the dataset folder and in folder inputFolderName below the dataset folder

            Dim validFolderFound As Boolean

            Dim datasetFolderPath As String = mAnalysisResources.FindValidFolder(
                dataset,
                "*_CacheInfo.txt",
                folderNameToFind:=inputFolderName,
                maxAttempts:=1,
                logFolderNotFound:=False,
                retrievingInstrumentDataFolder:=False,
                validFolderFound:=validFolderFound,
                assumeUnpurged:=False)

            If Not validFolderFound Then
                Return String.Empty
            End If

            Dim cacheInfoFileName As String = String.Empty
            Dim cacheInfoFileSourceType As String = String.Empty

            ' datasetFolderPath will hold the full path to the actual folder with the CacheInfo.txt file
            ' For example: \\proto-6\QExactP02\2016_2\Biodiversity_A_cryptum_FeTSB\Mz_Refinery_1_195_501572

            If datasetFolderPath.StartsWith(clsAnalysisResources.MYEMSL_PATH_FLAG) Then
                ' File found in MyEMSL
                ' Determine the MyEMSL FileID by searching for the expected file in m_MyEMSLUtilities.RecentlyFoundMyEMSLFiles

                For Each udtArchivedFile In mAnalysisResources.MyEMSLUtilities.RecentlyFoundMyEMSLFiles
                    Dim fiArchivedFile As New FileInfo(udtArchivedFile.FileInfo.RelativePathWindows)
                    If fiArchivedFile.Name.EndsWith("_CacheInfo.txt", StringComparison.InvariantCultureIgnoreCase) Then
                        mAnalysisResources.MyEMSLUtilities.AddFileToDownloadQueue(udtArchivedFile.FileInfo)
                        cacheInfoFileName = udtArchivedFile.FileInfo.Filename
                        Exit For
                    End If
                Next

                If String.IsNullOrEmpty(cacheInfoFileName) Then
                    OnErrorEvent("FindValidFolder reported a match to a file in MyEMSL (" & datasetFolderPath & ") " &
                                 "but MyEMSLUtilities.RecentlyFoundMyEMSLFiles is empty")
                    Return String.Empty
                Else
                    mAnalysisResources.ProcessMyEMSLDownloadQueue()
                    cacheInfoFileSourceType = "MyEMSL"
                End If
            Else
                Dim sourceFolder = New DirectoryInfo(datasetFolderPath)
                cacheInfoFileSourceType = sourceFolder.FullName

                If Not sourceFolder.Exists Then
                    OnErrorEvent("FindValidFolder reported a match to folder " & cacheInfoFileSourceType & " but the folder was not found")
                    Return String.Empty
                End If

                Dim cacheInfoFiles = sourceFolder.GetFiles("*_CacheInfo.txt")
                If cacheInfoFiles.Count = 0 Then
                    OnErrorEvent("FindValidFolder reported that folder " & cacheInfoFileSourceType & " has a _CacheInfo.txt file, but none was found")
                    Return String.Empty
                End If

                Dim sourceCacheInfoFile = cacheInfoFiles.First()

                Dim success = mAnalysisResources.CopyFileToWorkDir(sourceCacheInfoFile.Name, sourceCacheInfoFile.DirectoryName, workingDir, clsLogTools.LogLevels.ERROR)
                If Not success Then
                    ' The error should have already been logged
                    Return String.Empty
                End If

                cacheInfoFileName = sourceCacheInfoFile.Name
            End If

            ' Open the CacheInfo file and read the first line

            Dim localCacheInfoFile = New FileInfo(Path.Combine(workingDir, cacheInfoFileName))
            If Not localCacheInfoFile.Exists Then
                OnErrorEvent("CacheInfo file not found in the working directory; should have been retrieved from " & cacheInfoFileSourceType)
                Return String.Empty
            End If

            Dim remoteMsXmlFilePath As String = String.Empty

            Using reader = New StreamReader(New FileStream(localCacheInfoFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                If Not reader.EndOfStream Then
                    remoteMsXmlFilePath = reader.ReadLine
                End If
            End Using

            If String.IsNullOrEmpty(remoteMsXmlFilePath) Then
                OnErrorEvent("CacheInfo file retrieved from " & cacheInfoFileSourceType & " was empty")
            Else
                OnStatusEvent(String.Format("Found remote mzML file for job {0}: {1}", job, remoteMsXmlFilePath))
            End If

            Thread.Sleep(250)

            ' Delete the locally cached file
            localCacheInfoFile.Delete()

            Return remoteMsXmlFilePath

        Catch ex As Exception
            OnErrorEvent("Exception finding the .mzML file used by job " & job, ex)
            Return String.Empty
        End Try

    End Function

    ''' <summary>
    ''' Add expected .mzid file names
    ''' Originally .mzid files were named _msgfplus.zip
    ''' We switched to naming them _msgfplus.mzid.gz in January 2014
    ''' </summary>
    ''' <param name="datasetName"></param>
    ''' <param name="splitFastaResultID"></param>
    ''' <param name="zipFileCandidates">Potential names of zip files to decompress</param>
    ''' <param name="gzipFileCandidates">Potential names of gzip files to decompress</param>
    Private Sub GetMzIdFilesToFind(
      datasetName As String,
      splitFastaResultID As Integer,
      zipFileCandidates As ICollection(Of String),
      gzipFileCandidates As ICollection(Of String))

        Dim zipFile As String
        Dim gZipFile As String

        If splitFastaResultID > 0 Then
            zipFile = datasetName & "_msgfplus_Part" & splitFastaResultID & ".zip"
            gZipFile = datasetName & "_msgfplus_Part" & splitFastaResultID & ".mzid.gz"
        Else
            zipFile = datasetName & "_msgfplus.zip"
            gZipFile = datasetName & "_msgfplus.mzid.gz"
        End If

        zipFileCandidates.Add(zipFile)
        gzipFileCandidates.Add(gZipFile)

    End Sub

    Private Function GetJobInfoFilePath(intJob As Integer) As String
        Return GetJobInfoFilePath(intJob, mAnalysisResources.WorkDir)
    End Function

    Public Shared Function GetJobInfoFilePath(intJob As Integer, strWorkDirPath As String) As String
        Return Path.Combine(strWorkDirPath, JOB_INFO_FILE_PREFIX & intJob & ".txt")
    End Function

    ''' <summary>
    ''' Return the full path to the most recently unzipped file (.zip or .gz)
    ''' Returns an empty string if no recent unzipped files
    ''' </summary>
    ''' <param name="ionicZipTools"></param>
    ''' <returns></returns>
    Private Function MostRecentUnzippedFile(ionicZipTools As clsIonicZipTools) As String
        If ionicZipTools.MostRecentUnzippedFiles.Count > 0 Then
            Return ionicZipTools.MostRecentUnzippedFiles.First.Value
        End If

        Return String.Empty
    End Function

    ''' <summary>
    ''' Open an .mzid or .mzid.gz file and look for the SpectraData element
    ''' If the location of the file specified by SpectraData points to a .mzML or .mzML.gz file, return true
    ''' Otherwise, return false
    ''' </summary>
    ''' <param name="mzIdFileToInspect"></param>
    ''' <param name="ionicZipTools"></param>
    ''' <returns></returns>
    Private Function MSGFPlusSearchUsedMzML(mzIdFileToInspect As String, ionicZipTools As clsIonicZipTools) As Boolean

        Try
            Dim mzidFile = New FileInfo(mzIdFileToInspect)
            If Not mzidFile.Exists Then
                OnErrorEvent("Unable to examine the mzid file to determine whether MSGF+ searched a .mzML file; file Not found:   " & mzIdFileToInspect)
                Return False
            End If

            Dim mzidFilePathLocal As String
            Dim deleteLocalFile As Boolean
            Dim searchedUsedMzML = False

            If mzidFile.Name.EndsWith(".zip") Then
                If Not ionicZipTools.UnzipFile(mzidFile.FullName) Then
                    OnErrorEvent("Error unzipping " & mzidFile.FullName)
                    Return False
                End If

                mzidFilePathLocal = MostRecentUnzippedFile(ionicZipTools)
                deleteLocalFile = True

            ElseIf mzidFile.Name.EndsWith(".gz") Then
                If Not ionicZipTools.GUnzipFile(mzidFile.FullName) Then
                    OnErrorEvent("Error unzipping " & mzidFile.FullName)
                    Return False
                End If

                mzidFilePathLocal = MostRecentUnzippedFile(ionicZipTools)
                deleteLocalFile = True
            Else
                mzidFilePathLocal = mzidFile.FullName
                deleteLocalFile = False
            End If

            If Not File.Exists(mzidFilePathLocal) Then
                OnErrorEvent("mzID file not found in the working directory; cannot inspect it in MSGFPlusSearchUsedMzML: " & Path.GetFileName(mzidFilePathLocal))
                Return False
            End If

            Dim spectraLocationFound = False

            Using reader = New XmlTextReader(mzidFilePathLocal)
                Do While reader.Read()

                    Select Case reader.NodeType
                        Case XmlNodeType.Element
                            If reader.Name = "SpectraData" Then
                                ' The location attribute of the SpectraData element has the input file name
                                If reader.MoveToAttribute("location") Then
                                    Dim spectraDataFileName = Path.GetFileName(reader.Value)
                                    Const DOT_MZML = clsAnalysisResources.DOT_MZML_EXTENSION
                                    Const DOT_MZML_GZ = clsAnalysisResources.DOT_MZML_EXTENSION & clsAnalysisResources.DOT_GZ_EXTENSION

                                    If spectraDataFileName.EndsWith(DOT_MZML, StringComparison.InvariantCultureIgnoreCase) OrElse
                                       spectraDataFileName.EndsWith(DOT_MZML_GZ, StringComparison.InvariantCultureIgnoreCase) Then
                                        searchedUsedMzML = True
                                    End If

                                    spectraLocationFound = True
                                    Exit Do
                                End If
                            End If
                    End Select

                Loop
            End Using

            If deleteLocalFile Then
                Thread.Sleep(200)
                File.Delete(mzidFilePathLocal)
            End If

            If Not spectraLocationFound Then
                OnErrorEvent(".mzID file did not have node SpectraData with attribute location: " & Path.GetFileName(mzidFilePathLocal))
                Return False
            End If

            Return searchedUsedMzML

        Catch ex As Exception
            OnErrorEvent("Exception examining the mzid file To determine whether MSGF+ searched a .mzML file", ex)
            Return False
        End Try

    End Function

    Private Function RenameDuplicatePHRPFile(
      sourceFolderPath As String,
      sourceFilename As String,
      targetFolderPath As String,
      strPrefixToAdd As String,
      intJob As Integer,
      <Out()> newFilePath As String) As Boolean

        Try
            Dim fiFileToRename = New FileInfo(Path.Combine(sourceFolderPath, sourceFilename))
            newFilePath = Path.Combine(targetFolderPath, strPrefixToAdd & fiFileToRename.Name)

            Thread.Sleep(100)
            fiFileToRename.MoveTo(newFilePath)

            mAnalysisResources.AddResultFileToSkip(Path.GetFileName(newFilePath))
            Return True

        Catch ex As Exception
            OnErrorEvent("Exception renaming PHRP file " & sourceFilename & " For job " & intJob &
                         " (data package has multiple jobs For the same dataset)", ex)
            newFilePath = String.Empty
            Return False
        End Try

    End Function

    ''' <summary>
    ''' Retrieves the PHRP files for the PeptideHit jobs defined for the data package associated with this aggregation job
    ''' Also creates a batch file that can be manually run to retrieve the instrument data files
    ''' </summary>
    ''' <param name="udtOptions">File retrieval options</param>
    ''' <param name="lstDataPackagePeptideHitJobs">Output parameter: Job info for the peptide_hit jobs associated with this data package (output parameter)</param>
    ''' <param name="progressPercentAtStart">Percent complete value to use for computing incremental progress</param>
    ''' <param name="progressPercentAtFinish">Percent complete value to use for computing incremental progress</param>
    ''' <returns>True if success, false if an error</returns>
    ''' <remarks></remarks>
    Public Function RetrieveDataPackagePeptideHitJobPHRPFiles(
      udtOptions As udtDataPackageRetrievalOptionsType,
      <Out()> ByRef lstDataPackagePeptideHitJobs As List(Of clsDataPackageJobInfo),
      progressPercentAtStart As Single,
      progressPercentAtFinish As Single) As Boolean

        Dim sourceFolderPath = "??"
        Dim sourceFilename = "??"

        Dim blnFileCopied As Boolean
        Dim blnSuccess As Boolean

        ' The keys in this dictionary are data package job info entries; the values are KeyValuePairs of path to the .mzXML file and path to the .hashcheck file (if any)
        ' The KeyValuePair will have empty strings if the .Raw file needs to be retrieved
        ' This information is used by RetrieveDataPackageMzXMLFiles when copying files locally
        Dim dctInstrumentDataToRetrieve As Dictionary(Of clsDataPackageJobInfo, KeyValuePair(Of String, String))

        ' Keys in this dictionary are DatasetID, values are a command of the form "Copy \\Server\Share\Folder\Dataset.raw Dataset.raw"
        ' Note that we're explicitly defining the target filename to make sure the case of the letters matches the dataset name's case
        Dim dctRawFileRetrievalCommands = New Dictionary(Of Integer, String)

        ' Keys in this dictionary are dataset name, values are the full path to the instrument data file for the dataset
        ' This information is stored in packed job parameter JOB_PARAM_DICTIONARY_DATASET_FILE_PATHS
        Dim dctDatasetRawFilePaths = New Dictionary(Of String, String)

        ' This list tracks the info for the jobs associated with this aggregation job's data package
        lstDataPackagePeptideHitJobs = New List(Of clsDataPackageJobInfo)

        ' This dictionary tracks the datasets associated with this aggregation job's data package
        Dim dctDataPackageDatasets As Dictionary(Of Integer, clsDataPackageDatasetInfo) = Nothing

        Dim debugLevel = mAnalysisResources.DebugLevel
        Dim workingDir = mAnalysisResources.WorkDir

        Try
            Dim success = mDataPackageInfoLoader.LoadDataPackageDatasetInfo(dctDataPackageDatasets)

            If Not success OrElse dctDataPackageDatasets.Count = 0 Then
                OnErrorEvent("Did not find any datasets associated with this job's data package ID (" & mDataPackageInfoLoader.DataPackageID & ")")
                Return False
            End If

        Catch ex As Exception
            OnErrorEvent("Exception calling LoadDataPackageDatasetInfo", ex)
            Return False
        End Try

        ' The keys in this dictionary are data package job info entries; the values KeyValuePairs of path to the .mzXML file and path to the .hashcheck file (if any)
        ' The KeyValuePair will have empty strings if the .Raw file needs to be retrieved
        dctInstrumentDataToRetrieve = New Dictionary(Of clsDataPackageJobInfo, KeyValuePair(Of String, String))

        ' This list tracks analysis jobs that are not PeptideHit jobs
        Dim lstAdditionalJobs As List(Of clsDataPackageJobInfo) = Nothing

        Try
            lstDataPackagePeptideHitJobs = mDataPackageInfoLoader.RetrieveDataPackagePeptideHitJobInfo(lstAdditionalJobs)

            If lstDataPackagePeptideHitJobs.Count = 0 Then
                ' Did not find any peptide hit jobs associated with this job's data package ID
                ' This is atypical, but is allowed
            End If

        Catch ex As Exception
            OnErrorEvent("Exception calling RetrieveDataPackagePeptideHitJobInfo", ex)
            Return False
        End Try

        Try

            Dim ionicZipTools = New clsIonicZipTools(debugLevel, workingDir)

            ' Make sure the MyEMSL download queue is empty
            mAnalysisResources.ProcessMyEMSLDownloadQueue()

            ' Cache the current dataset and job info
            mAnalysisResources.CacheCurrentDataAndJobInfo()

            Dim intJobsProcessed = 0

            For Each dataPkgJob As clsDataPackageJobInfo In lstDataPackagePeptideHitJobs

                If Not mAnalysisResources.OverrideCurrentDatasetAndJobInfo(dataPkgJob) Then
                    ' Error message has already been logged
                    mAnalysisResources.RestoreCachedDataAndJobInfo()
                    Return False
                End If

                Dim datasetName = mAnalysisResources.DatasetName

                If dataPkgJob.PeptideHitResultType = clsPHRPReader.ePeptideHitResultType.Unknown Then
                    Dim msg = "PeptideHit ResultType not recognized for job " & dataPkgJob.Job & ": " & dataPkgJob.ResultType.ToString()
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, msg)
                    Console.WriteLine(msg)
                Else

                    ' Keys in this list are filenames; values are True if the file is required and False if not required
                    Dim lstFilesToGet = New SortedList(Of String, Boolean)
                    Dim localFolderPath As String
                    Dim lstPendingFileRenames = New List(Of String)
                    Dim strSynopsisFileName As String
                    Dim strSynopsisMSGFFileName As String
                    Dim eLogMsgTypeIfNotFound As clsLogTools.LogLevels

                    ' These two variables track filenames that should be decompressed if they were copied locally
                    Dim zipFileCandidates = New List(Of String)
                    Dim gzipFileCandidates = New List(Of String)

                    ' This tracks the _pepXML.zip filename, which will be unzipped if it was found
                    Dim zippedPepXmlFile = String.Empty

                    Dim blnPrefixRequired As Boolean

                    strSynopsisFileName = clsPHRPReader.GetPHRPSynopsisFileName(dataPkgJob.PeptideHitResultType, dataPkgJob.Dataset)
                    strSynopsisMSGFFileName = clsPHRPReader.GetMSGFFileName(strSynopsisFileName)

                    If udtOptions.RetrievePHRPFiles Then
                        lstFilesToGet.Add(strSynopsisFileName, True)

                        lstFilesToGet.Add(clsPHRPReader.GetPHRPResultToSeqMapFileName(dataPkgJob.PeptideHitResultType, dataPkgJob.Dataset), True)
                        lstFilesToGet.Add(clsPHRPReader.GetPHRPSeqInfoFileName(dataPkgJob.PeptideHitResultType, dataPkgJob.Dataset), True)
                        lstFilesToGet.Add(clsPHRPReader.GetPHRPSeqToProteinMapFileName(dataPkgJob.PeptideHitResultType, dataPkgJob.Dataset), True)
                        lstFilesToGet.Add(clsPHRPReader.GetPHRPModSummaryFileName(dataPkgJob.PeptideHitResultType, dataPkgJob.Dataset), True)

                        lstFilesToGet.Add(strSynopsisMSGFFileName, False)
                    End If

                    ' Names of mzid result files that we should look for
                    ' Examples include:
                    '  DatasetName_msgfplus.mzid.gz
                    '  DatasetName_msgfplus_Part5.mzid.gz
                    '  DatasetName_msgfplus.zip
                    Dim candidateMzIdFiles = New SortedSet(Of String)(StringComparer.InvariantCultureIgnoreCase)

                    If udtOptions.RetrieveMZidFiles AndAlso dataPkgJob.PeptideHitResultType = clsPHRPReader.ePeptideHitResultType.MSGFDB Then
                        ' Retrieve MSGF+ .mzID files
                        ' They will either be stored as .zip files or as .gz files

                        If dataPkgJob.NumberOfClonedSteps > 0 Then
                            For splitFastaResultID = 1 To dataPkgJob.NumberOfClonedSteps
                                GetMzIdFilesToFind(datasetName, splitFastaResultID, zipFileCandidates, gzipFileCandidates)
                            Next
                        Else
                            GetMzIdFilesToFind(datasetName, 0, zipFileCandidates, gzipFileCandidates)
                        End If

                        For Each candidateFile In zipFileCandidates.Union(gzipFileCandidates)
                            candidateMzIdFiles.Add(candidateFile)
                            lstFilesToGet.Add(candidateFile, False)
                        Next

                    End If

                    If udtOptions.RetrievePepXMLFiles AndAlso dataPkgJob.PeptideHitResultType <> clsPHRPReader.ePeptideHitResultType.Unknown Then
                        ' Retrieve .pepXML files, which are stored as _pepXML.zip files
                        zippedPepXmlFile = datasetName & "_pepXML.zip"
                        lstFilesToGet.Add(zippedPepXmlFile, False)
                    End If

                    sourceFolderPath = String.Empty

                    ' Check whether a synopsis file by this name has already been copied locally
                    ' If it has, then we have multiple jobs for the same dataset with the same analysis tool, and we'll thus need to add a prefix to each filename
                    If Not udtOptions.CreateJobPathFiles AndAlso File.Exists(Path.Combine(workingDir, strSynopsisFileName)) Then
                        blnPrefixRequired = True

                        localFolderPath = Path.Combine(workingDir, "FileRename")
                        If Not Directory.Exists(localFolderPath) Then
                            Directory.CreateDirectory(localFolderPath)
                        End If

                    Else
                        blnPrefixRequired = False
                        localFolderPath = String.Copy(workingDir)
                    End If

                    ' This list tracks files that have been found
                    ' If udtOptions.CreateJobPathFiles is true, it has the remote file paths
                    ' Otherwise, the file has been copied locally and it has local file paths
                    ' Note that files might need to be renamed; that's tracked via lstPendingFileRenames
                    Dim lstFoundFiles = New List(Of String)

                    For Each sourceFile In lstFilesToGet

                        sourceFilename = sourceFile.Key
                        Dim fileRequired = sourceFile.Value

                        ' Typically only use FindDataFile() for the first file in lstFilesToGet; we will assume the other files are in that folder
                        ' However, if the file resides in MyEMSL then we need to call FindDataFile for every new file because FindDataFile will append the MyEMSL File ID for each file
                        If String.IsNullOrEmpty(sourceFolderPath) OrElse sourceFolderPath.StartsWith(clsAnalysisResources.MYEMSL_PATH_FLAG) Then
                            sourceFolderPath = mAnalysisResources.FindDataFile(sourceFilename)

                            If String.IsNullOrEmpty(sourceFolderPath) Then
                                Dim alternateFileName As String = clsPHRPReader.AutoSwitchToLegacyMSGFDBIfRequired(sourceFilename, "Dataset_msgfdb.txt")
                                sourceFolderPath = mAnalysisResources.FindDataFile(alternateFileName)
                            End If
                        End If

                        If Not fileRequired Then
                            ' It's OK if this file doesn't exist, we'll just log a debug message
                            eLogMsgTypeIfNotFound = clsLogTools.LogLevels.DEBUG
                        Else
                            ' This file must exist; log an error if it's not found
                            eLogMsgTypeIfNotFound = clsLogTools.LogLevels.ERROR
                        End If

                        If udtOptions.CreateJobPathFiles And Not sourceFolderPath.StartsWith(clsAnalysisResources.MYEMSL_PATH_FLAG) Then
                            Dim sourceFilePath As String = Path.Combine(sourceFolderPath, sourceFilename)
                            Dim alternateFileNamelternateFileName As String = clsPHRPReader.AutoSwitchToLegacyMSGFDBIfRequired(sourceFilePath, "Dataset_msgfdb.txt")

                            If File.Exists(sourceFilePath) Then
                                lstFoundFiles.Add(sourceFilePath)
                            ElseIf File.Exists(alternateFileNamelternateFileName) Then
                                lstFoundFiles.Add(alternateFileNamelternateFileName)
                            Else
                                If eLogMsgTypeIfNotFound <> clsLogTools.LogLevels.DEBUG Then
                                    Dim warningMessage = "Required PHRP file not found: " & sourceFilename
                                    If sourceFilename.ToLower().EndsWith("_msgfplus.zip") Or sourceFilename.ToLower().EndsWith("_msgfplus.mzid.gz") Then
                                        warningMessage &= "; Confirm job used MSGF+ and not MSGFDB"
                                    End If
                                    mAnalysisResources.UpdateStatusMessage(warningMessage)
                                    OnWarningEvent("Required PHRP file not found: " & sourceFilePath)
                                    mAnalysisResources.RestoreCachedDataAndJobInfo()
                                    Return False
                                End If
                            End If

                        Else
                            ' Note for files in MyEMSL, this call will simply add the file to the download queue; use ProcessMyEMSLDownloadQueue() to retrieve the file
                            blnFileCopied = mAnalysisResources.CopyFileToWorkDir(sourceFilename, sourceFolderPath, localFolderPath, eLogMsgTypeIfNotFound)

                            If Not blnFileCopied Then
                                Dim alternateFileName As String = clsPHRPReader.AutoSwitchToLegacyMSGFDBIfRequired(sourceFilename, "Dataset_msgfdb.txt")
                                blnFileCopied = mAnalysisResources.CopyFileToWorkDir(alternateFileName, sourceFolderPath, localFolderPath, eLogMsgTypeIfNotFound)
                                If blnFileCopied Then
                                    sourceFilename = alternateFileName
                                End If
                            End If

                            If Not blnFileCopied Then

                                If eLogMsgTypeIfNotFound <> clsLogTools.LogLevels.DEBUG Then
                                    OnErrorEvent("CopyFileToWorkDir returned False for " + sourceFilename + " using folder " + sourceFolderPath)
                                    mAnalysisResources.RestoreCachedDataAndJobInfo()
                                    Return False
                                End If

                            Else
                                OnStatusEvent("Copied " + sourceFilename + " from folder " + sourceFolderPath)
                                lstFoundFiles.Add(Path.Combine(localFolderPath, sourceFilename))

                                If blnPrefixRequired Then
                                    lstPendingFileRenames.Add(sourceFilename)
                                Else
                                    mAnalysisResources.AddResultFileToSkip(sourceFilename)
                                End If
                            End If
                        End If

                    Next sourceFile     ' in lstFilesToGet

                    Dim success = mAnalysisResources.ProcessMyEMSLDownloadQueue()
                    If Not success Then
                        mAnalysisResources.RestoreCachedDataAndJobInfo()
                        Return False
                    End If

                    ' Now perform any required file renames
                    For Each sourceFilename In lstPendingFileRenames
                        Dim newFilePath As String = String.Empty
                        If RenameDuplicatePHRPFile(localFolderPath, sourceFilename,
                                                   workingDir, "Job" & dataPkgJob.Job.ToString() & "_",
                                                   dataPkgJob.Job, newFilePath) Then
                            ' Rename succeeded
                            lstFoundFiles.Remove(Path.Combine(localFolderPath, sourceFilename))
                            lstFoundFiles.Add(newFilePath)
                        Else
                            ' Rename failed
                            mAnalysisResources.RestoreCachedDataAndJobInfo()
                            Return False
                        End If
                    Next

                    If udtOptions.RetrieveDTAFiles Then
                        ' Find the _dta.txt or .mzML.gz file that was used for a MSGF+ search

                        Dim mzIDFileToInspect = String.Empty

                        ' Need to examine the .mzid file to determine which file was used for the search
                        For Each foundFile In lstFoundFiles
                            If candidateMzIdFiles.Contains(Path.GetFileName(foundFile)) Then
                                mzIDFileToInspect = foundFile
                                Exit For
                            End If
                        Next

                        Dim searchUsedmzML = False
                        If Not String.IsNullOrEmpty(mzIDFileToInspect) Then
                            searchUsedmzML = MSGFPlusSearchUsedMzML(mzIDFileToInspect, ionicZipTools)
                        End If

                        If searchUsedmzML Then
                            Dim stepToolFilter = String.Empty
                            If dataPkgJob.Tool.ToLower().StartsWith("msgfplus") Then
                                stepToolFilter = "MSGFPlus"
                            End If

                            Dim mzMLFilePathRemote As String = FindMzMLForJob(dataPkgJob.Dataset, dataPkgJob.Job, stepToolFilter, workingDir)

                            If Not String.IsNullOrEmpty(mzMLFilePathRemote) Then
                                If udtOptions.CreateJobPathFiles And Not mzMLFilePathRemote.StartsWith(clsAnalysisResources.MYEMSL_PATH_FLAG) Then
                                    If Not lstFoundFiles.Contains(mzMLFilePathRemote) Then
                                        lstFoundFiles.Add(mzMLFilePathRemote)
                                    End If
                                Else
                                    ' Note for files in MyEMSL, this call will simply add the file to the download queue 
                                    ' Use ProcessMyEMSLDownloadQueue() to retrieve the file
                                    Dim sourceMzMLFile = New FileInfo(mzMLFilePathRemote)
                                    Dim targetMzMLFile = New FileInfo(Path.Combine(localFolderPath, sourceMzMLFile.Name))

                                    ' Only copy the .mzML.gz file if it does not yet exist locally
                                    If Not targetMzMLFile.Exists Then
                                        eLogMsgTypeIfNotFound = clsLogTools.LogLevels.ERROR

                                        blnFileCopied = mAnalysisResources.CopyFileToWorkDir(sourceMzMLFile.Name, sourceMzMLFile.DirectoryName,
                                                                                             localFolderPath, eLogMsgTypeIfNotFound)

                                        If blnFileCopied Then
                                            OnStatusEvent("Copied " + sourceMzMLFile.Name + " from folder " + sourceMzMLFile.DirectoryName)
                                            lstFoundFiles.Add(Path.Combine(localFolderPath, sourceMzMLFile.Name))

                                        End If
                                    End If

                                End If
                            End If
                        Else

                            ' Find the CDTA file
                            If udtOptions.CreateJobPathFiles Then

                                Dim strErrorMessage As String = String.Empty
                                Dim sourceConcatenatedDTAFilePath As String = mAnalysisResources.FindCDTAFile(strErrorMessage)

                                If String.IsNullOrEmpty(sourceConcatenatedDTAFilePath) Then
                                    OnErrorEvent(strErrorMessage)
                                    mAnalysisResources.RestoreCachedDataAndJobInfo()
                                    Return False
                                Else
                                    lstFoundFiles.Add(sourceConcatenatedDTAFilePath)
                                End If
                            Else
                                If Not mAnalysisResources.RetrieveDtaFiles() Then
                                    'Errors were reported in function call, so just return
                                    mAnalysisResources.RestoreCachedDataAndJobInfo()
                                    Return False
                                Else
                                    ' The retrieved file is probably named Dataset_dta.zip
                                    ' We'll add it to lstFoundFiles, but the exact name is not critical
                                    lstFoundFiles.Add(Path.Combine(workingDir, datasetName + "_dta.zip"))
                                End If
                            End If
                        End If

                    End If

                    If udtOptions.CreateJobPathFiles Then
                        Dim strJobInfoFilePath As String = GetJobInfoFilePath(dataPkgJob.Job)
                        Using swJobInfoFile = New StreamWriter(New FileStream(strJobInfoFilePath, FileMode.Append, FileAccess.Write, FileShare.Read))
                            For Each filePath In lstFoundFiles
                                swJobInfoFile.WriteLine(filePath)
                            Next
                        End Using
                    Else
                        ' Unzip any mzid files that were found
                        If zipFileCandidates.Count > 0 OrElse gzipFileCandidates.Count > 0 Then

                            Dim unzippedFilePath = String.Empty

                            For Each gzipCandidate In gzipFileCandidates
                                Dim fiFileToUnzip = New FileInfo(Path.Combine(workingDir, gzipCandidate))
                                If fiFileToUnzip.Exists Then
                                    ionicZipTools.GUnzipFile(fiFileToUnzip.FullName)
                                    unzippedFilePath = MostRecentUnzippedFile(ionicZipTools)
                                    Exit For
                                End If
                            Next

                            If String.IsNullOrEmpty(unzippedFilePath) Then
                                For Each zipCandidate In zipFileCandidates
                                    Dim fiFileToUnzip = New FileInfo(Path.Combine(workingDir, zipCandidate))
                                    If fiFileToUnzip.Exists Then
                                        ionicZipTools.UnzipFile(fiFileToUnzip.FullName)
                                        unzippedFilePath = MostRecentUnzippedFile(ionicZipTools)
                                        Exit For
                                    End If
                                Next
                            End If

                            If String.IsNullOrEmpty(unzippedFilePath) Then
                                OnErrorEvent("Could not find either the _msgfplus.zip file or the _msgfplus.mzid.gz file for dataset")
                                mAnalysisResources.RestoreCachedDataAndJobInfo()
                                Return False
                            End If

                            If blnPrefixRequired Then
                                If RenameDuplicatePHRPFile(workingDir, datasetName & "_msgfplus.mzid",
                                                               workingDir, "Job" & dataPkgJob.Job.ToString() & "_",
                                                               dataPkgJob.Job, unzippedFilePath) Then
                                Else
                                    mAnalysisResources.RestoreCachedDataAndJobInfo()
                                    Return False
                                End If
                            End If

                            lstFoundFiles.Add(unzippedFilePath)

                        End If

                        If Not String.IsNullOrWhiteSpace(zippedPepXmlFile) Then
                            ' Unzip _pepXML.zip if it exists
                            Dim fiFileToUnzip = New FileInfo(Path.Combine(workingDir, zippedPepXmlFile))
                            If fiFileToUnzip.Exists Then
                                ionicZipTools.UnzipFile(fiFileToUnzip.FullName)
                                lstFoundFiles.Add(MostRecentUnzippedFile(ionicZipTools))
                            End If
                        End If
                    End If

                End If

                ' Find the instrument data file or folder if a new dataset
                If Not dctRawFileRetrievalCommands.ContainsKey(dataPkgJob.DatasetID) Then
                    If Not RetrieveDataPackageInstrumentFile(dataPkgJob, udtOptions, dctRawFileRetrievalCommands,
                                                             dctInstrumentDataToRetrieve, dctDatasetRawFilePaths) Then
                        mAnalysisResources.RestoreCachedDataAndJobInfo()
                        Return False
                    End If
                End If

                intJobsProcessed += 1
                Dim sngProgress = clsAnalysisToolRunnerBase.ComputeIncrementalProgress(progressPercentAtStart, progressPercentAtFinish, intJobsProcessed, lstDataPackagePeptideHitJobs.Count + lstAdditionalJobs.Count)

                OnProgressUpdate("RetrieveDataPackagePeptideHitJobPHRPFiles (PeptideHit Jobs)", sngProgress)


            Next     ' dataPkgJob in lstDataPackagePeptideHitJobs

            ' Now process the additional jobs to retrieve the instrument data for each one
            For Each dataPkgJob In lstAdditionalJobs

                ' Find the instrument data file or folder if a new dataset
                If Not dctRawFileRetrievalCommands.ContainsKey(dataPkgJob.DatasetID) Then
                    If Not RetrieveDataPackageInstrumentFile(dataPkgJob, udtOptions, dctRawFileRetrievalCommands,
                                                             dctInstrumentDataToRetrieve, dctDatasetRawFilePaths) Then
                        mAnalysisResources.RestoreCachedDataAndJobInfo()
                        Return False
                    End If
                End If

                intJobsProcessed += 1
                Dim sngProgress = clsAnalysisToolRunnerBase.ComputeIncrementalProgress(progressPercentAtStart, progressPercentAtFinish, intJobsProcessed, lstDataPackagePeptideHitJobs.Count + lstAdditionalJobs.Count)

                OnProgressUpdate("RetrieveDataPackagePeptideHitJobPHRPFiles (Additional Jobs)", sngProgress)


            Next        ' in lstAdditionalJobs

            ' Look for any datasets that are associated with this data package yet have no jobs
            For Each datasetItem In dctDataPackageDatasets
                If Not dctRawFileRetrievalCommands.ContainsKey(datasetItem.Key) Then

                    Dim dataPkgJob = clsAnalysisResources.GetPseudoDataPackageJobInfo(datasetItem.Value)
                    dataPkgJob.ResultsFolderName = "Undefined_Folder"

                    If Not mAnalysisResources.OverrideCurrentDatasetAndJobInfo(dataPkgJob) Then
                        ' Error message has already been logged
                        mAnalysisResources.RestoreCachedDataAndJobInfo()
                        Return False
                    End If

                    If Not RetrieveDataPackageInstrumentFile(dataPkgJob, udtOptions, dctRawFileRetrievalCommands,
                                                             dctInstrumentDataToRetrieve, dctDatasetRawFilePaths) Then
                        mAnalysisResources.RestoreCachedDataAndJobInfo()
                        Return False
                    End If
                End If
            Next

            ' Restore the dataset and job info for this aggregation job
            mAnalysisResources.RestoreCachedDataAndJobInfo()

            If dctRawFileRetrievalCommands.Count = 0 Then
                OnErrorEvent("Did not find any datasets associated with this job's data package ID (" & mDataPackageInfoLoader.DataPackageID & ")")
                Return False
            End If

            If dctRawFileRetrievalCommands.Count > 0 Then
                ' Create a batch file with commands for retrieve the dataset files
                Dim strBatchFilePath As String
                strBatchFilePath = Path.Combine(workingDir, "RetrieveInstrumentData.bat")
                Using swOutfile = New StreamWriter(New FileStream(strBatchFilePath, FileMode.Create, FileAccess.Write, FileShare.Read))
                    For Each item As String In dctRawFileRetrievalCommands.Values
                        swOutfile.WriteLine(item)
                    Next
                End Using

                ' Store the dataset paths in a Packed Job Parameter
                mAnalysisResources.StorePackedJobParameterDictionary(dctDatasetRawFilePaths, clsAnalysisResources.JOB_PARAM_DICTIONARY_DATASET_FILE_PATHS)

            End If

            If udtOptions.RetrieveMzXMLFile Then
                ' All of the PHRP data files have been successfully retrieved; now retrieve the mzXML files or the .Raw files
                ' If udtOptions.CreateJobPathFiles = True then we will create StoragePathInfo files
                blnSuccess = RetrieveDataPackageMzXMLFiles(dctInstrumentDataToRetrieve, udtOptions)
            Else
                blnSuccess = True
            End If

        Catch ex As Exception
            OnErrorEvent("RetrieveDataPackagePeptideHitJobPHRPFiles; Exception during copy of file: " + sourceFilename + " from folder " + sourceFolderPath, ex)
            blnSuccess = False
        End Try

        Return blnSuccess

    End Function


    ''' <summary>
    ''' Look for the .mzXML and/or .raw file
    ''' </summary>
    ''' <param name="dataPkgJob"></param>
    ''' <param name="udtOptions"></param>
    ''' <param name="dctRawFileRetrievalCommands">Commands to copy .raw files to the local computer (to be placed in a batch file)</param>
    ''' <param name="dctInstrumentDataToRetrieve">Instrument files that need to be copied locally so that an mzXML file can be made</param>
    ''' <param name="dctDatasetRawFilePaths">Mapping of dataset name to the remote location of the .raw file</param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Private Function RetrieveDataPackageInstrumentFile(
      dataPkgJob As clsDataPackageJobInfo,
      udtOptions As udtDataPackageRetrievalOptionsType,
      dctRawFileRetrievalCommands As IDictionary(Of Integer, String),
      dctInstrumentDataToRetrieve As IDictionary(Of clsDataPackageJobInfo, KeyValuePair(Of String, String)),
      dctDatasetRawFilePaths As IDictionary(Of String, String)) As Boolean

        If udtOptions.RetrieveMzXMLFile Then
            ' See if a .mzXML file already exists for this dataset
            Dim mzXMLFilePath As String
            Dim hashcheckFilePath As String = String.Empty

            mzXMLFilePath = mAnalysisResources.FindMZXmlFile(hashcheckFilePath)

            If String.IsNullOrEmpty(mzXMLFilePath) Then
                ' mzXML file not found
                If dataPkgJob.RawDataType = clsAnalysisResources.RAW_DATA_TYPE_DOT_RAW_FILES Then
                    ' Will need to retrieve the .Raw file for this dataset
                    dctInstrumentDataToRetrieve.Add(dataPkgJob, New KeyValuePair(Of String, String)(String.Empty, String.Empty))
                ElseIf udtOptions.RetrieveMzXMLFile Then
                    OnErrorEvent("mzXML file not found for dataset " & dataPkgJob.Dataset &
                             " and dataset file type is not a .Raw file and we thus cannot auto-create the missing mzXML file")
                    Return False
                End If
            Else
                dctInstrumentDataToRetrieve.Add(dataPkgJob, New KeyValuePair(Of String, String)(mzXMLFilePath, hashcheckFilePath))
            End If
        End If

        Dim blnIsFolder = False
        Dim rawFilePath = mAnalysisResources.FindDatasetFileOrFolder(blnIsFolder, udtOptions.AssumeInstrumentDataUnpurged)

        If Not String.IsNullOrEmpty(rawFilePath) Then

            Dim strCopyCommand As String
            If blnIsFolder Then
                strCopyCommand = "copy " & rawFilePath & " .\" & Path.GetFileName(rawFilePath) & " /S /I"
            Else
                ' Make sure the case of the filename matches the case of the dataset name
                ' Also, make sure the extension is lowercase
                strCopyCommand = "copy " & rawFilePath & " " & dataPkgJob.Dataset & Path.GetExtension(rawFilePath).ToLower()
            End If
            dctRawFileRetrievalCommands.Add(dataPkgJob.DatasetID, strCopyCommand)
            dctDatasetRawFilePaths.Add(dataPkgJob.Dataset, rawFilePath)
        End If

        Return True

    End Function

    ''' <summary>
    ''' Retrieve the .mzXML files for the jobs in dctInstrumentDataToRetrieve
    ''' </summary>
    ''' <param name="dctInstrumentDataToRetrieve">The keys in this dictionary are JobInfo entries; the values in this dictionary are KeyValuePairs of path to the .mzXML file and path to the .hashcheck file (if any); the KeyValuePair will have empty strings if the .Raw file needs to be retrieved</param>
    ''' <param name="udtOptions">File retrieval options</param>
    ''' <returns>True if success, false if an error</returns>
    ''' <remarks>If udtOptions.CreateJobPathFiles is True, then will create StoragePathInfo files for the .mzXML or .Raw files</remarks>
    Public Function RetrieveDataPackageMzXMLFiles(
      dctInstrumentDataToRetrieve As Dictionary(Of clsDataPackageJobInfo, KeyValuePair(Of String, String)),
      udtOptions As udtDataPackageRetrievalOptionsType) As Boolean

        Dim blnSuccess As Boolean
        Dim createStoragePathInfoOnly As Boolean

        Dim intCurrentJob = 0

        Try

            ' Make sure we don't move the .Raw, .mzXML, .mzML or .gz files into the results folder
            mAnalysisResources.AddResultFileExtensionToSkip(clsAnalysisResources.DOT_RAW_EXTENSION)             ' .Raw file
            mAnalysisResources.AddResultFileExtensionToSkip(clsAnalysisResources.DOT_MZXML_EXTENSION)           ' .mzXML file
            mAnalysisResources.AddResultFileExtensionToSkip(clsAnalysisResources.DOT_MZML_EXTENSION)            ' .mzML file
            mAnalysisResources.AddResultFileExtensionToSkip(clsAnalysisResources.DOT_GZ_EXTENSION)              ' .gz file

            If udtOptions.CreateJobPathFiles Then
                createStoragePathInfoOnly = True
            Else
                createStoragePathInfoOnly = False
            End If

            Dim lstDatasetsProcessed = New SortedSet(Of String)

            mAnalysisResources.CacheCurrentDataAndJobInfo()

            Dim dtLastProgressUpdate = Date.UtcNow
            Dim datasetsProcessed = 0
            Dim datasetsToProcess = dctInstrumentDataToRetrieve.Count

            ' Retrieve the instrument data
            ' Note that RetrieveMZXmlFileUsingSourceFile will add MyEMSL files to the download queue
            For Each kvItem In dctInstrumentDataToRetrieve

                ' The key in kvMzXMLFileInfo is the path to the .mzXML or .mzML file
                ' The value in kvMzXMLFileInfo is the path to the .hashcheck file
                Dim kvMzXMLFileInfo As KeyValuePair(Of String, String) = kvItem.Value
                Dim strMzXMLFilePath As String = kvMzXMLFileInfo.Key
                Dim strHashcheckFilePath As String = kvMzXMLFileInfo.Value

                intCurrentJob = kvItem.Key.Job

                If Not lstDatasetsProcessed.Contains(kvItem.Key.Dataset) Then

                    If Not mAnalysisResources.OverrideCurrentDatasetAndJobInfo(kvItem.Key) Then
                        ' Error message has already been logged
                        Return False
                    End If

                    If String.IsNullOrEmpty(strMzXMLFilePath) Then
                        ' The .mzXML or .mzML file was not found; we will need to obtain the .Raw file
                        blnSuccess = False
                    Else
                        ' mzXML or .mzML file exists; either retrieve it or create a StoragePathInfo file
                        blnSuccess = mAnalysisResources.RetrieveMZXmlFileUsingSourceFile(createStoragePathInfoOnly, strMzXMLFilePath, strHashcheckFilePath)
                    End If

                    If blnSuccess Then
                        ' .mzXML or .mzML file found and copied locally
                        Dim msXmlFileExtension As String

                        If strMzXMLFilePath.ToLower().EndsWith(clsAnalysisResources.DOT_GZ_EXTENSION) Then
                            msXmlFileExtension = Path.GetExtension(strMzXMLFilePath.Substring(0, strMzXMLFilePath.Length - clsAnalysisResources.DOT_GZ_EXTENSION.Length))
                        Else
                            msXmlFileExtension = Path.GetExtension(strMzXMLFilePath)
                        End If

                        If udtOptions.CreateJobPathFiles Then
                            OnStatusEvent(msXmlFileExtension & " file found for job " & intCurrentJob & " at " & strMzXMLFilePath)
                        Else
                            OnStatusEvent("Copied " & msXmlFileExtension & " file for job " & intCurrentJob & " from " & strMzXMLFilePath)
                        End If
                    Else
                        ' .mzXML file not found (or problem adding to the MyEMSL download queue)
                        ' Find or retrieve the .Raw file, which can be used to create the .mzXML file (the plugin will actually perform the work of converting the file; as an example, see the MSGF plugin)

                        If Not mAnalysisResources.RetrieveSpectra(kvItem.Key.RawDataType, createStoragePathInfoOnly, maxAttempts:=1) Then
                            OnErrorEvent("Error occurred retrieving instrument data file for job " & intCurrentJob)
                            Return False
                        End If

                    End If

                    lstDatasetsProcessed.Add(kvItem.Key.Dataset)
                End If

                datasetsProcessed += 1

                ' Compute a % complete value between 0 and 2%                
                Dim percentComplete = CSng(datasetsProcessed) / datasetsToProcess * 2
                OnProgressUpdate("Retrieving MzXML files", percentComplete)

                If (Date.UtcNow.Subtract(dtLastProgressUpdate).TotalSeconds >= 30) Then

                    dtLastProgressUpdate = Date.UtcNow

                    OnStatusEvent("Retrieving mzXML files: " & datasetsProcessed & " / " & datasetsToProcess & " datasets")
                End If

            Next kvItem

            ' Restore the dataset and job info for this aggregation job
            mAnalysisResources.RestoreCachedDataAndJobInfo()

            blnSuccess = True

        Catch ex As Exception
            OnErrorEvent("RetrieveDataPackageMzXMLFiles; Exception retrieving mzXML file or .Raw file for job " & intCurrentJob, ex)
            blnSuccess = False
        End Try

        Return blnSuccess

    End Function

#Region "Event Handlers"

    Private Sub ProcedureExecutor_DebugEvent(message As String)
        OnStatusEvent(message)

    End Sub

    Private Sub ProcedureExecutor_DBErrorEvent(message As String)
        If message.Contains("permission was denied") Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, message)
        End If
        OnErrorEvent(message)
    End Sub

#End Region
End Class

