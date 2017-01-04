'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2007, Battelle Memorial Institute
' Created 12/19/2007
'*********************************************************************************************************

Option Strict On

Imports System.Data.SqlClient
Imports PHRPReader
Imports System.IO
Imports System.Runtime.InteropServices
Imports System.Text.RegularExpressions
Imports System.Threading
Imports MyEMSLReader
Imports ParamFileGenerator.MakeParams

Public MustInherit Class clsAnalysisResources
    Inherits clsAnalysisMgrBase
    Implements IAnalysisResources

    '*********************************************************************************************************
    'Base class for job resource class
    '*********************************************************************************************************

#Region "Constants"
    Protected Const DEFAULT_FILE_EXISTS_RETRY_HOLDOFF_SECONDS As Integer = 15
    Protected Const DEFAULT_FOLDER_EXISTS_RETRY_HOLDOFF_SECONDS As Integer = 5

    ''' <summary>
    ''' Maximum number of attempts to find a folder or file
    ''' </summary>
    ''' <remarks></remarks>
    Protected Const DEFAULT_MAX_RETRY_COUNT As Integer = 3

    Protected Const FASTA_GEN_TIMEOUT_INTERVAL_MINUTES As Integer = 65

    Public Const MYEMSL_PATH_FLAG As String = clsMyEMSLUtilities.MYEMSL_PATH_FLAG

    Protected Const FORCE_WINHPC_FS As Boolean = True

    ' Define the maximum file size to process using IonicZip; 
    '  the reason we don't want to process larger files is that IonicZip is 1.5x to 2x slower than PkZip
    '  For example, given a 1.9 GB _isos.csv file zipped to a 660 MB .Zip file:
    '   SharpZipLib unzips the file in 130 seconds
    '   WinRar      unzips the file in 120 seconds
    '   PKZipC      unzips the file in  84 seconds
    '
    ' Re-tested on 1/7/2011 with a 611 MB file
    '   IonicZip    unzips the file in 70 seconds (reading/writing to the same drive)
    '   IonicZip    unzips the file in 62 seconds (reading/writing from different drives)
    '   WinRar      unzips the file in 36 seconds (reading/writing from different drives)
    '   PKZipC      unzips the file in 38 seconds (reading/writing from different drives)
    '
    ' For smaller files, the speed differences are much less noticable

    Protected Const IONIC_ZIP_MAX_FILESIZE_MB As Integer = 1280

    ' Note: All of the RAW_DATA_TYPE constants need to be all lowercase
    '
    Public Const RAW_DATA_TYPE_DOT_D_FOLDERS As String = "dot_d_folders"                'Agilent ion trap data, Agilent TOF data
    Public Const RAW_DATA_TYPE_ZIPPED_S_FOLDERS As String = "zipped_s_folders"          'FTICR data, including instrument 3T_FTICR, 7T_FTICR, 9T_FTICR, 11T_FTICR, 11T_FTICR_B, and 12T_FTICR 
    Public Const RAW_DATA_TYPE_DOT_RAW_FOLDER As String = "dot_raw_folder"              'Micromass QTOF data
    Public Const RAW_DATA_TYPE_DOT_RAW_FILES As String = "dot_raw_files"                'Finnigan ion trap/LTQ-FT data
    Public Const RAW_DATA_TYPE_DOT_WIFF_FILES As String = "dot_wiff_files"              'Agilent/QSTAR TOF data
    Public Const RAW_DATA_TYPE_DOT_UIMF_FILES As String = "dot_uimf_files"              'IMS_UIMF (IMS_Agilent_TOF in DMS)
    Public Const RAW_DATA_TYPE_DOT_MZXML_FILES As String = "dot_mzxml_files"            'mzXML
    Public Const RAW_DATA_TYPE_DOT_MZML_FILES As String = "dot_mzml_files"              'mzML

    ' 12T datasets acquired prior to 7/16/2010 use a Bruker data station and have an analysis.baf file, 0.ser folder, and a XMASS_Method.m subfolder with file apexAcquisition.method
    ' Datasets will have an instrument name of 12T_FTICR and raw_data_type of "zipped_s_folders"

    ' 12T datasets acquired after 9/1/2010 use the Agilent data station, and thus have a .D folder
    ' Datasets will have an instrument name of 12T_FTICR_B and raw_data_type of "bruker_ft"
    ' 15T datasets also have raw_data_type "bruker_ft"
    ' Inside the .D folder is the analysis.baf file; there is also .m subfolder that has a apexAcquisition.method file
    Public Const RAW_DATA_TYPE_BRUKER_FT_FOLDER As String = "bruker_ft"

    ' The following is used by BrukerTOF_01 (e.g. Bruker TOF_TOF)
    ' Folder has a .EMF file and a single sub-folder that has an acqu file and fid file
    Public Const RAW_DATA_TYPE_BRUKER_MALDI_SPOT As String = "bruker_maldi_spot"

    ' The following is used by instruments 9T_FTICR_Imaging and BrukerTOF_Imaging_01
    ' Series of zipped subfolders, with names like 0_R00X329.zip; subfolders inside the .Zip files have fid files
    Public Const RAW_DATA_TYPE_BRUKER_MALDI_IMAGING As String = "bruker_maldi_imaging"

    ' The following is used by instrument Maxis_01
    ' Inside the .D folder is the analysis.baf file; there is also .m subfolder that has a microTOFQMaxAcquisition.method file; there is not a ser or fid file
    Public Const RAW_DATA_TYPE_BRUKER_TOF_BAF_FOLDER As String = "bruker_tof_baf"

    Public Const RESULT_TYPE_SEQUEST As String = "Peptide_Hit"
    Public Const RESULT_TYPE_XTANDEM As String = "XT_Peptide_Hit"
    Public Const RESULT_TYPE_INSPECT As String = "IN_Peptide_Hit"
    Public Const RESULT_TYPE_MSGFPLUS As String = "MSG_Peptide_Hit"           ' Used for MSGFDB and MSGF+
    Public Const RESULT_TYPE_MSALIGN As String = "MSA_Peptide_Hit"
    Public Const RESULT_TYPE_MODA As String = "MODa_Peptide_Hit"
    Public Const RESULT_TYPE_MODPLUS As String = "MODPlus_Peptide_Hit"
    Public Const RESULT_TYPE_MSPATHFINDER As String = "MSP_Peptide_Hit"

    Public Const DOT_WIFF_EXTENSION As String = ".wiff"
    Public Const DOT_D_EXTENSION As String = ".d"
    Public Const DOT_RAW_EXTENSION As String = ".raw"
    Public Const DOT_UIMF_EXTENSION As String = ".uimf"

    Public Const DOT_GZ_EXTENSION As String = ".gz"
    Public Const DOT_MZXML_EXTENSION As String = ".mzXML"
    Public Const DOT_MZML_EXTENSION As String = ".mzML"

    Public Const DOT_MGF_EXTENSION As String = ".mgf"
    Public Const DOT_CDF_EXTENSION As String = ".cdf"

    Public Const DOT_PBF_EXTENSION As String = ".pbf"

    Public Const LOCK_FILE_EXTENSION As String = ".lock"

    ''' <summary>
    ''' Feature file, generated by the ProMex tool
    ''' </summary>
    ''' <remarks></remarks>
    Public Const DOT_MS1FT_EXTENSION As String = ".ms1ft"

    Public Const STORAGE_PATH_INFO_FILE_SUFFIX As String = "_StoragePathInfo.txt"

    Public Const SCAN_STATS_FILE_SUFFIX As String = "_ScanStats.txt"
    Public Const SCAN_STATS_EX_FILE_SUFFIX As String = "_ScanStatsEx.txt"

    Public Const REPORTERIONS_FILE_SUFFIX As String = "_ReporterIons.txt"

    Public Const DATA_PACKAGE_SPECTRA_FILE_SUFFIX As String = "_SpectraFile"

    Public Const BRUKER_ZERO_SER_FOLDER As String = "0.ser"
    Public Const BRUKER_SER_FILE As String = "ser"
    Public Const BRUKER_FID_FILE As String = "fid"

    Public Const JOB_PARAM_DICTIONARY_DATASET_FILE_PATHS As String = "PackedParam_DatasetFilePaths"

    ' This is used by clsAnalysisResourcesRepoPkgr
    Public Const JOB_PARAM_DICTIONARY_DATASET_RAW_DATA_TYPES = "PackedParam_DatasetRawDataTypes"

    ' These are used by clsAnalysisResourcesPhosphoFdrAggregator
    Public Const JOB_PARAM_DICTIONARY_JOB_DATASET_MAP As String = "PackedParam_JobDatasetMap"
    Public Const JOB_PARAM_DICTIONARY_JOB_SETTINGS_FILE_MAP As String = "PackedParam_JobSettingsFileMap"
    Public Const JOB_PARAM_DICTIONARY_JOB_TOOL_MAP As String = "PackedParam_JobToolNameMap"

    Public Const JOB_INFO_FILE_PREFIX As String = "JobInfoFile_Job"

    ' This constant is used by clsAnalysisToolRunnerMSGFDB, clsAnalysisResourcesMSGFDB, and clsAnalysisResourcesDtaRefinery
    Public Const SPECTRA_ARE_NOT_CENTROIDED As String = "None of the spectra are centroided; unable to process"

    Public Enum eRawDataTypeConstants
        Unknown = 0
        ThermoRawFile = 1
        UIMF = 2
        mzXML = 3
        mzML = 4
        AgilentDFolder = 5              ' Agilent ion trap data, Agilent TOF data
        AgilentQStarWiffFile = 6
        MicromassRawFolder = 7          ' Micromass QTOF data
        ZippedSFolders = 8              ' FTICR data, including instrument 3T_FTICR, 7T_FTICR, 9T_FTICR, 11T_FTICR, 11T_FTICR_B, and 12T_FTICR 
        BrukerFTFolder = 9              ' .D folder is the analysis.baf file; there is also .m subfolder that has a apexAcquisition.method file
        BrukerMALDISpot = 10            ' has a .EMF file and a single sub-folder that has an acqu file and fid file
        BrukerMALDIImaging = 11         ' Series of zipped subfolders, with names like 0_R00X329.zip; subfolders inside the .Zip files have fid files
        BrukerTOFBaf = 12               ' Used by Maxis01; Inside the .D folder is the analysis.baf file; there is also .m subfolder that has a microTOFQMaxAcquisition.method file; there is not a ser or fid file
    End Enum

    Public Enum MSXMLOutputTypeConstants
        mzXML = 0
        mzML = 1
    End Enum

    Public Enum DataPackageFileRetrievalModeConstants
        Undefined = 0
        Ascore = 1
    End Enum

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
        ''' <remarks></remarks>
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

    Public Structure udtHPCOptionsType
        Public HeadNode As String
        Public UsingHPC As Boolean
        Public SharePath As String
        Public ResourceType As String
        ' Obsolete parameter; no longer used: Public NodeGroup As String
        Public MinimumMemoryMB As Integer
        Public MinimumCores As Integer
        Public WorkDirPath As String
    End Structure

#End Region

#Region "Module variables"
    Protected m_jobParams As IJobParams
    Protected m_mgrParams As IMgrParams
    Protected m_WorkingDir As String
    Protected m_JobNum As Integer
    Protected m_DatasetName As String
    Protected m_MgrName As String

    Protected m_StatusTools As IStatusFile      ' Might be nothing

    Protected m_FastaToolsCnStr As String = ""
    Protected m_FastaFileName As String = ""

    Protected WithEvents m_FastaTools As Protein_Exporter.ExportProteinCollectionsIFC.IGetFASTAFromDMS
    Protected m_IonicZipTools As clsIonicZipTools

    Protected WithEvents m_CDTAUtilities As clsCDTAUtilities

    Protected WithEvents m_SplitFastaFileUtility As clsSplitFastaFileUtilities

    Protected m_SplitFastaLastUpdateTime As DateTime
    Protected m_SplitFastaLastPercentComplete As Integer

    Protected m_MyEMSLUtilities As clsMyEMSLUtilities

    Private Shared mLastJobParameterFromHistoryLookup As DateTime = Date.UtcNow

    Private m_ResourceOptions As Dictionary(Of clsGlobal.eAnalysisResourceOptions, Boolean)

    Private m_AuroraAvailable As Boolean
    Private m_MyEmslAvailable As Boolean

    Public WithEvents mSpectraTypeClassifier As SpectraTypeClassifier.clsSpectrumTypeClassifier

#End Region

#Region "Properties"

    Public ReadOnly Property MyEMSLAvailable() As Boolean
        Get
            Return m_MyEmslAvailable AndAlso Not MyEMSLSearchDisabled
        End Get
    End Property

    Public Property MyEMSLSearchDisabled() As Boolean

    ' Explanation of what happened to last operation this class performed
    Public ReadOnly Property Message() As String Implements IAnalysisResources.Message
        Get
            Return m_message
        End Get
    End Property
#End Region

#Region "Methods"
    ''' <summary>
    ''' Constructor
    ''' </summary>
    ''' <remarks></remarks>
    Public Sub New()
        MyBase.New("clsAnalysisResources")
        m_CDTAUtilities = New clsCDTAUtilities
    End Sub

    ''' <summary>
    ''' Initialize class
    ''' </summary>
    ''' <param name="mgrParams">Manager parameter object</param>
    ''' <param name="jobParams">Job parameter object</param>
    ''' <param name="statusTools">Object for status reporting</param>
    ''' <param name="myEMSLUtilities">MyEMSL download Utilities</param>
    ''' <remarks></remarks>
    Public Overridable Sub Setup(
      mgrParams As IMgrParams,
      jobParams As IJobParams,
      statusTools As IStatusFile,
      myEMSLUtilities As clsMyEMSLUtilities) Implements IAnalysisResources.Setup

        m_mgrParams = mgrParams
        m_jobParams = jobParams

        m_DebugLevel = CShort(m_mgrParams.GetParam("debuglevel", 1))
        m_FastaToolsCnStr = m_mgrParams.GetParam("fastacnstring")
        m_MgrName = m_mgrParams.GetParam("MgrName", "Undefined-Manager")

        m_WorkingDir = m_mgrParams.GetParam("workdir")

        Dim jobNum = m_jobParams.GetParam("StepParameters", "Job")
        If Not String.IsNullOrEmpty(jobNum) Then
            Integer.TryParse(jobNum, m_JobNum)
        End If

        m_DatasetName = m_jobParams.GetParam("JobParameters", "DatasetNum")

        m_IonicZipTools = New clsIonicZipTools(m_DebugLevel, m_WorkingDir)

        MyBase.InitFileTools(m_MgrName, m_DebugLevel)

        If myEMSLUtilities Is Nothing Then
            m_MyEMSLUtilities = New clsMyEMSLUtilities(m_DebugLevel, m_WorkingDir)
        Else
            m_MyEMSLUtilities = myEMSLUtilities
        End If

        AddHandler m_MyEMSLUtilities.ErrorEvent, AddressOf m_MyEMSLUtilities_ErrorEvent
        AddHandler m_MyEMSLUtilities.WarningEvent, AddressOf m_MyEMSLUtilities_WarningEvent

        m_ResourceOptions = New Dictionary(Of clsGlobal.eAnalysisResourceOptions, Boolean)
        SetOption(clsGlobal.eAnalysisResourceOptions.OrgDbRequired, False)

        m_StatusTools = statusTools

        m_AuroraAvailable = m_mgrParams.GetParam("AuroraAvailable", True)

        m_MyEmslAvailable = m_mgrParams.GetParam("MyEmslAvailable", True)

    End Sub

    Public MustOverride Function GetResources() As IJobParams.CloseOutType Implements IAnalysisResources.GetResources

    Public Function GetOption(resourceOption As clsGlobal.eAnalysisResourceOptions) As Boolean Implements IAnalysisResources.GetOption
        If m_ResourceOptions Is Nothing Then Return False

        Dim enabled As Boolean
        If m_ResourceOptions.TryGetValue(resourceOption, enabled) Then
            Return enabled
        Else
            Return False
        End If

    End Function

    ''' <summary>
    ''' Gets resources required by all step tools
    ''' </summary>
    ''' <returns>True if success, false if an error</returns>
    Protected Function GetSharedResources() As IJobParams.CloseOutType

        Dim success = GetExistingJobParametersFile()

        If success Then
            Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
        Else
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

    End Function

    Public Sub SetOption(resourceOption As clsGlobal.eAnalysisResourceOptions, enabled As Boolean) Implements IAnalysisResources.SetOption
        If m_ResourceOptions Is Nothing Then
            m_ResourceOptions = New Dictionary(Of clsGlobal.eAnalysisResourceOptions, Boolean)
        End If

        If m_ResourceOptions.ContainsKey(resourceOption) Then
            m_ResourceOptions(resourceOption) = enabled
        Else
            m_ResourceOptions.Add(resourceOption, enabled)
        End If

    End Sub

    Private Sub AddMzIdFilesToFind(
      datasetName As String,
      splitFastaResultID As Integer,
      zipFileCandidates As List(Of String),
      gzipFileCandidates As List(Of String),
      lstFilesToGet As SortedList(Of String, Boolean))

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
        lstFilesToGet.Add(zipFile, False)
        lstFilesToGet.Add(gZipFile, False)

    End Sub

    ''' <summary>
    ''' Add a folder or file path to a list of paths to examine
    ''' </summary>
    ''' <param name="lstPathsToCheck">List of Tuples where the string is a folder or file path, and the boolean is logIfMissing</param>
    ''' <param name="folderOrFilePath">Path to add</param>
    ''' <param name="logIfMissing">True to log a message if the path is not found</param>
    ''' <remarks></remarks>
    Private Sub AddPathToCheck(lstPathsToCheck As ICollection(Of Tuple(Of String, Boolean)), folderOrFilePath As String, logIfMissing As Boolean)
        lstPathsToCheck.Add(New Tuple(Of String, Boolean)(folderOrFilePath, logIfMissing))
    End Sub

    ''' <summary>
    ''' Appends file specified file path to the JobInfo file for the given Job
    ''' </summary>
    ''' <param name="intJob"></param>
    ''' <param name="strFilePath"></param>
    ''' <remarks></remarks>
    Protected Sub AppendToJobInfoFile(intJob As Integer, strFilePath As String)

        Dim strJobInfoFilePath As String = GetJobInfoFilePath(intJob)

        Using swJobInfoFile = New StreamWriter(New FileStream(strJobInfoFilePath, FileMode.Append, FileAccess.Write, FileShare.Read))
            swJobInfoFile.WriteLine(strFilePath)
        End Using

    End Sub

    ''' <summary>
    ''' Look for a lock file named dataFilePath + ".lock"
    ''' If found, and if less than maxWaitTimeMinutes old, waits for it to be deleted by another process or to age
    ''' </summary>
    ''' <param name="dataFilePath">Data file path</param>
    ''' <param name="dataFileDescription">User friendly description of the data file, e.g. LipidMapsDB</param>
    ''' <param name="statusTools">Status Tools object</param>
    ''' <param name="maxWaitTimeMinutes">Maximum age of the lock file</param>
    ''' <remarks>
    ''' Typical steps for using lock files to assure that only one manager is creating a specific file
    ''' 1. Call CheckForLockFile() to check for a lock file; wait for it to age
    ''' 2. Once CheckForLockFile() exits, check for the required data file; exit the function if the desired file is found
    ''' 3. If the file was not found, create a new lock file by calling CreateLockFile()
    ''' 4. Do the work and create the data file, including copying to the central location
    ''' 5. Delete the lock file by calling DeleteLockFile() or by deleting the file path returned by CreateLockFile()
    ''' </remarks>
    Public Shared Sub CheckForLockFile(
       dataFilePath As String,
       dataFileDescription As String,
       statusTools As IStatusFile,
       Optional maxWaitTimeMinutes As Integer = 120,
       Optional logIntervalMinutes As Integer = 5)

        Dim blnWaitingForLockFile = False
        Dim dtLockFileCreated As DateTime

        ' Look for a recent .lock file
        Dim fiLockFile = New FileInfo(dataFilePath & LOCK_FILE_EXTENSION)

        If fiLockFile.Exists Then
            If Date.UtcNow.Subtract(fiLockFile.LastWriteTimeUtc).TotalMinutes < maxWaitTimeMinutes Then
                blnWaitingForLockFile = True
                dtLockFileCreated = fiLockFile.LastWriteTimeUtc

                Dim debugMessage = dataFileDescription & " lock file found; will wait for file to be deleted or age; " &
                    fiLockFile.Name & " created " & fiLockFile.LastWriteTime.ToString()
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, debugMessage)
                Console.WriteLine(debugMessage)
            Else
                ' Lock file has aged; delete it
                fiLockFile.Delete()
            End If
        End If

        If blnWaitingForLockFile Then

            Dim dtLastProgressTime = Date.UtcNow
            If logIntervalMinutes < 1 Then logIntervalMinutes = 1

            Do While blnWaitingForLockFile
                ' Wait 5 seconds
                Thread.Sleep(5000)

                fiLockFile.Refresh()

                If Not fiLockFile.Exists Then
                    blnWaitingForLockFile = False
                ElseIf Date.UtcNow.Subtract(dtLockFileCreated).TotalMinutes > maxWaitTimeMinutes Then
                    blnWaitingForLockFile = False
                Else
                    If Date.UtcNow.Subtract(dtLastProgressTime).TotalMinutes >= logIntervalMinutes Then
                        LogDebugMessage("Waiting for lock file " & fiLockFile.Name, statusTools)
                        dtLastProgressTime = Date.UtcNow
                    End If
                End If
            Loop

            fiLockFile.Refresh()
            If fiLockFile.Exists Then
                ' Lock file is over 2 hours old; delete it
                DeleteLockFile(dataFilePath)
            End If
        End If

    End Sub

    ''' <summary>
    ''' Create a new lock file named dataFilePath + ".lock"
    ''' </summary>
    ''' <param name="dataFilePath">Data file path</param>
    ''' <param name="taskDescription">Description of current task; will be written the lock file, followed by " at yyyy-MM-dd hh:mm:ss tt"</param>
    ''' <returns>Full path to the lock file</returns>
    ''' <remarks></remarks>
    Public Shared Function CreateLockFile(dataFilePath As String, taskDescription As String) As String

        Dim strLockFilePath = dataFilePath & LOCK_FILE_EXTENSION
        Using swLockFile = New StreamWriter(New FileStream(strLockFilePath, FileMode.CreateNew, FileAccess.Write, FileShare.Read))
            swLockFile.WriteLine(taskDescription & " at " & Now.ToString(clsAnalysisToolRunnerBase.DATE_TIME_FORMAT))
        End Using

        Return strLockFilePath

    End Function


    ''' <summary>
    ''' Delete the lock file for the correspond data file
    ''' </summary>
    ''' <param name="dataFilePath"></param>
    ''' <remarks></remarks>
    Public Shared Sub DeleteLockFile(dataFilePath As String)

        ' Delete the lock file
        Try
            Dim lockFilePath = dataFilePath & LOCK_FILE_EXTENSION

            Dim fiLockFile = New FileInfo(lockFilePath)
            If fiLockFile.Exists Then
                fiLockFile.Delete()
            End If

        Catch ex As Exception
            ' Ignore errors here
        End Try

    End Sub
    ''' <summary>
    ''' Copies the zipped s-folders to the working directory
    ''' </summary>
    ''' <param name="CreateStoragePathInfoOnly">
    ''' When true, then does not actually copy the specified files, 
    ''' but instead creates a series of files named s*.zip_StoragePathInfo.txt, 
    ''' and each file's first line will be the full path to the source file
    ''' </param>
    ''' <returns>TRUE for success; FALSE for failure</returns>
    ''' <remarks></remarks>
    Private Function CopySFoldersToWorkDir(createStoragePathInfoOnly As Boolean) As Boolean

        Dim DSFolderPath As String = FindValidFolder(m_DatasetName, "s*.zip", RetrievingInstrumentDataFolder:=True)

        Dim ZipFiles() As String
        Dim DestFilePath As String

        ' Verify dataset folder exists
        If Not Directory.Exists(DSFolderPath) Then Return False

        ' Get a listing of the zip files to process
        ZipFiles = Directory.GetFiles(DSFolderPath, "s*.zip")
        If ZipFiles.GetLength(0) < 1 Then Return False 'No zipped data files found

        ' Copy each of the s*.zip files to the working directory
        For Each ZipFilePath As String In ZipFiles

            If m_DebugLevel > 3 Then
                LogDebugMessage("Copying file " + ZipFilePath + " to work directory")
            End If

            DestFilePath = Path.Combine(m_WorkingDir, Path.GetFileName(ZipFilePath))

            If createStoragePathInfoOnly Then
                If Not CreateStoragePathInfoFile(ZipFilePath, DestFilePath) Then
                    LogError("Error creating storage path info file for " + ZipFilePath)
                    Return False
                End If
            Else
                If Not CopyFileWithRetry(ZipFilePath, DestFilePath, False) Then
                    LogError("Error copying file " + ZipFilePath)
                    Return False
                End If
            End If
        Next

        ' If we got to here, everything worked
        Return True

    End Function

    ''' <summary>
    ''' Copies a file with retries in case of failure
    ''' </summary>
    ''' <param name="SrcFilePath">Full path to source file</param>
    ''' <param name="DestFilePath">Full path to destination file</param>
    ''' <param name="Overwrite">TRUE to overwrite existing destination file; FALSE otherwise</param>
    ''' <returns>TRUE for success; FALSE for error</returns>
    ''' <remarks>Logs copy errors</remarks>
    Private Function CopyFileWithRetry(SrcFilePath As String, DestFilePath As String, Overwrite As Boolean) As Boolean
        Const MaxCopyAttempts = 3
        Return CopyFileWithRetry(SrcFilePath, DestFilePath, Overwrite, MaxCopyAttempts)
    End Function

    ''' <summary>
    ''' Copies a file with retries in case of failure
    ''' </summary>
    ''' <param name="srcFilePath">Full path to source file</param>
    ''' <param name="destFilePath">Full path to destination file</param>
    ''' <param name="overwrite">TRUE to overwrite existing destination file; FALSE otherwise</param>
    ''' <param name="maxCopyAttempts">Maximum number of attempts to make when errors are encountered while copying the file</param>
    ''' <returns>TRUE for success; FALSE for error</returns>
    ''' <remarks>Logs copy errors</remarks>
    Private Function CopyFileWithRetry(srcFilePath As String, destFilePath As String, overwrite As Boolean, maxCopyAttempts As Integer) As Boolean

        Const RETRY_HOLDOFF_SECONDS = 15

        If maxCopyAttempts < 1 Then maxCopyAttempts = 1
        Dim retryCount As Integer = maxCopyAttempts

        While retryCount > 0
            Try
                ResetTimestampForQueueWaitTimeLogging()
                If m_FileTools.CopyFileUsingLocks(srcFilePath, destFilePath, m_MgrName, overwrite) Then
                    Return True
                Else
                    ReportStatus("CopyFileUsingLocks returned false copying " & srcFilePath & " to " & destFilePath, 0, True)
                    Return False
                End If
            Catch ex As Exception
                ReportStatus("Exception copying file " + srcFilePath + " to " + destFilePath & "; Retry Count = " + retryCount.ToString, ex)

                retryCount -= 1

                If Not overwrite AndAlso File.Exists(destFilePath) Then
                    ReportStatus("Tried to overwrite an existing file when Overwrite = False: " + destFilePath, 0, True)
                    Return False
                End If

                Thread.Sleep(RETRY_HOLDOFF_SECONDS * 1000)       'Wait several seconds before retrying
            End Try
        End While

        'If we got to here, there were too many failures
        If retryCount < 1 Then
            LogError(m_message)
            Return False
        End If

        Return False

    End Function

    ''' <summary>
    ''' Copies specified file from storage server to local working directory
    ''' </summary>
    ''' <param name="sourceFileName">Name of file to copy</param>
    ''' <param name="sourceFolderPath">Path to folder where input file is located</param>
    ''' <param name="targetFolderPath">Destination directory for file copy</param>
    ''' <returns>TRUE for success; FALSE for failure</returns>
    ''' <remarks>If the file was found in MyEMSL, then InpFolder will be of the form \\MyEMSL@MyEMSLID_84327</remarks>
    Protected Function CopyFileToWorkDir(
      sourceFileName As String,
      sourceFolderPath As String,
      targetFolderPath As String) As Boolean

        Const MAX_ATTEMPTS = 3
        Return CopyFileToWorkDir(sourceFileName, sourceFolderPath, targetFolderPath, clsLogTools.LogLevels.ERROR, createStoragePathInfoOnly:=False, maxCopyAttempts:=MAX_ATTEMPTS)

    End Function

    ''' <summary>
    ''' Copies specified file from storage server to local working directory
    ''' </summary>
    ''' <param name="sourceFileName">Name of file to copy</param>
    ''' <param name="sourceFolderPath">Path to folder where input file is located</param>
    ''' <param name="targetFolderPath">Destination directory for file copy</param>
    ''' <param name="eLogMsgTypeIfNotFound">Type of message to log if the file is not found</param>
    ''' <returns>TRUE for success; FALSE for failure</returns>
    ''' <remarks>If the file was found in MyEMSL, then InpFolder will be of the form \\MyEMSL@MyEMSLID_84327</remarks>
    Protected Function CopyFileToWorkDir(
      sourceFileName As String,
      sourceFolderPath As String,
      targetFolderPath As String,
      eLogMsgTypeIfNotFound As clsLogTools.LogLevels) As Boolean

        Const MAX_ATTEMPTS = 3
        Return CopyFileToWorkDir(sourceFileName, sourceFolderPath, targetFolderPath, eLogMsgTypeIfNotFound, createStoragePathInfoOnly:=False, maxCopyAttempts:=MAX_ATTEMPTS)

    End Function

    ''' <summary>
    ''' Copies specified file from storage server to local working directory
    ''' </summary>
    ''' <param name="sourceFileName">Name of file to copy</param>
    ''' <param name="sourceFolderPath">Path to folder where input file is located</param>
    ''' <param name="targetFolderPath">Destination directory for file copy</param>
    ''' <param name="eLogMsgTypeIfNotFound">Type of message to log if the file is not found</param>
    ''' <param name="maxCopyAttempts">Maximum number of attempts to make when errors are encountered while copying the file</param>
    ''' <returns>TRUE for success; FALSE for failure</returns>
    ''' <remarks>If the file was found in MyEMSL, then InpFolder will be of the form \\MyEMSL@MyEMSLID_84327</remarks>
    Protected Function CopyFileToWorkDir(
      sourceFileName As String,
      sourceFolderPath As String,
      targetFolderPath As String,
      eLogMsgTypeIfNotFound As clsLogTools.LogLevels,
      maxCopyAttempts As Integer) As Boolean

        Return CopyFileToWorkDir(sourceFileName, sourceFolderPath, targetFolderPath, eLogMsgTypeIfNotFound, createStoragePathInfoOnly:=False, maxCopyAttempts:=maxCopyAttempts)

    End Function

    ''' <summary>
    ''' Copies specified file from storage server to local working directory
    ''' </summary>
    ''' <param name="sourceFileName">Name of file to copy</param>
    ''' <param name="sourceFolderPath">Path to folder where input file is located</param>
    ''' <param name="targetFolderPath">Destination directory for file copy</param>
    ''' <param name="eLogMsgTypeIfNotFound">Type of message to log if the file is not found</param>
    ''' <param name="CreateStoragePathInfoOnly">TRUE if a storage path info file should be created instead of copying the file</param>
    ''' <returns>TRUE for success; FALSE for failure</returns>
    ''' <remarks>If the file was found in MyEMSL, then InpFolder will be of the form \\MyEMSL@MyEMSLID_84327</remarks>
    Protected Function CopyFileToWorkDir(
      sourceFileName As String,
      sourceFolderPath As String,
      targetFolderPath As String,
      eLogMsgTypeIfNotFound As clsLogTools.LogLevels,
      createStoragePathInfoOnly As Boolean) As Boolean

        Const MAX_ATTEMPTS = 3
        Return CopyFileToWorkDir(sourceFileName, sourceFolderPath, targetFolderPath, eLogMsgTypeIfNotFound, createStoragePathInfoOnly, MAX_ATTEMPTS)

    End Function

    ''' <summary>
    ''' Copies specified file from storage server to local working directory
    ''' </summary>
    ''' <param name="sourceFileName">Name of file to copy</param>
    ''' <param name="sourceFolderPath">Path to folder where input file is located</param>
    ''' <param name="targetFolderPath">Destination directory for file copy</param>
    ''' <param name="eLogMsgTypeIfNotFound">Type of message to log if the file is not found</param>
    ''' <param name="createStoragePathInfoOnly">TRUE if a storage path info file should be created instead of copying the file</param>
    ''' <param name="maxCopyAttempts">Maximum number of attempts to make when errors are encountered while copying the file</param>
    ''' <returns>TRUE for success; FALSE for failure</returns>
    ''' <remarks>If the file was found in MyEMSL, then InpFolder will be of the form \\MyEMSL@MyEMSLID_84327</remarks>
    Protected Function CopyFileToWorkDir(
      sourceFileName As String,
      sourceFolderPath As String,
      targetFolderPath As String,
      eLogMsgTypeIfNotFound As clsLogTools.LogLevels,
      createStoragePathInfoOnly As Boolean,
      maxCopyAttempts As Integer) As Boolean


        Try

            Dim sourceFilePath = Path.Combine(sourceFolderPath, sourceFileName)

            If sourceFolderPath.StartsWith(MYEMSL_PATH_FLAG) Then
                Return m_MyEMSLUtilities.AddFileToDownloadQueue(sourceFilePath)
            End If

            Dim destFilePath = Path.Combine(targetFolderPath, sourceFileName)

            'Verify source file exists
            Const HOLDOFF_SECONDS = 1
            Const MAX_ATTEMPTS = 1
            If Not FileExistsWithRetry(sourceFilePath, HOLDOFF_SECONDS, eLogMsgTypeIfNotFound, MAX_ATTEMPTS) Then
                m_message = "File not found: " + sourceFilePath
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, eLogMsgTypeIfNotFound, m_message)
                If eLogMsgTypeIfNotFound = clsLogTools.LogLevels.ERROR Then
                    ReportStatus(m_message, 0, True)
                End If
                Return False
            End If

            If createStoragePathInfoOnly Then
                ' Create a storage path info file
                Return CreateStoragePathInfoFile(sourceFilePath, destFilePath)
            End If

            If CopyFileWithRetry(sourceFilePath, destFilePath, True, maxCopyAttempts) Then
                If m_DebugLevel > 3 Then
                    LogDebugMessage("CopyFileToWorkDir, File copied: " + sourceFilePath)
                End If
                Return True
            Else
                LogError("Error copying file " + sourceFilePath)
                Return False
            End If

        Catch ex As Exception
            LogError("Exception in CopyFileToWorkDir for " + Path.Combine(sourceFolderPath, sourceFileName))
        End Try

        Return False

    End Function

    ''' <summary>
    ''' Copies specified file from storage server to local working directory
    ''' </summary>
    ''' <param name="InpFile">Name of file to copy</param>
    ''' <param name="InpFolder">Path to folder where input file is located</param>
    ''' <param name="OutDir">Destination directory for file copy</param>
    ''' <returns>TRUE for success; FALSE for failure</returns>
    ''' <remarks></remarks>
    Protected Function CopyFileToWorkDirWithRename(
      InpFile As String,
      InpFolder As String,
      OutDir As String) As Boolean
        Const MaxCopyAttempts = 3
        Return CopyFileToWorkDirWithRename(InpFile, InpFolder, OutDir, clsLogTools.LogLevels.ERROR, createStoragePathInfoOnly:=False, MaxCopyAttempts:=MaxCopyAttempts)
    End Function

    ''' <summary>
    ''' Copies specified file from storage server to local working directory
    ''' </summary>
    ''' <param name="InpFile">Name of file to copy</param>
    ''' <param name="InpFolder">Path to folder where input file is located</param>
    ''' <param name="OutDir">Destination directory for file copy</param>
    ''' <param name="eLogMsgTypeIfNotFound">Type of message to log if the file is not found</param>
    ''' <returns>TRUE for success; FALSE for failure</returns>
    ''' <remarks></remarks>
    Protected Function CopyFileToWorkDirWithRename(
      InpFile As String,
      InpFolder As String,
      OutDir As String,
      eLogMsgTypeIfNotFound As clsLogTools.LogLevels) As Boolean
        Const MaxCopyAttempts = 3
        Return CopyFileToWorkDirWithRename(InpFile, InpFolder, OutDir, eLogMsgTypeIfNotFound, createStoragePathInfoOnly:=False, MaxCopyAttempts:=MaxCopyAttempts)
    End Function

    ''' <summary>
    ''' Copies specified file from storage server to local working directory
    ''' </summary>
    ''' <param name="InpFile">Name of file to copy</param>
    ''' <param name="InpFolder">Path to folder where input file is located</param>
    ''' <param name="OutDir">Destination directory for file copy</param>
    ''' <param name="eLogMsgTypeIfNotFound">Type of message to log if the file is not found</param>
    ''' ''' <param name="MaxCopyAttempts">Maximum number of attempts to make when errors are encountered while copying the file</param>
    ''' <returns>TRUE for success; FALSE for failure</returns>
    ''' <remarks></remarks>
    Protected Function CopyFileToWorkDirWithRename(
      InpFile As String,
      InpFolder As String,
      OutDir As String,
      eLogMsgTypeIfNotFound As clsLogTools.LogLevels,
      MaxCopyAttempts As Integer) As Boolean
        Return CopyFileToWorkDirWithRename(InpFile, InpFolder, OutDir, eLogMsgTypeIfNotFound, createStoragePathInfoOnly:=False, MaxCopyAttempts:=MaxCopyAttempts)
    End Function

    ''' <summary>
    ''' Copies specified file from storage server to local working directory, renames destination with dataset name
    ''' </summary>
    ''' <param name="InpFile">Name of file to copy</param>
    ''' <param name="InpFolder">Path to folder where input file is located</param>
    ''' <param name="OutDir">Destination directory for file copy</param>
    ''' <param name="eLogMsgTypeIfNotFound">Type of message to log if the file is not found</param>
    ''' <param name="CreateStoragePathInfoOnly">When true, then does not actually copy the specified file, and instead creates a file named FileName_StoragePathInfo.txt, and this file's first line will be the full path to the source file</param>
    ''' <param name="MaxCopyAttempts">Maximum number of attempts to make when errors are encountered while copying the file</param>
    ''' <returns>TRUE for success; FALSE for failure</returns>
    ''' <remarks></remarks>
    Protected Function CopyFileToWorkDirWithRename(
      InpFile As String,
      InpFolder As String,
      OutDir As String,
      eLogMsgTypeIfNotFound As clsLogTools.LogLevels,
      createStoragePathInfoOnly As Boolean,
      MaxCopyAttempts As Integer) As Boolean


        Dim SourceFile As String = String.Empty
        Dim DestFilePath As String

        Try
            SourceFile = Path.Combine(InpFolder, InpFile)

            'Verify source file exists
            If Not FileExistsWithRetry(SourceFile, eLogMsgTypeIfNotFound) Then
                m_message = "File not found: " + SourceFile
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, eLogMsgTypeIfNotFound, m_message)
                If eLogMsgTypeIfNotFound = clsLogTools.LogLevels.ERROR Then
                    ReportStatus(m_message, 0, True)
                End If
                Return False
            End If

            Dim Fi As New FileInfo(SourceFile)
            Dim TargetName As String = m_DatasetName + Fi.Extension
            DestFilePath = Path.Combine(OutDir, TargetName)

            If createStoragePathInfoOnly Then
                ' Create a storage path info file
                Return CreateStoragePathInfoFile(SourceFile, DestFilePath)
            End If

            If CopyFileWithRetry(SourceFile, DestFilePath, True, MaxCopyAttempts) Then
                If m_DebugLevel > 3 Then
                    LogDebugMessage("CopyFileToWorkDirWithRename, File copied: " + SourceFile)
                End If
                Return True
            Else
                LogError("Error copying file " + SourceFile)
                Return False
            End If

        Catch ex As Exception
            If SourceFile Is Nothing Then SourceFile = InpFile
            If SourceFile Is Nothing Then SourceFile = "??"

            LogError("Exception in CopyFileToWorkDirWithRename for " + SourceFile, ex)
        End Try

        Return False

    End Function

    ''' <summary>
    ''' Creates a Fasta file based on Ken's DLL
    ''' </summary>
    ''' <param name="DestFolder">Folder where file will be created</param>
    ''' <returns>TRUE for success; FALSE for failure</returns>
    ''' <remarks></remarks>
    Public Function CreateFastaFile(proteinCollectionInfo As clsProteinCollectionInfo, destFolder As String) As Boolean

        If m_DebugLevel >= 1 Then
            ReportStatus("Creating fasta file at " & destFolder)
        End If

        If Not Directory.Exists(destFolder) Then
            Directory.CreateDirectory(destFolder)
        End If

        ' Instantiate fasta tool if not already done
        If m_FastaTools Is Nothing Then
            If String.IsNullOrWhiteSpace(m_FastaToolsCnStr) Then
                m_message = "Protein database connection string not specified"
                ReportStatus("Error in CreateFastaFile: " + m_message, 0, True)
                Return False
            End If

        End If

        Dim retryCount = 1

        While retryCount > 0
            Try
                m_FastaTools = New Protein_Exporter.clsGetFASTAFromDMS(m_FastaToolsCnStr)
                Exit While
            Catch ex As Exception
                If retryCount > 1 Then
                    ReportStatus("Error instantiating clsGetFASTAFromDMS", ex)
                    ' Sleep 20 seconds after the first failure and 30 seconds after the second failure
                    If retryCount = 3 Then
                        Thread.Sleep(20000)
                    Else
                        Thread.Sleep(30000)
                    End If
                Else
                    m_message = "Error retrieving protein collection or legacy FASTA file: "
                    If ex.Message.Contains("could not open database connection") Then
                        m_message &= "could not open database connection"
                    Else
                        m_message &= ex.Message
                    End If
                    LogError(m_message, ex)
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Connection string: " & m_FastaToolsCnStr)
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Current user: " & Environment.UserName)
                    Return False
                End If
                retryCount -= 1
            End Try
        End While

        ' Initialize fasta generation state variables
        m_FastaFileName = String.Empty

        ' Set up variables for fasta creation call

        If Not proteinCollectionInfo.IsValid Then
            If String.IsNullOrWhiteSpace(proteinCollectionInfo.ErrorMessage) Then
                m_message = "Unknown error determining the Fasta file or protein collection to use; unable to obtain Fasta file"
            Else
                m_message = proteinCollectionInfo.ErrorMessage & "; unable to obtain Fasta file"
            End If

            ReportStatus("Error in CreateFastaFile: " + m_message, 0, True)
            Return False
        End If

        Dim stepToolName = m_jobParams.GetJobParameter("StepTool", "Unknown")

        Dim legacyFastaToUse As String
        Dim orgDBDescription = String.Copy(proteinCollectionInfo.OrgDBDescription)

        If proteinCollectionInfo.UsingSplitFasta AndAlso Not String.Equals(stepToolName, "DataExtractor", StringComparison.CurrentCultureIgnoreCase) Then

            If Not proteinCollectionInfo.UsingLegacyFasta Then
                LogError("Cannot use protein collections when running a SplitFasta job; choose a Legacy fasta file instead")
                Return False
            End If

            ' Running a SplitFasta job; need to update the name of the fasta file to be of the form FastaFileName_NNx_nn.fasta
            ' where NN is the number of total cloned steps and nn is this job's specific step number
            Dim numberOfClonedSteps As Integer

            legacyFastaToUse = GetSplitFastaFileName(m_jobParams, m_message, numberOfClonedSteps)

            If String.IsNullOrEmpty(legacyFastaToUse) Then
                ' The error should have already been logged
                Return False
            End If

            orgDBDescription = "Legacy DB: " + legacyFastaToUse

            ' Lookup connection strings
            ' Proteinseqs.Protein_Sequences
            Dim proteinSeqsDBConnectionString = m_mgrParams.GetParam("fastacnstring")
            If String.IsNullOrWhiteSpace(proteinSeqsDBConnectionString) Then
                LogError("Error in CreateFastaFile: manager parameter fastacnstring is not defined")
                Return False
            End If

            ' Gigasax.DMS5
            Dim dmsConnectionString = m_mgrParams.GetParam("connectionstring")
            If String.IsNullOrWhiteSpace(proteinSeqsDBConnectionString) Then
                LogError("Error in CreateFastaFile: manager parameter connectionstring is not defined")
                Return False
            End If

            ' Lookup the MSGFPlus Index Folder path
            Dim strMSGFPlusIndexFilesFolderPathLegacyDB = m_mgrParams.GetParam("MSGFPlusIndexFilesFolderPathLegacyDB", "\\Proto-7\MSGFPlus_Index_Files")
            If String.IsNullOrWhiteSpace(strMSGFPlusIndexFilesFolderPathLegacyDB) Then
                strMSGFPlusIndexFilesFolderPathLegacyDB = "\\Proto-7\MSGFPlus_Index_Files\Other"
            Else
                strMSGFPlusIndexFilesFolderPathLegacyDB = Path.Combine(strMSGFPlusIndexFilesFolderPathLegacyDB, "Other")
            End If

            If m_DebugLevel >= 1 Then
                ReportStatus("Verifying that split fasta file exists: " & legacyFastaToUse)
            End If

            ' Make sure the original fasta file has already been split into the appropriate number parts
            ' and that DMS knows about them
            '
            m_SplitFastaFileUtility = New clsSplitFastaFileUtilities(dmsConnectionString, proteinSeqsDBConnectionString, numberOfClonedSteps, m_MgrName)
            m_SplitFastaFileUtility.MSGFPlusIndexFilesFolderPathLegacyDB = strMSGFPlusIndexFilesFolderPathLegacyDB

            m_SplitFastaLastUpdateTime = Date.UtcNow
            m_SplitFastaLastPercentComplete = 0

            Dim success = m_SplitFastaFileUtility.ValidateSplitFastaFile(proteinCollectionInfo.LegacyFastaName, legacyFastaToUse)
            If Not success Then
                m_message = m_SplitFastaFileUtility.ErrorMessage
                Return False
            End If

        Else
            legacyFastaToUse = String.Copy(proteinCollectionInfo.LegacyFastaName)
        End If

        If m_DebugLevel >= 2 Then
            ReportStatus("ProteinCollectionList=" + proteinCollectionInfo.ProteinCollectionList + "; " +
                         "CreationOpts=" + proteinCollectionInfo.ProteinCollectionOptions + "; " +
                         "LegacyFasta=" + legacyFastaToUse)
        End If

        Try
            Dim hashString = m_FastaTools.ExportFASTAFile(proteinCollectionInfo.ProteinCollectionList, proteinCollectionInfo.ProteinCollectionOptions, legacyFastaToUse, destFolder)

            If String.IsNullOrEmpty(hashString) Then
                ' Fasta generator returned empty hash string
                LogError("m_FastaTools.ExportFASTAFile returned an empty Hash string for the OrgDB; unable to continue; " + orgDBDescription)
                Return False
            End If

        Catch ex As Exception
            If ex.Message.StartsWith("Legacy fasta file not found:") Then
                Dim rePathMatcher = New Regex("not found: (?<SourceFolder>.+)\\")
                Dim reMatch = rePathMatcher.Match(ex.Message)
                If reMatch.Success Then
                    m_message = "Legacy fasta file not found at " & reMatch.Groups("SourceFolder").Value
                Else
                    m_message = "Legacy fasta file not found in the organism folder for this job"
                End If
            Else
                m_message = "Exception generating OrgDb file"
            End If

            ReportStatus("Exception generating OrgDb file; " + orgDBDescription, ex)
            Return False
        End Try


        If String.IsNullOrEmpty(m_FastaFileName) Then
            ' Fasta generator never raised event FileGenerationCompleted
            LogError("m_FastaTools did not raise event FileGenerationCompleted; unable to continue; " + orgDBDescription)
            Return False
        End If

        Dim fiFastaFile As FileInfo
        Dim strFastaFileMsg As String
        fiFastaFile = New FileInfo(Path.Combine(destFolder, m_FastaFileName))

        If m_DebugLevel >= 1 Then
            ' Log the name of the .Fasta file we're using
            LogDebugMessage("Fasta generation complete, using database: " + m_FastaFileName, Nothing)

            If m_DebugLevel >= 2 Then
                ' Also log the file creation and modification dates
                Try

                    strFastaFileMsg = "Fasta file last modified: " +
                        GetHumanReadableTimeInterval(Date.UtcNow.Subtract(fiFastaFile.LastWriteTimeUtc)) + " ago at " + fiFastaFile.LastWriteTime.ToString()

                    strFastaFileMsg &= "; file created: " +
                        GetHumanReadableTimeInterval(Date.UtcNow.Subtract(fiFastaFile.CreationTimeUtc)) + " ago at " + fiFastaFile.CreationTime.ToString()

                    strFastaFileMsg &= "; file size: " + fiFastaFile.Length.ToString() + " bytes"

                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, strFastaFileMsg)
                Catch ex As Exception
                    ' Ignore errors here
                End Try
            End If

        End If

        ' Create/Update the .LastUsed file for the newly created Fasta File
        Dim lastUsedFilePath = fiFastaFile.FullName & ".LastUsed"
        Try
            Using swLastUsedFile = New StreamWriter(New FileStream(lastUsedFilePath, FileMode.Create, FileAccess.Write, FileShare.Read))
                swLastUsedFile.WriteLine(Date.UtcNow.ToString(clsAnalysisToolRunnerBase.DATE_TIME_FORMAT))
            End Using
        Catch ex As Exception
            ReportStatus("Warning: unable to create a new .LastUsed file at " & lastUsedFilePath & ": " & ex.Message)
        End Try

        ' If we got to here, everything worked OK
        Return True

    End Function

    ''' <summary>
    ''' Creates an XML formatted settings file based on data from broker
    ''' </summary>
    ''' <param name="FileText">String containing XML file contents</param>
    ''' <param name="FileNamePath">Name of file to create</param>
    ''' <returns>TRUE for success; FALSE for failure</returns>
    ''' <remarks>XML handling based on code provided by Matt Monroe</remarks>
    Private Function CreateSettingsFile(FileText As String, FileNamePath As String) As Boolean

        Dim objFormattedXMLWriter As New clsFormattedXMLWriter

        If Not objFormattedXMLWriter.WriteXMLToFile(FileText, FileNamePath) Then
            LogError("Error creating settings file " + FileNamePath + ": " + objFormattedXMLWriter.ErrMsg)
            m_message = "Error creating settings file"
            Return False
        Else
            Return True
        End If

    End Function

    ''' <summary>
    ''' Creates a file named DestFilePath but with "_StoragePathInfo.txt" appended to the name
    ''' The file's contents is the path given by SourceFilePath
    ''' </summary>
    ''' <param name="SourceFilePath">The path to write to the StoragePathInfo file</param>
    ''' <param name="DestFilePath">The path where the file would have been copied to</param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Protected Function CreateStoragePathInfoFile(SourceFilePath As String, DestFilePath As String) As Boolean

        Dim strInfoFilePath As String = String.Empty

        Try
            If SourceFilePath Is Nothing Or DestFilePath Is Nothing Then
                Return False
            End If

            strInfoFilePath = DestFilePath + STORAGE_PATH_INFO_FILE_SUFFIX

            Using swOutFile = New StreamWriter(New FileStream(strInfoFilePath, FileMode.Create, FileAccess.Write, FileShare.Read))
                swOutFile.WriteLine(SourceFilePath)
            End Using

        Catch ex As Exception
            LogError("Exception in CreateStoragePathInfoFile for " + strInfoFilePath, ex)
            Return False
        End Try

        Return True

    End Function

    ''' <summary>
    ''' Given two dates, returns the most recent date
    ''' </summary>
    ''' <param name="date1"></param>
    ''' <param name="date2"></param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Protected Shared Function DateMax(date1 As Date, date2 As Date) As Date
        If date1 > date2 Then
            Return date1
        Else
            Return date2
        End If
    End Function

    ''' <summary>
    ''' Tries to delete the first file whose path is defined in strFilesToDelete
    ''' If deletion succeeds, then removes the file from the queue
    ''' </summary>
    ''' <param name="strFilesToDelete">Queue of files to delete (full file paths)</param>
    ''' <param name="strFileToQueueForDeletion">Optional: new file to add to the queue; blank to do nothing</param>
    ''' <remarks></remarks>
    Protected Sub DeleteQueuedFiles(strFilesToDelete As Queue(Of String), strFileToQueueForDeletion As String)

        If strFilesToDelete.Count > 0 Then
            ' Call the garbage collector, then try to delete the first queued file
            ' Note, do not call WaitForPendingFinalizers since that could block this thread
            ' Thus, do not use PRISM.Processes.clsProgRunner.GarbageCollectNow
            GC.Collect()

            Try
                Dim strFileToDelete As String
                strFileToDelete = strFilesToDelete.Peek()

                File.Delete(strFileToDelete)

                ' If we get here, then the delete succeeded, so we can dequeue the file
                strFilesToDelete.Dequeue()

            Catch ex As Exception
                ' Exception deleting the file; ignore this error
            End Try

        End If

        If Not String.IsNullOrEmpty(strFileToQueueForDeletion) Then
            strFilesToDelete.Enqueue(strFileToQueueForDeletion)
        End If

    End Sub

    Protected Sub DisableMyEMSLSearch()
        m_MyEMSLUtilities.ClearDownloadQueue()
        MyEMSLSearchDisabled = True
    End Sub

    ''' <summary>
    ''' Test for file existence with a retry loop in case of temporary glitch
    ''' </summary>
    ''' <param name="fileName"></param>
    ''' <param name="eLogMsgTypeIfNotFound">Type of message to log if the file is not found</param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Private Function FileExistsWithRetry(fileName As String, eLogMsgTypeIfNotFound As clsLogTools.LogLevels) As Boolean

        Return FileExistsWithRetry(fileName, DEFAULT_FILE_EXISTS_RETRY_HOLDOFF_SECONDS, eLogMsgTypeIfNotFound)

    End Function

    ''' <summary>
    ''' Test for file existence with a retry loop in case of temporary glitch
    ''' </summary>
    ''' <param name="fileName"></param>
    ''' <param name="retryHoldoffSeconds">Number of seconds to wait between subsequent attempts to check for the file</param>
    ''' <param name="eLogMsgTypeIfNotFound">Type of message to log if the file is not found</param>
    ''' <returns>True if the file exists, otherwise false</returns>
    ''' <remarks></remarks>
    Private Function FileExistsWithRetry(fileName As String, retryHoldoffSeconds As Integer, eLogMsgTypeIfNotFound As clsLogTools.LogLevels) As Boolean

        Const MAX_ATTEMPTS = 3
        Return FileExistsWithRetry(fileName, retryHoldoffSeconds, eLogMsgTypeIfNotFound, MAX_ATTEMPTS)

    End Function

    ''' <summary>
    ''' Test for file existence with a retry loop in case of temporary glitch
    ''' </summary>
    ''' <param name="fileName"></param>
    ''' <param name="retryHoldoffSeconds">Number of seconds to wait between subsequent attempts to check for the file</param>
    ''' <param name="eLogMsgTypeIfNotFound">Type of message to log if the file is not found</param>
    ''' <param name="maxAttempts">Maximum number of attempts</param>
    ''' <returns>True if the file exists, otherwise false</returns>
    ''' <remarks></remarks>
    Private Function FileExistsWithRetry(
      fileName As String,
      retryHoldoffSeconds As Integer,
      eLogMsgTypeIfNotFound As clsLogTools.LogLevels,
      maxAttempts As Integer) As Boolean

        If maxAttempts < 1 Then maxAttempts = 1
        If maxAttempts > 10 Then maxAttempts = 10
        Dim retryCount = maxAttempts

        If retryHoldoffSeconds <= 0 Then retryHoldoffSeconds = DEFAULT_FILE_EXISTS_RETRY_HOLDOFF_SECONDS
        If retryHoldoffSeconds > 600 Then retryHoldoffSeconds = 600

        While retryCount > 0
            If File.Exists(fileName) Then
                Return True
            Else
                If eLogMsgTypeIfNotFound = clsLogTools.LogLevels.ERROR Then
                    ' Only log each failed attempt to find the file if eLogMsgTypeIfNotFound = ILogger.logMsgType.logError
                    ' Otherwise, we won't log each failed attempt
                    Dim errMsg As String = "File " + fileName + " not found. Retry count = " + retryCount.ToString
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, eLogMsgTypeIfNotFound, errMsg)
                    If eLogMsgTypeIfNotFound = clsLogTools.LogLevels.ERROR Then
                        ReportStatus(errMsg, 0, True)
                    End If
                End If
                retryCount -= 1
                If retryCount > 0 Then
                    Thread.Sleep(New TimeSpan(0, 0, retryHoldoffSeconds))     'Wait RetryHoldoffSeconds seconds before retrying
                End If
            End If
        End While

        ' If we got to here, there were too many failures
        If retryCount < 1 Then
            If maxAttempts = 1 Then
                m_message = "File not found: " & fileName
            Else
                m_message = "File not be found after " & maxAttempts & " tries: " & fileName
            End If

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, eLogMsgTypeIfNotFound, m_message)
            If eLogMsgTypeIfNotFound = clsLogTools.LogLevels.ERROR Then
                ReportStatus(m_message, 0, True)
            End If
            Return False
        End If

        Return False

    End Function

    ''' <summary>
    ''' Retrieves specified file from storage server, xfer folder, or archive and unzips if necessary
    ''' </summary>
    ''' <param name="fileName">Name of file to be retrieved</param>
    ''' <param name="unzip">TRUE if retrieved file should be unzipped after retrieval</param>
    ''' <returns>TRUE for success; FALSE for failure</returns>
    ''' <remarks>Logs an error if the file is not found</remarks>
    Protected Function FindAndRetrieveMiscFiles(fileName As String, unzip As Boolean) As Boolean
        Return FindAndRetrieveMiscFiles(fileName, unzip, searchArchivedDatasetFolder:=True)
    End Function

    ''' <summary>
    ''' Retrieves specified file from storage server, xfer folder, or archive and unzips if necessary
    ''' </summary>
    ''' <param name="fileName">Name of file to be retrieved</param>
    ''' <param name="unzip">TRUE if retrieved file should be unzipped after retrieval</param>
    ''' <param name="searchArchivedDatasetFolder">TRUE if the EMSL archive (Aurora) should also be searched</param>
    ''' <returns>TRUE for success; FALSE for failure</returns>
    ''' <remarks>Logs an error if the file is not found</remarks>
    Protected Function FindAndRetrieveMiscFiles(fileName As String, unzip As Boolean, searchArchivedDatasetFolder As Boolean) As Boolean
        Return FindAndRetrieveMiscFiles(fileName, unzip, searchArchivedDatasetFolder, "")
    End Function

    ''' <summary>
    ''' Retrieves specified file from storage server, xfer folder, or archive and unzips if necessary
    ''' </summary>
    ''' <param name="fileName">Name of file to be retrieved</param>
    ''' <param name="unzip">TRUE if retrieved file should be unzipped after retrieval</param>
    ''' <param name="searchArchivedDatasetFolder">TRUE if the EMSL archive (Aurora) should also be searched</param>
    ''' <returns>TRUE for success; FALSE for failure</returns>
    ''' <remarks></remarks>
    Protected Function FindAndRetrieveMiscFiles(fileName As String, unzip As Boolean, searchArchivedDatasetFolder As Boolean, logFileNotFound As Boolean) As Boolean
        Return FindAndRetrieveMiscFiles(fileName, unzip, searchArchivedDatasetFolder, "", logFileNotFound)
    End Function

    ''' <summary>
    ''' Retrieves specified file from storage server, xfer folder, or archive and unzips if necessary
    ''' </summary>
    ''' <param name="fileName">Name of file to be retrieved</param>
    ''' <param name="unzip">TRUE if retrieved file should be unzipped after retrieval</param>
    ''' <param name="searchArchivedDatasetFolder">TRUE if the EMSL archive (Aurora) should also be searched</param>
    ''' <param name="sourceFolderPath">Output parameter: the folder from which the file was copied</param>
    ''' <returns>TRUE for success; FALSE for failure</returns>
    ''' <remarks>Logs an error if the file is not found</remarks>
    Protected Function FindAndRetrieveMiscFiles(
      fileName As String,
      unzip As Boolean,
      searchArchivedDatasetFolder As Boolean,
      <Out()> ByRef sourceFolderPath As String) As Boolean

        Return FindAndRetrieveMiscFiles(fileName, unzip, searchArchivedDatasetFolder, sourceFolderPath, logFileNotFound:=True)
    End Function

    ''' <summary>
    ''' Retrieves specified file from storage server, xfer folder, or archive and unzips if necessary
    ''' </summary>
    ''' <param name="fileName">Name of file to be retrieved</param>
    ''' <param name="unzip">TRUE if retrieved file should be unzipped after retrieval</param>
    ''' <param name="searchArchivedDatasetFolder">TRUE if the EMSL archive (Aurora) should also be searched</param>
    ''' <param name="sourceFolderPath">Output parameter: the folder from which the file was copied</param>
    ''' <param name="logFileNotFound">True if an error should be logged when a file is not found</param>
    ''' <returns>TRUE for success; FALSE for failure</returns>
    ''' <remarks></remarks>
    Protected Function FindAndRetrieveMiscFiles(
      fileName As String,
      unzip As Boolean,
      searchArchivedDatasetFolder As Boolean,
      <Out()> ByRef sourceFolderPath As String,
      logFileNotFound As Boolean) As Boolean

        Const CreateStoragePathInfoFile = False

        ' Look for the file in the various folders
        ' A message will be logged if the file is not found
        sourceFolderPath = FindDataFile(fileName, searchArchivedDatasetFolder, logFileNotFound)

        ' Exit if file was not found
        If String.IsNullOrEmpty(sourceFolderPath) Then
            ' No folder found containing the specified file
            sourceFolderPath = String.Empty
            Return False
        End If

        If sourceFolderPath.StartsWith(MYEMSL_PATH_FLAG) Then
            Return m_MyEMSLUtilities.AddFileToDownloadQueue(sourceFolderPath)
        End If

        ' Copy the file
        If Not CopyFileToWorkDir(fileName, sourceFolderPath, m_WorkingDir, clsLogTools.LogLevels.ERROR, CreateStoragePathInfoFile) Then
            Return False
        End If

        ' Check whether unzipping was requested
        If Not unzip Then Return True

        ReportStatus("Unzipping file " + fileName)
        If UnzipFileStart(Path.Combine(m_WorkingDir, fileName), m_WorkingDir, "FindAndRetrieveMiscFiles", False) Then
            If m_DebugLevel >= 1 Then
                ReportStatus("Unzipped file " + fileName)
            End If
        End If

        Return True

    End Function

    ' ReSharper disable once UnusedMember.Global
    ''' <summary>
    ''' Search for the specified PHRP file and copy it to the work directory
    ''' If the filename contains _msgfplus and the file is not found, auto looks for the _msgfdb version of the file
    ''' </summary>
    ''' <param name="fileToGet">File to find; if the file is found with an alternative name, this variable is updated with the new name</param>
    ''' <param name="synopsisFileName">Synopsis file name, if known</param>
    ''' <param name="addToResultFileSkipList">If true, add the filename to the list of files to skip copying to the result folder</param>
    ''' <returns>True if success, false if not found</returns>
    ''' <remarks>Used by the IDPicker and MSGF plugins</remarks>    
    Protected Function FindAndRetrievePHRPDataFile(
      ByRef fileToGet As String,
      synopsisFileName As String,
      Optional addToResultFileSkipList As Boolean = True) As Boolean

        Dim blnSuccess = FindAndRetrieveMiscFiles(fileToGet, False)

        If Not blnSuccess AndAlso fileToGet.ToLower().Contains("msgfplus") Then
            If String.IsNullOrEmpty(synopsisFileName) Then synopsisFileName = "Dataset_msgfdb.txt"
            Dim alternativeName = clsPHRPReader.AutoSwitchToLegacyMSGFDBIfRequired(fileToGet, synopsisFileName)

            If Not String.Equals(alternativeName, fileToGet) Then
                blnSuccess = FindAndRetrieveMiscFiles(alternativeName, False)
                If blnSuccess Then
                    fileToGet = alternativeName
                End If
            End If
        End If

        If Not blnSuccess Then
            Return False
        End If

        If addToResultFileSkipList Then
            m_jobParams.AddResultFileToSkip(fileToGet)
        End If

        Return True

    End Function

    ''' <summary>
    ''' Finds the server or archive folder where specified file is located
    ''' </summary>
    ''' <param name="fileToFind">Name of the file to search for</param>
    ''' <returns>Path to the directory containing the file if the file was found; empty string if not found found</returns>
    ''' <remarks>If the file is found in MyEMSL, then the directory path returned will be of the form \\MyEMSL@MyEMSLID_84327</remarks>
    Protected Function FindDataFile(fileToFind As String) As String
        Return FindDataFile(fileToFind, searchArchivedDatasetFolder:=True)
    End Function

    ''' <summary>
    ''' Finds the server or archive folder where specified file is located
    ''' </summary>
    ''' <param name="FileToFind">Name of the file to search for</param>
    ''' <param name="searchArchivedDatasetFolder">TRUE if the EMSL archive (Aurora) should also be searched</param>
    ''' <returns>Path to the directory containing the file if the file was found; empty string if not found found</returns>
    ''' <remarks>If the file is found in MyEMSL, then the directory path returned will be of the form \\MyEMSL@MyEMSLID_84327</remarks>
    Protected Function FindDataFile(fileToFind As String, searchArchivedDatasetFolder As Boolean) As String
        Return FindDataFile(fileToFind, searchArchivedDatasetFolder, logFileNotFound:=True)
    End Function

    ''' <summary>
    ''' Finds the server or archive folder where specified file is located
    ''' </summary>
    ''' <param name="fileToFind">Name of the file to search for</param>
    ''' <param name="searchArchivedDatasetFolder">TRUE if the EMSL archive (Aurora) or MyEMSL should also be searched (m_AuroraAvailable and MyEMSLAvailable take precedence)</param>
    ''' <param name="logFileNotFound">True if an error should be logged when a file is not found</param>
    ''' <returns>Path to the directory containing the file if the file was found; empty string if not found found</returns>
    ''' <remarks>If the file is found in MyEMSL, then the directory path returned will be of the form \\MyEMSL@MyEMSLID_84327</remarks>
    Protected Function FindDataFile(fileToFind As String, searchArchivedDatasetFolder As Boolean, logFileNotFound As Boolean) As String

        Try
            ' Fill collection with possible folder locations
            ' The order of searching is:
            '  a. Check the "inputFolderName" and then each of the Shared Results Folders in the Transfer folder
            '  b. Check the "inputFolderName" and then each of the Shared Results Folders in the Dataset folder
            '  c. Check the "inputFolderName" and then each of the Shared Results Folders in MyEMSL for this dataset
            '  d. Check the "inputFolderName" and then each of the Shared Results Folders in the Archived dataset folder
            '
            ' Note that "SharedResultsFolders" will typically only contain one folder path, 
            '  but can contain a comma-separated list of folders

            Dim strDatasetFolderName = m_jobParams.GetParam("DatasetFolderName")
            Dim strInputFolderName = m_jobParams.GetParam("inputFolderName")

            Dim sharedResultFolderNames = GetSharedResultFolderList().ToList()

            Dim strParentFolderPaths = New List(Of String)
            strParentFolderPaths.Add(m_jobParams.GetParam("transferFolderPath"))
            strParentFolderPaths.Add(m_jobParams.GetParam("DatasetStoragePath"))

            If searchArchivedDatasetFolder Then
                If MyEMSLAvailable Then
                    strParentFolderPaths.Add(MYEMSL_PATH_FLAG)
                End If
                If m_AuroraAvailable Then
                    strParentFolderPaths.Add(m_jobParams.GetParam("DatasetArchivePath"))
                End If
            End If

            Dim foldersToSearch = New List(Of String)

            For Each strParentFolderPath As String In strParentFolderPaths

                If Not String.IsNullOrEmpty(strParentFolderPath) Then
                    If Not String.IsNullOrEmpty(strInputFolderName) Then
                        ' Parent Folder \ Dataset Folder \ Input folder
                        foldersToSearch.Add(FindDataFileAddFolder(strParentFolderPath, strDatasetFolderName, strInputFolderName))
                    End If

                    For Each strSharedFolderName As String In sharedResultFolderNames
                        ' Parent Folder \ Dataset Folder \  Shared results folder
                        foldersToSearch.Add(FindDataFileAddFolder(strParentFolderPath, strDatasetFolderName, strSharedFolderName))
                    Next

                    ' Parent Folder \ Dataset Folder
                    foldersToSearch.Add(FindDataFileAddFolder(strParentFolderPath, strDatasetFolderName, String.Empty))
                End If

            Next

            Dim matchingDirectory As String = String.Empty
            Dim matchFound = False

            ' Now search for FileToFind in each folder in FoldersToSearch
            For Each folderPath In foldersToSearch
                Try
                    Dim diFolderToCheck = New DirectoryInfo(folderPath)

                    If folderPath.StartsWith(MYEMSL_PATH_FLAG) Then

                        Dim matchingMyEMSLFiles = m_MyEMSLUtilities.FindFiles(fileToFind, diFolderToCheck.Name, m_DatasetName, recurse:=False)

                        If matchingMyEMSLFiles.Count > 0 Then
                            matchFound = True

                            ' Include the MyEMSL FileID in TempDir so that it is available for downloading
                            matchingDirectory = DatasetInfoBase.AppendMyEMSLFileID(folderPath, matchingMyEMSLFiles.First().FileID)
                            Exit For
                        End If

                    Else

                        If diFolderToCheck.Exists Then
                            If File.Exists(Path.Combine(folderPath, fileToFind)) Then
                                matchFound = True
                                matchingDirectory = folderPath
                                Exit For
                            End If
                        End If

                    End If

                Catch ex As Exception
                    ' Exception checking TempDir; log an error, but continue checking the other folders in FoldersToSearch
                    LogError("Exception in FindDataFile looking for: " + fileToFind + " in " + folderPath, ex)
                End Try
            Next

            If matchFound Then
                If m_DebugLevel >= 2 Then
                    LogDebugMessage("Data file found: " & fileToFind)
                End If
                Return matchingDirectory
            End If

            ' Data file not found
            ' Log this as an error if SearchArchivedDatasetFolder=True
            ' Log this as a warning if SearchArchivedDatasetFolder=False

            If logFileNotFound Then
                If searchArchivedDatasetFolder OrElse (Not m_AuroraAvailable And Not MyEMSLAvailable) Then
                    LogError("Data file not found: " + fileToFind)
                Else
                    ReportStatus("Warning: Data file not found (did not check archive): " + fileToFind)
                End If
            End If

            Return String.Empty

        Catch ex As Exception
            LogError("Exception in FindDataFile looking for: " + fileToFind, ex)
        End Try

        ' We'll only get here if an exception occurs
        Return String.Empty

    End Function

    Private Function FindDataFileAddFolder(
      strParentFolderPath As String,
      strDatasetFolderName As String,
      strInputFolderName As String) As String
        Dim strTargetFolderPath As String

        strTargetFolderPath = Path.Combine(strParentFolderPath, strDatasetFolderName)
        If Not String.IsNullOrEmpty(strInputFolderName) Then
            strTargetFolderPath = Path.Combine(strTargetFolderPath, strInputFolderName)
        End If

        Return strTargetFolderPath

    End Function

    ''' <summary>
    ''' Looks for file strFileName in strFolderPath or any of its subfolders
    ''' The filename may contain a wildcard character, in which case the first match will be returned
    ''' </summary>
    ''' <param name="strFolderPath">Folder path to examine</param>
    ''' <param name="strFileName">File name to find</param>
    ''' <returns>Full path to the file, if found; empty string if no match</returns>
    ''' <remarks></remarks>
    Public Shared Function FindFileInDirectoryTree(strFolderPath As String, strFileName As String) As String
        Return FindFileInDirectoryTree(strFolderPath, strFileName, New SortedSet(Of String))
    End Function

    ''' <summary>
    ''' Looks for file strFileName in strFolderPath or any of its subfolders
    ''' The filename may contain a wildcard character, in which case the first match will be returned
    ''' </summary>
    ''' <param name="strFolderPath">Folder path to examine</param>
    ''' <param name="strFileName">File name to find</param>
    ''' <param name="lstFolderNamesToSkip">List of folder names that should not be examined</param>
    ''' <returns>Full path to the file, if found; empty string if no match</returns>
    ''' <remarks></remarks>
    Public Shared Function FindFileInDirectoryTree(strFolderPath As String, strFileName As String, lstFolderNamesToSkip As SortedSet(Of String)) As String
        Dim diFolder As DirectoryInfo
        Dim fiFile As FileSystemInfo
        Dim ioSubFolder As FileSystemInfo

        Dim strFilePathMatch As String

        diFolder = New DirectoryInfo(strFolderPath)

        If diFolder.Exists Then
            ' Examine the files for this folder
            For Each fiFile In diFolder.GetFiles(strFileName)
                strFilePathMatch = fiFile.FullName
                Return strFilePathMatch
            Next

            ' Match not found
            ' Recursively call this function with the subdirectories in this folder

            For Each ioSubFolder In diFolder.GetDirectories
                If Not lstFolderNamesToSkip.Contains(ioSubFolder.Name) Then
                    strFilePathMatch = FindFileInDirectoryTree(ioSubFolder.FullName, strFileName)
                    If Not String.IsNullOrEmpty(strFilePathMatch) Then
                        Return strFilePathMatch
                    End If
                End If
            Next
        End If

        Return String.Empty

    End Function

    ''' <summary>
    ''' Determines the full path to the dataset file
    ''' Returns a folder path for data that is stored in folders (e.g. .D folders)
    ''' For instruments with multiple data folders, returns the path to the first folder
    ''' For instrument with multiple zipped data files, returns the dataset folder path
    ''' </summary>
    ''' <param name="blnIsFolder">Output variable: true if the path returned is a folder path; false if a file</param>
    ''' <param name="assumeUnpurged">
    ''' When true, assume that the instrument data exists on the storage server 
    ''' (and thus do not search MyEMSL or the archive for the file)
    ''' </param>
    ''' <returns>The full path to the dataset file or folder</returns>
    ''' <remarks>When assumeUnpurged is true, this function returns the expected path 
    ''' to the instrument data file (or folder) on the storage server, even if the file/folder wasn't actually found</remarks>
    Protected Function FindDatasetFileOrFolder(<Out()> ByRef blnIsFolder As Boolean, Optional assumeUnpurged As Boolean = False) As String
        Return FindDatasetFileOrFolder(DEFAULT_MAX_RETRY_COUNT, blnIsFolder, assumeUnpurged:=assumeUnpurged)
    End Function

    ''' <summary>
    ''' Determines the full path to the dataset file
    ''' Returns a folder path for data that is stored in folders (e.g. .D folders)
    ''' For instruments with multiple data folders, returns the path to the first folder
    ''' For instrument with multiple zipped data files, returns the dataset folder path
    ''' </summary>
    ''' <param name="maxAttempts">Maximum number of attempts to look for the folder</param>
    ''' <param name="blnIsFolder">Output variable: true if the path returned is a folder path; false if a file</param>
    ''' <param name="assumeUnpurged">
    ''' When true, assume that the instrument data exists on the storage server 
    ''' (and thus do not search MyEMSL or the archive for the file)
    ''' </param>
    ''' <returns>The full path to the dataset file or folder</returns>
    ''' <remarks>When assumeUnpurged is true, this function returns the expected path 
    ''' to the instrument data file (or folder) on the storage server, even if the file/folder wasn't actually found</remarks>
    Protected Function FindDatasetFileOrFolder(
      maxAttempts As Integer,
      <Out()> ByRef blnIsFolder As Boolean,
      Optional assumeUnpurged As Boolean = False) As String

        Dim RawDataType As String = m_jobParams.GetParam("RawDataType")
        Dim StoragePath As String = m_jobParams.GetParam("DatasetStoragePath")
        Dim eRawDataType As eRawDataTypeConstants
        Dim strFileOrFolderPath As String = String.Empty

        blnIsFolder = False

        eRawDataType = GetRawDataType(RawDataType)
        Select Case eRawDataType
            Case eRawDataTypeConstants.AgilentDFolder           'Agilent ion trap data

                If StoragePath.ToLower().Contains("Agilent_SL1".ToLower()) OrElse
                   StoragePath.ToLower().Contains("Agilent_XCT1".ToLower()) Then
                    ' For Agilent Ion Trap datasets acquired on Agilent_SL1 or Agilent_XCT1 in 2005, 
                    '  we would pre-process the data beforehand to create MGF files
                    ' The following call can be used to retrieve the files
                    strFileOrFolderPath = FindMGFFile(maxAttempts, assumeUnpurged)
                Else
                    ' DeconTools_V2 now supports reading the .D files directly
                    ' Call RetrieveDotDFolder() to copy the folder and all subfolders
                    strFileOrFolderPath = FindDotDFolder(assumeUnpurged)
                    blnIsFolder = True
                End If

            Case eRawDataTypeConstants.AgilentQStarWiffFile         'Agilent/QSTAR TOF data
                strFileOrFolderPath = FindDatasetFile(maxAttempts, DOT_WIFF_EXTENSION, assumeUnpurged)

            Case eRawDataTypeConstants.ZippedSFolders           'FTICR data
                strFileOrFolderPath = FindSFolders(assumeUnpurged)
                blnIsFolder = True

            Case eRawDataTypeConstants.ThermoRawFile            'Finnigan ion trap/LTQ-FT data
                strFileOrFolderPath = FindDatasetFile(maxAttempts, DOT_RAW_EXTENSION, assumeUnpurged)

            Case eRawDataTypeConstants.MicromassRawFolder           'Micromass QTOF data
                strFileOrFolderPath = FindDotRawFolder(assumeUnpurged)
                blnIsFolder = True

            Case eRawDataTypeConstants.UIMF         'IMS UIMF data
                strFileOrFolderPath = FindDatasetFile(maxAttempts, DOT_UIMF_EXTENSION, assumeUnpurged)

            Case eRawDataTypeConstants.mzXML
                strFileOrFolderPath = FindDatasetFile(maxAttempts, DOT_MZXML_EXTENSION, assumeUnpurged)

            Case eRawDataTypeConstants.mzML
                strFileOrFolderPath = FindDatasetFile(maxAttempts, DOT_MZML_EXTENSION, assumeUnpurged)

            Case eRawDataTypeConstants.BrukerFTFolder, eRawDataTypeConstants.BrukerTOFBaf
                ' Call RetrieveDotDFolder() to copy the folder and all subfolders

                ' Both the MSXml step tool and DeconTools require the .Baf file
                ' We previously didn't need this file for DeconTools, but, now that DeconTools is using CompassXtract, so we need the file

                strFileOrFolderPath = FindDotDFolder(assumeUnpurged)
                blnIsFolder = True

            Case eRawDataTypeConstants.BrukerMALDIImaging
                strFileOrFolderPath = FindBrukerMALDIImagingFolders(assumeUnpurged)
                blnIsFolder = True

        End Select

        Return strFileOrFolderPath

    End Function

    ''' <summary>
    ''' Finds the dataset folder containing Bruker Maldi imaging .zip files
    ''' </summary>
    ''' <returns>The full path to the dataset folder</returns>
    ''' <remarks></remarks>
    Public Function FindBrukerMALDIImagingFolders(Optional assumeUnpurged As Boolean = False) As String

        Const ZIPPED_BRUKER_IMAGING_SECTIONS_FILE_MASK = "*R*X*.zip"

        ' Look for the dataset folder; it must contain .Zip files with names like 0_R00X442.zip
        ' If a matching folder isn't found, then ServerPath will contain the folder path defined by Job Param "DatasetStoragePath"

        Dim DSFolderPath As String
        DSFolderPath = FindValidFolder(
            m_DatasetName,
            ZIPPED_BRUKER_IMAGING_SECTIONS_FILE_MASK,
            RetrievingInstrumentDataFolder:=True,
            assumeUnpurged:=assumeUnpurged)

        If String.IsNullOrEmpty(DSFolderPath) Then Return String.Empty

        Return DSFolderPath

    End Function

    ''' <summary>
    ''' Finds a file named DatasetName.FileExtension
    ''' </summary>
    ''' <param name="FileExtension"></param>
    ''' <returns>The full path to the folder; an empty string if no match</returns>
    ''' <remarks></remarks>
    Protected Function FindDatasetFile(FileExtension As String) As String
        Return FindDatasetFile(DEFAULT_MAX_RETRY_COUNT, FileExtension)
    End Function

    ''' <summary>
    ''' Finds a file named DatasetName.FileExtension
    ''' </summary>
    ''' <param name="maxAttempts">Maximum number of attempts to look for the folder</param>
    ''' <param name="FileExtension"></param>
    ''' <returns>The full path to the folder; an empty string if no match</returns>
    ''' <remarks></remarks>
    Protected Function FindDatasetFile(
     maxAttempts As Integer,
     fileExtension As String,
     Optional assumeUnpurged As Boolean = False) As String

        If Not fileExtension.StartsWith(".") Then
            fileExtension = "." + fileExtension
        End If

        Dim DataFileName As String = m_DatasetName + fileExtension
        Dim validFolderFound As Boolean

        Dim DSFolderPath As String = FindValidFolder(
          m_DatasetName,
          DataFileName,
          folderNameToFind:="",
          maxAttempts:=maxAttempts,
          logFolderNotFound:=True,
          retrievingInstrumentDataFolder:=False,
          validFolderFound:=validFolderFound,
          assumeUnpurged:=assumeUnpurged)

        If Not String.IsNullOrEmpty(DSFolderPath) Then
            Return Path.Combine(DSFolderPath, DataFileName)
        Else
            Return String.Empty
        End If

    End Function

    ''' <summary>
    ''' Finds a .Raw folder below the dataset folder
    ''' </summary>
    ''' <returns>The full path to the folder; an empty string if no match</returns>
    ''' <remarks></remarks>
    Protected Function FindDotDFolder(Optional assumeUnpurged As Boolean = False) As String
        Return FindDotXFolder(DOT_D_EXTENSION, assumeUnpurged)
    End Function

    ''' <summary>
    ''' Finds a .D folder below the dataset folder
    ''' </summary>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Protected Function FindDotRawFolder(Optional assumeUnpurged As Boolean = False) As String
        Return FindDotXFolder(DOT_RAW_EXTENSION, assumeUnpurged)
    End Function

    ''' <summary>
    ''' Finds a subfolder (typically Dataset.D or Dataset.Raw) below the dataset folder
    ''' </summary>
    ''' <param name="FolderExtension"></param>
    ''' <returns>The full path to the folder; an empty string if no match</returns>
    ''' <remarks></remarks>
    Protected Function FindDotXFolder(folderExtension As String, assumeUnpurged As Boolean) As String

        If Not folderExtension.StartsWith(".") Then
            folderExtension = "." + folderExtension
        End If

        Dim validFolderFound As Boolean

        Dim FileNameToFind As String = String.Empty
        Dim FolderExtensionWildcard As String = "*" + folderExtension

        Dim ServerPath As String = FindValidFolder(
            m_DatasetName,
            FileNameToFind,
            FolderExtensionWildcard,
            DEFAULT_MAX_RETRY_COUNT,
            logFolderNotFound:=True,
            retrievingInstrumentDataFolder:=True,
            validFolderFound:=validFolderFound,
            assumeUnpurged:=assumeUnpurged)

        If (ServerPath.StartsWith(MYEMSL_PATH_FLAG)) Then
            Return ServerPath
        End If

        Dim diDatasetFolder = New DirectoryInfo(ServerPath)

        'Find the instrument data folder (e.g. Dataset.D or Dataset.Raw) in the dataset folder
        For Each diSubFolder As DirectoryInfo In diDatasetFolder.GetDirectories(FolderExtensionWildcard)
            Return diSubFolder.FullName
        Next

        ' No match found
        Return String.Empty

    End Function

    ''' <summary>
    ''' Finds the dataset folder containing either a 0.ser subfolder or containing zipped S-folders
    ''' </summary>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Protected Function FindSFolders(Optional assumeUnpurged As Boolean = False) As String

        ' First Check for the existence of a 0.ser Folder
        Dim FileNameToFind As String = String.Empty
        Dim validFolderFound As Boolean

        Dim DSFolderPath As String = FindValidFolder(
            m_DatasetName,
            FileNameToFind,
            BRUKER_ZERO_SER_FOLDER,
            DEFAULT_MAX_RETRY_COUNT,
            logFolderNotFound:=True,
            retrievingInstrumentDataFolder:=True,
            validFolderFound:=validFolderFound,
            assumeUnpurged:=assumeUnpurged)

        If Not String.IsNullOrEmpty(DSFolderPath) Then
            Return Path.Combine(DSFolderPath, BRUKER_ZERO_SER_FOLDER)
        End If

        ' The 0.ser folder does not exist; look for zipped s-folders
        DSFolderPath = FindValidFolder(m_DatasetName, "s*.zip", RetrievingInstrumentDataFolder:=True, assumeUnpurged:=assumeUnpurged)

        Return DSFolderPath

    End Function

    ''' <summary>
    ''' Finds the best .mgf file for the current dataset
    ''' </summary>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Protected Function FindMGFFile(maxAttempts As Integer, assumeUnpurged As Boolean) As String

        ' Data files are in a subfolder off of the main dataset folder
        ' Files are renamed with dataset name because MASIC requires this. Other analysis types don't care

        Dim validFolderFound As Boolean
        Dim serverPath As String = FindValidFolder(
            m_DatasetName,
            "",
            "*" + DOT_D_EXTENSION,
            maxAttempts,
            logFolderNotFound:=True,
            retrievingInstrumentDataFolder:=False,
            validFolderFound:=validFolderFound,
            assumeUnpurged:=assumeUnpurged)

        Dim diServerFolder = New DirectoryInfo(serverPath)

        ' Get a list of the subfolders in the dataset folder		
        ' Go through the folders looking for a file with a ".mgf" extension
        For Each diSubFolder As DirectoryInfo In diServerFolder.GetDirectories()

            For Each fiFile As FileInfo In diSubFolder.GetFiles("*" + DOT_MGF_EXTENSION)
                ' Return the first .mgf file that was found
                Return fiFile.FullName
            Next
        Next

        ' No match was found
        Return String.Empty

    End Function

    ''' <summary>
    ''' Looks for the .mzXML file for this dataset
    ''' </summary>
    ''' <param name="strHashcheckFilePath">Output parameter: path to the hashcheck file if the .mzXML file was found in the MSXml cache</param>
    ''' <returns>Full path to the file, if found; empty string if no match</returns>
    ''' <remarks>Supports both gzipped mzXML files and unzipped ones (gzipping was enabled in September 2014)</remarks>
    Protected Function FindMZXmlFile(<Out()> ByRef strHashcheckFilePath As String) As String

        ' First look in the MsXML cache folder
        Dim strMatchingFilePath = FindMsXmlFileInCache(MSXMLOutputTypeConstants.mzXML, strHashcheckFilePath)

        If Not String.IsNullOrEmpty(strMatchingFilePath) Then
            Return strMatchingFilePath
        End If

        ' Not found in the cache; look in the dataset folder

        Dim DatasetID As String = m_jobParams.GetParam("JobParameters", "DatasetID")

        Const MSXmlFoldernameBase = "MSXML_Gen_1_"
        Dim MzXMLFilename As String = m_DatasetName + ".mzXML"

        Const MAX_ATTEMPTS = 1

        Dim lstValuesToCheck As List(Of Integer)
        lstValuesToCheck = New List(Of Integer)

        ' Initialize the values we'll look for
        ' Note that these values are added to the list in the order of the preferred file to retrieve
        lstValuesToCheck.Add(154)           ' MSXML_Gen_1_154_DatasetID,   CentroidMSXML=True;  MSXMLGenerator=MSConvert.exe; CentroidPeakCountToRetain=250; MSXMLOutputType=mzXML;
        lstValuesToCheck.Add(132)           ' MSXML_Gen_1_132_DatasetID,   CentroidMSXML=True;  MSXMLGenerator=MSConvert.exe; CentroidPeakCountToRetain=150; MSXMLOutputType=mzXML;
        lstValuesToCheck.Add(93)            ' MSXML_Gen_1_93_DatasetID,    CentroidMSXML=True;  MSXMLGenerator=ReadW.exe;     MSXMLOutputType=mzXML;
        lstValuesToCheck.Add(126)           ' MSXML_Gen_1_126_DatasetID,   CentroidMSXML=True;  MSXMLGenerator=ReadW.exe;     MSXMLOutputType=mzXML; ReAdW_Version=v2.1;
        lstValuesToCheck.Add(39)            ' MSXML_Gen_1_39_DatasetID,    CentroidMSXML=False; MSXMLGenerator=ReadW.exe;     MSXMLOutputType=mzXML;

        strHashcheckFilePath = String.Empty

        For Each intVersion As Integer In lstValuesToCheck

            Dim MSXmlFoldername As String
            MSXmlFoldername = MSXmlFoldernameBase + intVersion.ToString() + "_" + DatasetID

            ' Look for the MSXmlFolder
            ' If the folder cannot be found, then FindValidFolder will return the folder defined by "DatasetStoragePath"
            Dim ServerPath As String = FindValidFolder(m_DatasetName, "", MSXmlFoldername, MAX_ATTEMPTS, False, retrievingInstrumentDataFolder:=False)

            If String.IsNullOrEmpty(ServerPath) Then
                Continue For
            End If

            If ServerPath.StartsWith(MYEMSL_PATH_FLAG) Then
                ' File found in MyEMSL
                ' Determine the MyEMSL FileID by searching for the expected file in m_MyEMSLUtilities.RecentlyFoundMyEMSLFiles

                Dim myEmslFileID As Int64 = 0

                For Each udtArchivedFile In m_MyEMSLUtilities.RecentlyFoundMyEMSLFiles
                    Dim fiArchivedFile As New FileInfo(udtArchivedFile.FileInfo.RelativePathWindows)
                    If clsGlobal.IsMatch(fiArchivedFile.Name, MzXMLFilename) Then
                        myEmslFileID = udtArchivedFile.FileID
                        Exit For
                    End If
                Next

                If myEmslFileID > 0 Then
                    Return Path.Combine(ServerPath, MSXmlFoldername, DatasetInfoBase.AppendMyEMSLFileID(MzXMLFilename, myEmslFileID))
                End If
            Else

                ' Due to quirks with how FindValidFolder behaves, we need to confirm that the mzXML file actually exists
                Dim diFolderInfo As DirectoryInfo
                diFolderInfo = New DirectoryInfo(ServerPath)

                If diFolderInfo.Exists Then

                    'See if the ServerPath folder actually contains a subfolder named MSXmlFoldername
                    Dim diSubfolders() As DirectoryInfo = diFolderInfo.GetDirectories(MSXmlFoldername)
                    If diSubfolders.Length > 0 Then

                        ' MSXmlFolder found; return the path to the file     
                        Return Path.Combine(diSubfolders(0).FullName, MzXMLFilename)

                    End If

                End If

            End If

        Next

        ' If we get here, then no match was found
        Return String.Empty

    End Function

    ''' <summary>
    ''' Looks for the mzXML or mzML file for this dataset
    ''' </summary>
    ''' <param name="msXmlType">File type to find (mzXML or mzML)</param>
    ''' <param name="strHashcheckFilePath">Output parameter: path to the hashcheck file if the .mzXML file was found in the MSXml cache</param>
    ''' <returns>Full path to the file if a match; empty string if no match</returns>
    ''' <remarks>Supports gzipped .mzML files and supports both gzipped .mzXML files and unzipped ones (gzipping was enabled in September 2014)</remarks>
    Protected Function FindMsXmlFileInCache(msXmlType As MSXMLOutputTypeConstants, <Out()> ByRef strHashcheckFilePath As String) As String

        Dim MsXMLFilename As String = m_DatasetName
        strHashcheckFilePath = String.Empty

        Select Case msXmlType
            Case MSXMLOutputTypeConstants.mzXML
                MsXMLFilename &= DOT_MZXML_EXTENSION & DOT_GZ_EXTENSION
            Case MSXMLOutputTypeConstants.mzML
                ' All MzML files should be gzipped
                MsXMLFilename &= DOT_MZML_EXTENSION & DOT_GZ_EXTENSION
            Case Else
                Throw New ArgumentOutOfRangeException(NameOf(msXmlType), "Unsupported enum value for MSXMLOutputTypeConstants: " & msXmlType)
        End Select

        ' Lookup the MSXML cache path (typically \\Proto-11\MSXML_Cache )
        Dim strMSXMLCacheFolderPath As String = m_mgrParams.GetParam("MSXMLCacheFolderPath", String.Empty)
        Dim diCacheFolder = New DirectoryInfo(strMSXMLCacheFolderPath)

        If Not diCacheFolder.Exists Then
            ReportStatus("Warning: Cache folder not found: " & strMSXMLCacheFolderPath)
            Return String.Empty
        End If

        ' Determine the YearQuarter code for this dataset
        Dim strDatasetStoragePath As String = m_jobParams.GetParam("JobParameters", "DatasetStoragePath")
        If String.IsNullOrEmpty(strDatasetStoragePath) AndAlso (m_AuroraAvailable OrElse MyEMSLAvailable) Then
            strDatasetStoragePath = m_jobParams.GetParam("JobParameters", "DatasetArchivePath")
        End If

        Dim strYearQuarter As String = GetDatasetYearQuarter(strDatasetStoragePath)

        Dim lstMatchingFiles = New List(Of FileInfo)

        If String.IsNullOrEmpty(strYearQuarter) Then

            ' Perform an exhaustive recursive search of the MSXML file cache
            Dim lstFilesToAppend = diCacheFolder.GetFiles(MsXMLFilename, SearchOption.AllDirectories)

            If lstFilesToAppend.Count = 0 AndAlso msXmlType = MSXMLOutputTypeConstants.mzXML Then
                ' Older .mzXML files were not gzipped
                lstFilesToAppend = diCacheFolder.GetFiles(m_DatasetName & DOT_MZXML_EXTENSION, SearchOption.AllDirectories)
            End If

            Dim query = (From item In lstFilesToAppend Select item Order By item.LastWriteTimeUtc Descending).Take(1)

            lstMatchingFiles.AddRange(query)

        Else

            ' Look for the file in the top level subfolders of the MSXML file cache
            For Each diToolFolder In diCacheFolder.GetDirectories()
                Dim lstSubFolders = diToolFolder.GetDirectories(strYearQuarter)

                If lstSubFolders.Count > 0 Then

                    Dim lstFilesToAppend = lstSubFolders.First.GetFiles(MsXMLFilename, SearchOption.TopDirectoryOnly)
                    If lstFilesToAppend.Count = 0 AndAlso msXmlType = MSXMLOutputTypeConstants.mzXML Then
                        ' Older .mzXML files were not gzipped
                        lstFilesToAppend = lstSubFolders.First.GetFiles(m_DatasetName & DOT_MZXML_EXTENSION, SearchOption.TopDirectoryOnly)
                    End If

                    Dim query = (From item In lstFilesToAppend Select item Order By item.LastWriteTimeUtc Descending).Take(1)
                    lstMatchingFiles.AddRange(query)

                End If

            Next

        End If

        If lstMatchingFiles.Count = 0 Then
            Return String.Empty
        End If

        ' One or more matches were found; select the newest one 
        Dim sortQuery = (From item In lstMatchingFiles Select item Order By item.LastWriteTimeUtc Descending).Take(1)
        Dim matchedFilePath = sortQuery.First().FullName

        ' Confirm that the file has a .hashcheck file and that the information in the .hashcheck file matches the file
        Dim errorMessage As String = String.Empty
        strHashcheckFilePath = matchedFilePath & clsGlobal.SERVER_CACHE_HASHCHECK_FILE_SUFFIX

        If clsGlobal.ValidateFileVsHashcheck(matchedFilePath, strHashcheckFilePath, errorMessage) Then
            Return matchedFilePath
        Else
            ReportStatus("Warning: " & errorMessage)
            Return String.Empty
        End If

    End Function

    ''' <summary>
    ''' Determines the most appropriate folder to use to obtain dataset files from
    ''' Optionally, can require that a certain file also be present in the folder for it to be deemed valid
    ''' If no folder is deemed valid, then returns the path defined by "DatasetStoragePath"
    ''' </summary>
    ''' <param name="DSName">Name of the dataset</param>
    ''' <param name="FileNameToFind">Name of a file that must exist in the folder; can contain a wildcard, e.g. *.zip</param>
    ''' <returns>Path to the most appropriate dataset folder</returns>
    ''' <remarks>Although FileNameToFind could be empty, you are highly encouraged to filter by either Filename or by FolderName when using FindValidFolder</remarks>
    Protected Function FindValidFolder(DSName As String, FileNameToFind As String) As String

        Return FindValidFolder(DSName, FileNameToFind, "", DEFAULT_MAX_RETRY_COUNT, logFolderNotFound:=True, retrievingInstrumentDataFolder:=False)

    End Function

    ''' <summary>
    ''' Determines the most appropriate folder to use to obtain dataset files from
    ''' Optionally, can require that a certain file also be present in the folder for it to be deemed valid
    ''' If no folder is deemed valid, then returns the path defined by "DatasetStoragePath"
    ''' </summary>
    ''' <param name="DSName">Name of the dataset</param>
    ''' <param name="FileNameToFind">Name of a file that must exist in the folder; can contain a wildcard, e.g. *.zip</param>
    ''' <param name="RetrievingInstrumentDataFolder">Set to True when retrieving an instrument data folder</param>
    ''' <returns>Path to the most appropriate dataset folder</returns>
    ''' <remarks>Although FileNameToFind could be empty, you are highly encouraged to filter by either Filename or by FolderName when using FindValidFolder</remarks>
    Protected Function FindValidFolder(
      DSName As String,
      FileNameToFind As String,
      RetrievingInstrumentDataFolder As Boolean) As String

        Const folderNameToFind = ""
        Return FindValidFolder(DSName, FileNameToFind, folderNameToFind, DEFAULT_MAX_RETRY_COUNT, logFolderNotFound:=True, retrievingInstrumentDataFolder:=RetrievingInstrumentDataFolder)

    End Function


    ''' <summary>
    ''' Determines the most appropriate folder to use to obtain dataset files from
    ''' Optionally, can require that a certain file also be present in the folder for it to be deemed valid
    ''' If no folder is deemed valid, then returns the path defined by "DatasetStoragePath"
    ''' </summary>
    ''' <param name="DSName">Name of the dataset</param>
    ''' <param name="FileNameToFind">Name of a file that must exist in the folder; can contain a wildcard, e.g. *.zip</param>
    ''' <param name="RetrievingInstrumentDataFolder">Set to True when retrieving an instrument data folder</param>
    ''' <returns>Path to the most appropriate dataset folder</returns>
    ''' <remarks>Although FileNameToFind could be empty, you are highly encouraged to filter by either Filename or by FolderName when using FindValidFolder</remarks>
    Protected Function FindValidFolder(
      DSName As String,
      FileNameToFind As String,
      RetrievingInstrumentDataFolder As Boolean,
      assumeUnpurged As Boolean) As String

        Const folderNameToFind = ""
        Dim validFolderFound As Boolean
        Return FindValidFolder(DSName, FileNameToFind, folderNameToFind, DEFAULT_MAX_RETRY_COUNT, logFolderNotFound:=True, retrievingInstrumentDataFolder:=RetrievingInstrumentDataFolder, validFolderFound:=validFolderFound, assumeUnpurged:=assumeUnpurged)

    End Function

    ''' <summary>
    ''' Determines the most appropriate folder to use to obtain dataset files from
    ''' Optionally, can require that a certain file also be present in the folder for it to be deemed valid
    ''' If no folder is deemed valid, then returns the path defined by "DatasetStoragePath"
    ''' </summary>
    ''' <param name="DSName">Name of the dataset</param>
    ''' <param name="FileNameToFind">Name of a file that must exist in the folder; can contain a wildcard, e.g. *.zip</param>
    ''' <param name="FolderNameToFind">Optional: Name of a folder that must exist in the dataset folder; can contain a wildcard, e.g. SEQ*</param>
    ''' <returns>Path to the most appropriate dataset folder</returns>
    ''' <remarks>Although FileNameToFind and FolderNameToFind could both be empty, you are highly encouraged to filter by either Filename or by FolderName when using FindValidFolder</remarks>
    Protected Function FindValidFolder(
      DSName As String,
      FileNameToFind As String,
      FolderNameToFind As String) As String

        Return FindValidFolder(DSName, FileNameToFind, FolderNameToFind, DEFAULT_MAX_RETRY_COUNT, logFolderNotFound:=True, retrievingInstrumentDataFolder:=False)

    End Function

    ''' <summary>
    ''' Determines the most appropriate folder to use to obtain dataset files from
    ''' Optionally, can require that a certain file also be present in the folder for it to be deemed valid
    ''' If no folder is deemed valid, then returns the path defined by "DatasetStoragePath"
    ''' </summary>
    ''' <param name="DSName">Name of the dataset</param>
    ''' <param name="FileNameToFind">Name of a file that must exist in the folder; can contain a wildcard, e.g. *.zip</param>
    ''' <param name="FolderNameToFind">Optional: Name of a folder that must exist in the dataset folder; can contain a wildcard, e.g. SEQ*</param>
    ''' <param name="RetrievingInstrumentDataFolder">Set to True when retrieving an instrument data folder</param>
    ''' <returns>Path to the most appropriate dataset folder</returns>
    ''' <remarks>Although FileNameToFind and FolderNameToFind could both be empty, you are highly encouraged to filter by either Filename or by FolderName when using FindValidFolder</remarks>
    Protected Function FindValidFolder(
      DSName As String,
      FileNameToFind As String,
      FolderNameToFind As String,
      RetrievingInstrumentDataFolder As Boolean) As String

        Return FindValidFolder(DSName, FileNameToFind, FolderNameToFind, DEFAULT_MAX_RETRY_COUNT, logFolderNotFound:=True, retrievingInstrumentDataFolder:=RetrievingInstrumentDataFolder)

    End Function

    ''' <summary>
    ''' Determines the most appropriate folder to use to obtain dataset files from
    ''' Optionally, can require that a certain file also be present in the folder for it to be deemed valid
    ''' If no folder is deemed valid, then returns the path defined by "DatasetStoragePath"
    ''' </summary>
    ''' <param name="DSName">Name of the dataset</param>
    ''' <param name="FileNameToFind">Name of a file that must exist in the folder; can contain a wildcard, e.g. *.zip</param>
    ''' <param name="FolderNameToFind">Optional: Name of a folder that must exist in the dataset folder; can contain a wildcard, e.g. SEQ*</param>
    ''' <param name="MaxRetryCount">Maximum number of attempts</param>
    ''' <returns>Path to the most appropriate dataset folder</returns>
    ''' <remarks>Although FileNameToFind and FolderNameToFind could both be empty, you are highly encouraged to filter by either Filename or by FolderName when using FindValidFolder</remarks>
    Protected Function FindValidFolder(
      DSName As String,
      FileNameToFind As String,
      FolderNameToFind As String,
      MaxRetryCount As Integer) As String

        Return FindValidFolder(DSName, FileNameToFind, FolderNameToFind, MaxRetryCount, logFolderNotFound:=True, retrievingInstrumentDataFolder:=False)

    End Function

    ''' <summary>
    ''' Determines the most appropriate folder to use to obtain dataset files from
    ''' Optionally, can require that a certain file also be present in the folder for it to be deemed valid
    ''' If no folder is deemed valid, then returns the path defined by Job Param "DatasetStoragePath"
    ''' </summary>
    ''' <param name="DSName">Name of the dataset</param>
    ''' <param name="FileNameToFind">Optional: Name of a file that must exist in the dataset folder; can contain a wildcard, e.g. *.zip</param>
    ''' <param name="FolderNameToFind">Optional: Name of a subfolder that must exist in the dataset folder; can contain a wildcard, e.g. SEQ*</param>
    ''' <param name="MaxRetryCount">Maximum number of attempts</param>
    ''' <param name="LogFolderNotFound">If true, then log a warning if the folder is not found</param>
    ''' <param name="RetrievingInstrumentDataFolder">Set to True when retrieving an instrument data folder</param>
    ''' <returns>Path to the most appropriate dataset folder</returns>
    ''' <remarks>The path returned will be "\\MyEMSL" if the best folder is in MyEMSL</remarks>
    Protected Function FindValidFolder(
      dsName As String,
      fileNameToFind As String,
      folderNameToFind As String,
      maxRetryCount As Integer,
      logFolderNotFound As Boolean,
      retrievingInstrumentDataFolder As Boolean) As String

        Dim validFolderFound As Boolean
        Return FindValidFolder(dsName, fileNameToFind, folderNameToFind, maxRetryCount, logFolderNotFound, retrievingInstrumentDataFolder, validFolderFound, assumeUnpurged:=False)

    End Function

    ''' <summary>
    ''' Determines the most appropriate folder to use to obtain dataset files from
    ''' Optionally, can require that a certain file also be present in the folder for it to be deemed valid
    ''' If no folder is deemed valid, then returns the path defined by Job Param "DatasetStoragePath"
    ''' </summary>
    ''' <param name="dsName">Name of the dataset</param>
    ''' <param name="fileNameToFind">Optional: Name of a file that must exist in the dataset folder; can contain a wildcard, e.g. *.zip</param>
    ''' <param name="folderNameToFind">Optional: Name of a subfolder that must exist in the dataset folder; can contain a wildcard, e.g. SEQ*</param>
    ''' <param name="maxAttempts">Maximum number of attempts</param>
    ''' <param name="logFolderNotFound">If true, then log a warning if the folder is not found</param>
    ''' <param name="retrievingInstrumentDataFolder">Set to True when retrieving an instrument data folder</param>
    ''' <param name="validFolderFound">Output parameter: True if a valid folder is ultimately found, otherwise false</param>
    ''' <param name="assumeUnpurged">When true, this function returns the path to the dataset folder on the storage server</param>
    ''' <returns>Path to the most appropriate dataset folder</returns>
    ''' <remarks>The path returned will be "\\MyEMSL" if the best folder is in MyEMSL</remarks>
    Protected Function FindValidFolder(
      dsName As String,
      fileNameToFind As String,
      folderNameToFind As String,
      maxAttempts As Integer,
      logFolderNotFound As Boolean,
      retrievingInstrumentDataFolder As Boolean,
      <Out()> ByRef validFolderFound As Boolean,
      assumeUnpurged As Boolean) As String

        Dim strBestPath As String = String.Empty

        ' The tuples in this list are the path to check, and True if we should warn that the folder was not found
        Dim lstPathsToCheck = New List(Of Tuple(Of String, Boolean))

        Dim blnValidFolder As Boolean
        Dim blnFileNotFoundEncountered As Boolean

        validFolderFound = False

        Try
            If fileNameToFind Is Nothing Then fileNameToFind = String.Empty
            If folderNameToFind Is Nothing Then folderNameToFind = String.Empty

            If assumeUnpurged Then
                maxAttempts = 1
                logFolderNotFound = False
            End If

            Dim instrumentDataPurged = m_jobParams.GetJobParameter("InstrumentDataPurged", 0)

            If retrievingInstrumentDataFolder AndAlso instrumentDataPurged <> 0 AndAlso Not assumeUnpurged Then
                ' The instrument data is purged and we're retrieving instrument data
                ' Skip the primary dataset folder since the primary data files were most likely purged
            Else
                AddPathToCheck(lstPathsToCheck, Path.Combine(m_jobParams.GetParam("DatasetStoragePath"), dsName), True)
            End If

            If MyEMSLAvailable AndAlso Not assumeUnpurged Then
                ' \\MyEMSL
                AddPathToCheck(lstPathsToCheck, MYEMSL_PATH_FLAG, False)
            End If

            ' Optional Temp Debug: Enable compilation constant DISABLE_MYEMSL_SEARCH to disable checking MyEMSL (and thus speed things up)
#If DISABLE_MYEMSL_SEARCH Then
            If m_mgrParams.GetParam("MgrName").ToLower().Contains("monroe") Then
                lstPathsToCheck.Remove(MYEMSL_PATH_FLAG)
            End If
#End If
            If (m_AuroraAvailable OrElse Not MyEMSLAvailable) AndAlso Not assumeUnpurged Then
                AddPathToCheck(lstPathsToCheck, Path.Combine(m_jobParams.GetParam("DatasetArchivePath"), dsName), True)
            End If

            AddPathToCheck(lstPathsToCheck, Path.Combine(m_jobParams.GetParam("transferFolderPath"), dsName), False)

            blnFileNotFoundEncountered = False

            strBestPath = lstPathsToCheck.First().Item1
            For Each pathToCheck In lstPathsToCheck
                Try
                    If m_DebugLevel > 3 Then
                        Dim Msg As String = "FindValidDatasetFolder, Looking for folder " + pathToCheck.Item1
                        LogDebugMessage(Msg)
                    End If

                    If pathToCheck.Item1 = MYEMSL_PATH_FLAG Then

                        Const recurseMyEMSL = False
                        blnValidFolder = FindValidFolderMyEMSL(dsName, fileNameToFind, folderNameToFind, False, recurseMyEMSL)

                    Else

                        blnValidFolder = FindValidFolderUNC(pathToCheck.Item1, fileNameToFind, folderNameToFind, maxAttempts, logFolderNotFound And pathToCheck.Item2)
                        If Not blnValidFolder AndAlso Not String.IsNullOrEmpty(fileNameToFind) AndAlso Not String.IsNullOrEmpty(folderNameToFind) Then
                            ' Look for a subfolder named folderNameToFind that contains file fileNameToFind
                            Dim pathToCheckAlt = Path.Combine(pathToCheck.Item1, folderNameToFind)
                            blnValidFolder = FindValidFolderUNC(pathToCheckAlt, fileNameToFind, String.Empty, maxAttempts, logFolderNotFound And pathToCheck.Item2)

                            If blnValidFolder Then
                                pathToCheck = New Tuple(Of String, Boolean)(pathToCheckAlt, pathToCheck.Item2)
                            End If
                        End If

                    End If

                    If blnValidFolder Then
                        strBestPath = String.Copy(pathToCheck.Item1)
                    Else
                        blnFileNotFoundEncountered = True
                    End If

                    If blnValidFolder Then Exit For

                Catch ex As Exception
                    LogError("Exception looking for folder: " + pathToCheck.Item1, ex)
                End Try
            Next

            If blnValidFolder Then

                validFolderFound = True

                If m_DebugLevel >= 4 OrElse m_DebugLevel >= 1 AndAlso blnFileNotFoundEncountered Then
                    Dim Msg As String = "FindValidFolder, Valid dataset folder has been found:  " + strBestPath
                    If fileNameToFind.Length > 0 Then
                        Msg &= " (matched file " + fileNameToFind + ")"
                    End If
                    If folderNameToFind.Length > 0 Then
                        Msg &= " (matched folder " + folderNameToFind + ")"
                    End If
                    LogDebugMessage(Msg)
                End If

            Else
                Dim folderNotFoundMessage = "Could not find a valid dataset folder"
                If fileNameToFind.Length > 0 Then
                    ' Could not find a valid dataset folder containing file
                    folderNotFoundMessage &= " containing file " + fileNameToFind
                End If

                If logFolderNotFound AndAlso m_DebugLevel >= 1 Then
                    If assumeUnpurged Then
                        ReportStatus(folderNotFoundMessage)
                    Else
                        Dim msg As String = folderNotFoundMessage + ", Job " + m_jobParams.GetParam("StepParameters", "Job") + ", Dataset " + dsName
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, msg)
                        Console.WriteLine(msg)
                    End If
                End If

                If Not assumeUnpurged Then
                    m_message = folderNotFoundMessage
                End If
            End If

        Catch ex As Exception
            LogError("Exception looking for a valid dataset folder for dataset " + dsName, ex)
            m_message = "Exception looking for a valid dataset folder"
        End Try

        Return strBestPath

    End Function

    ''' <summary>
    ''' Determines whether the folder specified by strPathToCheck is appropriate for retrieving dataset files
    ''' </summary>
    ''' <param name="datasetName">Dataset name</param>
    ''' <param name="FileNameToFind">Optional: Name of a file that must exist in the dataset folder; can contain a wildcard, e.g. *.zip</param>
    ''' <param name="subFolderName">Optional: Name of a subfolder that must exist in the dataset folder; can contain a wildcard, e.g. SEQ*</param>
    ''' <param name="LogFolderNotFound">If true, then log a warning if the folder is not found</param>
    ''' <param name="Recurse">True to look for FileNameToFind in all subfolders of a dataset; false to only look in the primary dataset folder</param>
    ''' <returns>Path to the most appropriate dataset folder</returns>
    ''' <remarks>FileNameToFind is a file in the dataset folder; it is NOT a file in FolderNameToFind</remarks>
    Private Function FindValidFolderMyEMSL(datasetName As String, fileNameToFind As String, subFolderName As String, logFolderNotFound As Boolean, recurse As Boolean) As Boolean

        If String.IsNullOrEmpty(fileNameToFind) Then fileNameToFind = "*"

        If m_DebugLevel > 3 Then
            LogDebugMessage("FindValidFolderMyEMSL, querying MyEMSL for this dataset's files")
        End If

        Dim matchingMyEMSLFiles As List(Of MyEMSLReader.DatasetFolderOrFileInfo)

        If String.IsNullOrEmpty(subFolderName) Then
            ' Simply look for the file
            matchingMyEMSLFiles = m_MyEMSLUtilities.FindFiles(fileNameToFind, String.Empty, datasetName, recurse)
        Else
            ' First look for the subfolder
            ' If there are multiple matching subfolders, then choose the newest one
            ' The entries in matchingMyEMSLFiles will be folder entries where the "Filename" field is the folder name while the "SubDirPath" field is any parent folders above the found folder
            matchingMyEMSLFiles = m_MyEMSLUtilities.FindFiles(fileNameToFind, subFolderName, datasetName, recurse)
        End If

        If matchingMyEMSLFiles.Count > 0 Then
            Return True
        Else
            If logFolderNotFound Then
                Dim msg As String = "MyEMSL does not have any files for dataset " & datasetName
                If Not String.IsNullOrEmpty(fileNameToFind) Then
                    msg &= " and file " & fileNameToFind
                End If

                If Not String.IsNullOrEmpty(subFolderName) Then
                    msg &= " and subfolder " & subFolderName
                End If

                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, msg)
                Console.WriteLine(msg)
            End If
            Return False
        End If

    End Function

    ''' <summary>
    ''' Determines whether the folder specified by strPathToCheck is appropriate for retrieving dataset files
    ''' </summary>
    ''' <param name="PathToCheck">Path to examine</param>
    ''' <param name="FileNameToFind">Optional: Name of a file that must exist in the dataset folder; can contain a wildcard, e.g. *.zip</param>
    ''' <param name="FolderNameToFind">Optional: Name of a subfolder that must exist in the dataset folder; can contain a wildcard, e.g. SEQ*</param>
    ''' <param name="maxAttempts">Maximum number of attempts</param>
    ''' <param name="LogFolderNotFound">If true, then log a warning if the folder is not found</param>
    ''' <returns>Path to the most appropriate dataset folder</returns>
    ''' <remarks>FileNameToFind is a file in the dataset folder; it is NOT a file in FolderNameToFind</remarks>
    Private Function FindValidFolderUNC(
      PathToCheck As String,
      FileNameToFind As String,
      FolderNameToFind As String,
      maxAttempts As Integer,
      LogFolderNotFound As Boolean) As Boolean

        ' First check whether this folder exists
        ' Using a 1 second holdoff between retries
        If Not FolderExistsWithRetry(PathToCheck, 1, maxAttempts, LogFolderNotFound) Then
            Return False
        End If

        ' Folder was found
        Dim blnValidFolder = True

        If m_DebugLevel > 3 Then
            LogDebugMessage("FindValidFolderUNC, Folder found " + PathToCheck)
        End If

        ' Optionally look for FileNameToFind
        If Not String.IsNullOrEmpty(FileNameToFind) Then

            If FileNameToFind.Contains("*") Then
                If m_DebugLevel > 3 Then
                    LogDebugMessage("FindValidFolderUNC, Looking for files matching " + FileNameToFind)
                End If

                ' Wildcard in the name
                ' Look for any files matching FileNameToFind
                Dim objFolderInfo = New DirectoryInfo(PathToCheck)

                If objFolderInfo.GetFiles(FileNameToFind).Length = 0 Then
                    blnValidFolder = False
                End If
            Else
                If m_DebugLevel > 3 Then
                    LogDebugMessage("FindValidFolderUNC, Looking for file named " + FileNameToFind)
                End If

                ' Look for file FileNameToFind in this folder
                ' Note: Using a 1 second holdoff between retries
                Dim folderFound = FileExistsWithRetry(
                    Path.Combine(PathToCheck, FileNameToFind),
                    RetryHoldoffSeconds:=1,
                    eLogMsgTypeIfNotFound:=clsLogTools.LogLevels.WARN,
                    maxAttempts:=maxAttempts)

                If Not folderFound Then
                    blnValidFolder = False
                End If
            End If
        End If

        ' Optionally look for FolderNameToFind
        If blnValidFolder AndAlso Not String.IsNullOrEmpty(FolderNameToFind) Then
            If FolderNameToFind.Contains("*") Then
                If m_DebugLevel > 3 Then
                    LogDebugMessage("FindValidFolderUNC, Looking for folders matching " + FolderNameToFind)
                End If

                ' Wildcard in the name
                ' Look for any folders matching FolderNameToFind
                Dim objFolderInfo = New DirectoryInfo(PathToCheck)

                If objFolderInfo.GetDirectories(FolderNameToFind).Length = 0 Then
                    blnValidFolder = False
                End If
            Else
                If m_DebugLevel > 3 Then
                    LogDebugMessage("FindValidFolderUNC, Looking for folder named " + FolderNameToFind)
                End If

                ' Look for folder FolderNameToFind in this folder
                ' Note: Using a 1 second holdoff between retries
                If Not FolderExistsWithRetry(Path.Combine(PathToCheck, FolderNameToFind), 1, maxAttempts, LogFolderNotFound) Then
                    blnValidFolder = False
                End If
            End If
        End If

        Return blnValidFolder

    End Function

    ' Obsolete code:
    '
    ' ''' <summary>
    ' ''' Test for folder existence with a retry loop in case of temporary glitch
    ' ''' </summary>
    ' ''' <param name="FolderName">Folder name to look for</param>	
    'Private Function FolderExistsWithRetry(FolderName As String) As Boolean
    '	Return FolderExistsWithRetry(FolderName, DEFAULT_FOLDER_EXISTS_RETRY_HOLDOFF_SECONDS, DEFAULT_MAX_RETRY_COUNT, True)
    'End Function

    ' ''' <summary>
    ' ''' Test for folder existence with a retry loop in case of temporary glitch
    ' ''' </summary>
    ' ''' <param name="FolderName">Folder name to look for</param>
    ' ''' <param name="RetryHoldoffSeconds">Time, in seconds, to wait between retrying; if 0, then will default to 5 seconds; maximum value is 600 seconds</param>
    'Private Function FolderExistsWithRetry(FolderName As String, RetryHoldoffSeconds As Integer) As Boolean
    '	Return FolderExistsWithRetry(FolderName, RetryHoldoffSeconds, DEFAULT_MAX_RETRY_COUNT, True)
    'End Function

    ' ''' <summary>
    ' ''' Test for folder existence with a retry loop in case of temporary glitch
    ' ''' </summary>
    ' ''' <param name="FolderName">Folder name to look for</param>
    ' ''' <param name="RetryHoldoffSeconds">Time, in seconds, to wait between retrying; if 0, then will default to 5 seconds; maximum value is 600 seconds</param>
    ' ''' <param name="maxAttempts">Maximum number of attempts</param>
    'Private Function FolderExistsWithRetry(FolderName As String, RetryHoldoffSeconds As Integer, maxAttempts As Integer) As Boolean
    '	Return FolderExistsWithRetry(FolderName, RetryHoldoffSeconds, maxAttempts, True)
    'End Function


    ''' <summary>
    ''' Test for folder existence with a retry loop in case of temporary glitch
    ''' </summary>
    ''' <param name="FolderName">Folder name to look for</param>
    ''' <param name="RetryHoldoffSeconds">Time, in seconds, to wait between retrying; if 0, then will default to 5 seconds; maximum value is 600 seconds</param>
    ''' <param name="maxAttempts">Maximum number of attempts</param>
    ''' <param name="LogFolderNotFound">If true, then log a warning if the folder is not found</param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Private Function FolderExistsWithRetry(
      FolderName As String,
      RetryHoldoffSeconds As Integer,
      maxAttempts As Integer,
      LogFolderNotFound As Boolean) As Boolean

        If maxAttempts < 1 Then maxAttempts = 1
        If maxAttempts > 10 Then maxAttempts = 10
        Dim retryCount = maxAttempts

        If RetryHoldoffSeconds <= 0 Then RetryHoldoffSeconds = DEFAULT_FOLDER_EXISTS_RETRY_HOLDOFF_SECONDS
        If RetryHoldoffSeconds > 600 Then RetryHoldoffSeconds = 600

        While retryCount > 0
            If Directory.Exists(FolderName) Then
                Return True
            Else
                If LogFolderNotFound Then
                    If m_DebugLevel >= 2 OrElse m_DebugLevel >= 1 AndAlso retryCount = 1 Then
                        Dim errMsg As String = "Folder " + FolderName + " not found. Retry count = " + retryCount.ToString
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, errMsg)
                        Console.WriteLine(errMsg)
                    End If
                End If
                retryCount -= 1
                If retryCount <= 0 Then
                    Return False
                Else
                    Thread.Sleep(New TimeSpan(0, 0, RetryHoldoffSeconds))     'Wait RetryHoldoffSeconds seconds before retrying
                End If
            End If
        End While

        Return False

    End Function


    ''' <summary>
    ''' Creates the _ScanStats.txt file for this job's dataset
    ''' </summary>
    ''' <returns>True if success, false if a problem</returns>
    ''' <remarks>Only valid for Thermo .Raw files and .UIMF files.  Will delete the .Raw (or .UIMF) after creating the ScanStats file</remarks>
    Protected Function GenerateScanStatsFile() As Boolean

        Const deleteRawDataFile = True
        Return GenerateScanStatsFile(deleteRawDataFile)

    End Function

    ''' <summary>
    ''' Creates the _ScanStats.txt file for this job's dataset
    ''' </summary>
    ''' <param name="deleteRawDataFile">True to delete the .raw (or .uimf) file after creating the ScanStats file </param>
    ''' <returns>True if success, false if a problem</returns>
    ''' <remarks>Only valid for Thermo .Raw files and .UIMF files</remarks>
    Protected Function GenerateScanStatsFile(deleteRawDataFile As Boolean) As Boolean

        Dim strRawDataType = m_jobParams.GetParam("RawDataType")
        Dim intDatasetID = m_jobParams.GetJobParameter("DatasetID", 0)

        Dim strMSFileInfoScannerDir = m_mgrParams.GetParam("MSFileInfoScannerDir")
        If String.IsNullOrEmpty(strMSFileInfoScannerDir) Then
            LogError("Manager parameter 'MSFileInfoScannerDir' is not defined (GenerateScanStatsFile)")
            Return False
        End If

        Dim strMSFileInfoScannerDLLPath = Path.Combine(strMSFileInfoScannerDir, "MSFileInfoScanner.dll")
        If Not File.Exists(strMSFileInfoScannerDLLPath) Then
            LogError("File Not Found (GenerateScanStatsFile): " + strMSFileInfoScannerDLLPath)
            Return False
        End If

        Dim strInputFilePath As String

        ' Confirm that this dataset is a Thermo .Raw file or a .UIMF file
        Select Case GetRawDataType(strRawDataType)
            Case eRawDataTypeConstants.ThermoRawFile
                strInputFilePath = m_DatasetName + DOT_RAW_EXTENSION
            Case eRawDataTypeConstants.UIMF
                strInputFilePath = m_DatasetName + DOT_UIMF_EXTENSION
            Case Else
                LogError("Invalid dataset type for auto-generating ScanStats.txt file: " + strRawDataType)
                Return False
        End Select

        strInputFilePath = Path.Combine(m_WorkingDir, strInputFilePath)

        If Not File.Exists(strInputFilePath) Then
            If Not RetrieveSpectra(strRawDataType) Then
                Dim strExtraMsg As String = m_message
                m_message = "Error retrieving spectra file"
                If Not String.IsNullOrWhiteSpace(strExtraMsg) Then
                    m_message &= "; " + strExtraMsg
                End If
                ReportStatus(m_message, 0, True)
                Return False
            End If

            If Not m_MyEMSLUtilities.ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders) Then
                Return False
            End If
        End If

        ' Make sure the raw data file does not get copied to the results folder
        m_jobParams.AddResultFileToSkip(Path.GetFileName(strInputFilePath))

        Dim objScanStatsGenerator = New clsScanStatsGenerator(strMSFileInfoScannerDLLPath, m_DebugLevel)

        ' Create the _ScanStats.txt and _ScanStatsEx.txt files
        Dim blnSuccess = objScanStatsGenerator.GenerateScanStatsFile(strInputFilePath, m_WorkingDir, intDatasetID)

        If blnSuccess Then
            If m_DebugLevel >= 1 Then
                ReportStatus("Generated ScanStats file using " + strInputFilePath)
            End If

            Thread.Sleep(125)
            PRISM.Processes.clsProgRunner.GarbageCollectNow()

            If deleteRawDataFile Then
                Try
                    File.Delete(strInputFilePath)
                Catch ex As Exception
                    ' Ignore errors here
                End Try
            End If

        Else
            LogError("Error generating ScanStats files with clsScanStatsGenerator", objScanStatsGenerator.ErrorMessage)
            If objScanStatsGenerator.MSFileInfoScannerErrorCount > 0 Then
                ReportStatus("MSFileInfoScanner encountered " + objScanStatsGenerator.MSFileInfoScannerErrorCount.ToString() + " errors")
            End If
        End If

        Return blnSuccess

    End Function

    ''' <summary>
    ''' Split apart coordinates that look like "R00X438Y093" into R, X, and Y
    ''' </summary>
    ''' <param name="strCoord"></param>
    ''' <param name="R"></param>
    ''' <param name="X"></param>
    ''' <param name="Y"></param>
    ''' <returns>True if success, false otherwise</returns>
    ''' <remarks></remarks>
    Public Shared Function GetBrukerImagingFileCoords(
      strCoord As String,
      <Out()> ByRef R As Integer,
      <Out()> ByRef X As Integer,
      <Out()> ByRef Y As Integer) As Boolean

        Static reRegExRXY As Text.RegularExpressions.Regex
        Static reRegExRX As Text.RegularExpressions.Regex

        Dim reMatch As Text.RegularExpressions.Match
        Dim blnSuccess As Boolean

        If reRegExRXY Is Nothing Then
            reRegExRXY = New Text.RegularExpressions.Regex("R(\d+)X(\d+)Y(\d+)", Text.RegularExpressions.RegexOptions.Compiled Or Text.RegularExpressions.RegexOptions.IgnoreCase)
            reRegExRX = New Text.RegularExpressions.Regex("R(\d+)X(\d+)", Text.RegularExpressions.RegexOptions.Compiled Or Text.RegularExpressions.RegexOptions.IgnoreCase)
        End If

        ' Try to match names like R00X438Y093
        reMatch = reRegExRXY.Match(strCoord)

        blnSuccess = False

        If reMatch.Success Then
            ' Match succeeded; extract out the coordinates
            If Integer.TryParse(reMatch.Groups.Item(1).Value, R) Then blnSuccess = True
            If Integer.TryParse(reMatch.Groups.Item(2).Value, X) Then blnSuccess = True
            Integer.TryParse(reMatch.Groups.Item(3).Value, Y)

        Else
            ' Try to match names like R00X438
            reMatch = reRegExRX.Match(strCoord)

            If reMatch.Success Then
                If Integer.TryParse(reMatch.Groups.Item(1).Value, R) Then blnSuccess = True
                If Integer.TryParse(reMatch.Groups.Item(2).Value, X) Then blnSuccess = True
                Y = 0
            Else
                R = 0
                X = 0
                Y = 0
            End If
        End If

        Return blnSuccess

    End Function

    ''' <summary>
    ''' Looks for job parameters BrukerMALDI_Imaging_StartSectionX and BrukerMALDI_Imaging_EndSectionX
    ''' If defined, then populates StartSectionX and EndSectionX with the Start and End X values to filter on
    ''' </summary>
    ''' <param name="objJobParams"></param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public Shared Function GetBrukerImagingSectionFilter(
      objJobParams As IJobParams,
      <Out()> ByRef StartSectionX As Integer,
      <Out()> ByRef EndSectionX As Integer) As Boolean

        Dim blnApplySectionFilter As Boolean

        Dim strParam As String

        blnApplySectionFilter = False
        StartSectionX = -1
        EndSectionX = Int32.MaxValue

        strParam = objJobParams.GetParam("MALDI_Imaging_StartSectionX")
        If Not String.IsNullOrEmpty(strParam) Then
            If Integer.TryParse(strParam, StartSectionX) Then
                blnApplySectionFilter = True
            End If
        End If

        strParam = objJobParams.GetParam("MALDI_Imaging_EndSectionX")
        If Not String.IsNullOrEmpty(strParam) Then
            If Integer.TryParse(strParam, EndSectionX) Then
                blnApplySectionFilter = True
            End If
        End If

        Return blnApplySectionFilter

    End Function

    Protected Function GetCurrentDatasetAndJobInfo() As clsDataPackageJobInfo

        Dim jobNumber = m_jobParams.GetJobParameter("StepParameters", "Job", 0)
        Dim datasetName = m_jobParams.GetJobParameter("JobParameters", "DatasetNum", m_DatasetName)

        Dim jobInfo = New clsDataPackageJobInfo(jobNumber, datasetName)

        With jobInfo
            .DatasetID = m_jobParams.GetJobParameter("JobParameters", "DatasetID", 0)

            .Instrument = m_jobParams.GetJobParameter("JobParameters", "Instrument", String.Empty)
            .InstrumentGroup = m_jobParams.GetJobParameter("JobParameters", "InstrumentGroup", String.Empty)

            .Experiment = m_jobParams.GetJobParameter("JobParameters", "Experiment", String.Empty)
            .Experiment_Reason = String.Empty
            .Experiment_Comment = String.Empty
            .Experiment_Organism = String.Empty
            .Experiment_NEWT_ID = 0
            .Experiment_NEWT_Name = String.Empty

            .Tool = m_jobParams.GetJobParameter("JobParameters", "ToolName", String.Empty)
            .NumberOfClonedSteps = m_jobParams.GetJobParameter("NumberOfClonedSteps", 0)

            .ResultType = m_jobParams.GetJobParameter("JobParameters", "ResultType", String.Empty)
            .SettingsFileName = m_jobParams.GetJobParameter("JobParameters", "SettingsFileName", String.Empty)

            .ParameterFileName = m_jobParams.GetJobParameter("PeptideSearch", "ParmFileName", String.Empty)

            .LegacyFastaFileName = m_jobParams.GetJobParameter("PeptideSearch", "legacyFastaFileName", String.Empty)
            .OrganismDBName = String.Copy(.LegacyFastaFileName)

            .ProteinCollectionList = m_jobParams.GetJobParameter("PeptideSearch", "ProteinCollectionList", String.Empty)
            .ProteinOptions = m_jobParams.GetJobParameter("PeptideSearch", "ProteinOptions", String.Empty)

            .ServerStoragePath = m_jobParams.GetJobParameter("JobParameters", "DatasetStoragePath", String.Empty)
            .ArchiveStoragePath = m_jobParams.GetJobParameter("JobParameters", "DatasetArchivePath", String.Empty)
            .ResultsFolderName = m_jobParams.GetJobParameter("JobParameters", "inputFolderName", String.Empty)
            .DatasetFolderName = m_jobParams.GetJobParameter("JobParameters", "DatasetFolderName", String.Empty)
            .SharedResultsFolder = m_jobParams.GetJobParameter("JobParameters", "SharedResultsFolders", String.Empty)
            .RawDataType = m_jobParams.GetJobParameter("JobParameters", "RawDataType", String.Empty)
        End With

        Return jobInfo

    End Function

    ''' <summary>
    ''' Lookups up the storage path for a given data package
    ''' </summary>
    ''' <param name="ConnectionString">Database connection string (DMS_Pipeline DB, aka the broker DB)</param>
    ''' <param name="DataPackageID">Data Package ID</param>
    ''' <returns>Storage path if successful, empty path if an error or unknown data package</returns>
    ''' <remarks></remarks>
    Public Shared Function GetDataPackageStoragePath(connectionString As String, dataPackageID As Integer) As String

        'Requests Dataset information from a data package
        Const RETRY_COUNT As Short = 3

        Dim sqlStr = New Text.StringBuilder

        sqlStr.Append("Select [Share Path] AS StoragePath ")
        sqlStr.Append("From V_DMS_Data_Packages ")
        sqlStr.Append("Where ID = " & dataPackageID.ToString())

        Dim resultSet As DataTable = Nothing

        ' Get a table to hold the results of the query
        Dim success = clsGlobal.GetDataTableByQuery(sqlStr.ToString(), connectionString, "GetDataPackageStoragePath", RETRY_COUNT, resultSet)

        If Not success Then
            Dim errorMessage = "GetDataPackageStoragePath; Excessive failures attempting to retrieve data package info from database"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, errorMessage)
            Console.WriteLine(errorMessage)
            resultSet.Dispose()
            Return String.Empty
        End If

        ' Verify at least one row returned
        If resultSet.Rows.Count < 1 Then
            ' No data was returned
            ' Log an error

            Dim errorMessage = "GetDataPackageStoragePath; Data package not found: " & dataPackageID.ToString()
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, errorMessage)
            Console.WriteLine(errorMessage)
            Return String.Empty
        Else
            Dim curRow As DataRow = resultSet.Rows(0)

            Dim storagePath = clsGlobal.DbCStr(curRow(0))

            resultSet.Dispose()
            Return storagePath
        End If

    End Function

    ''' <summary>
    ''' Examines the folder tree in strFolderPath to find the a folder with a name like 2013_2
    ''' </summary>
    ''' <param name="strFolderPath"></param>
    ''' <returns>Matching folder name if found, otherwise an empty string</returns>
    ''' <remarks></remarks>
    Public Shared Function GetDatasetYearQuarter(strFolderPath As String) As String

        If String.IsNullOrEmpty(strFolderPath) Then
            Return String.Empty
        End If

        ' RegEx to find the year_quarter folder name 
        ' Valid matches include: 2014_1, 2014_01, 2014_4
        Dim reYearQuarter = New Text.RegularExpressions.Regex("^[0-9]{4}_0*[1-4]$", Text.RegularExpressions.RegexOptions.Compiled)
        Dim reMatch As Text.RegularExpressions.Match

        ' Split strFolderPath on the path separator
        Dim lstFolders = strFolderPath.Split(Path.DirectorySeparatorChar).ToList()
        lstFolders.Reverse()

        For Each strFolder As String In lstFolders
            reMatch = reYearQuarter.Match(strFolder)
            If reMatch.Success Then
                Return reMatch.Value
            End If
        Next

        Return String.Empty

    End Function

    ''' <summary>
    ''' Examine the fasta file to determine the fraction of the proteins that are decoy (reverse) proteins
    ''' </summary>
    ''' <param name="fiFastaFile">FASTA file to examine</param>
    ''' <param name="proteinCount">Output parameter: total protein count</param>
    ''' <returns>Fraction of the proteins that are decoy (for example 0.5 if half of the proteins start with Reversed_)</returns>
    ''' <remarks>Decoy proteins start with Reversed_</remarks>
    Public Shared Function GetDecoyFastaCompositionStats(
      fiFastaFile As FileInfo,
      <Out()> ByRef proteinCount As Integer) As Double

        Dim decoyProteinPrefix = GetDefaultDecoyPrefixes().First()
        Return GetDecoyFastaCompositionStats(fiFastaFile, decoyProteinPrefix, proteinCount)

    End Function

    ''' <summary>
    ''' Examine the fasta file to determine the fraction of the proteins that are decoy (reverse) proteins
    ''' </summary>
    ''' <param name="fiFastaFile">FASTA file to examine</param>
    ''' <param name="proteinCount">Output parameter: total protein count</param>
    ''' <returns>Fraction of the proteins that are decoy (for example 0.5 if half of the proteins start with Reversed_)</returns>
    ''' <remarks>Decoy proteins start with decoyProteinPrefix</remarks>
    Public Shared Function GetDecoyFastaCompositionStats(
      fiFastaFile As FileInfo,
      decoyProteinPrefix As String,
      <Out()> ByRef proteinCount As Integer) As Double

        ' Look for protein names that look like:
        ' >decoyProteinPrefix
        ' where
        ' decoyProteinPrefix is typically XXX. or XXX_ or Reversed_

        Dim prefixToFind = ">" & decoyProteinPrefix
        Dim forwardProteinCount = 0
        Dim reverseProteinCount = 0

        Using srFastaFile = New StreamReader(New FileStream(fiFastaFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            While Not srFastaFile.EndOfStream
                Dim dataLine = srFastaFile.ReadLine()
                If String.IsNullOrWhiteSpace(dataLine) Then
                    Continue While
                End If

                If dataLine.StartsWith(">") Then
                    ' Protein header line found
                    If dataLine.StartsWith(prefixToFind) Then
                        reverseProteinCount += 1
                    Else
                        forwardProteinCount += 1
                    End If
                End If
            End While
        End Using

        Dim fractionDecoy As Double = 0

        proteinCount = forwardProteinCount + reverseProteinCount
        If proteinCount > 0 Then
            fractionDecoy = reverseProteinCount / CDbl(proteinCount)
        End If

        Return fractionDecoy

    End Function

    Public Shared Function GetDefaultDecoyPrefixes() As List(Of String)

        ' Decoy proteins created by MSGF+ start with XXX_
        ' Decoy proteins created by DMS start with Reversed_
        Dim decoyPrefixes = New List(Of String) From {
          "Reversed_",
          "XXX_",
          "XXX:"}

        Return decoyPrefixes

    End Function

    ''' <summary>
    ''' Look for a JobParameters file from the previous job step
    ''' If found, copy to the working directory
    ''' </summary>
    ''' <returns>True if success, false if an error</returns>
    Private Function GetExistingJobParametersFile() As Boolean

        Try
            Dim stepNum = m_jobParams.GetJobParameter("StepParameters", "Step", 1)
            If stepNum = 1 Then
                ' This is the first step; nothing to retrieve
                Return True
            End If

            Dim transferFolderPath = GetTransferFolderPathForJobStep(useInputFolder:=True)
            If String.IsNullOrEmpty(transferFolderPath) Then
                ' Transfer folder parameter is empty; nothing to retrieve
            End If

            Dim jobParametersFilename = "JobParameters_" & m_JobNum & ".xml"
            Dim sourceFile = New FileInfo(Path.Combine(transferFolderPath, jobParametersFilename))

            If Not sourceFile.Exists Then
                ' File not found; nothing to copy
                Return True
            End If

            ' Copy the file
            If Not CopyFileToWorkDir(sourceFile.Name, transferFolderPath, m_WorkingDir, clsLogTools.LogLevels.ERROR) Then
                ' Error copying; silently ignore
            End If

            Return True

        Catch ex As Exception
            m_message = "Exception in GetExistingJobParametersFile: " & ex.Message
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex)
            Return False
        End Try

    End Function

    ''' <summary>
    ''' Examine the specified DMS_Temp_Org folder to find the FASTA files and their corresponding .fasta.LastUsed or .hashcheck files
    ''' </summary>
    ''' <param name="diOrgDbFolder"></param>
    ''' <returns>Dictionary of FASTA files, including the last usage date for each</returns>
    Private Function GetFastaFilesByLastUse(diOrgDbFolder As DirectoryInfo) As Dictionary(Of FileInfo, DateTime)

        ' Keys are the fasta file; values are the dtLastUsed time of the file (nominally obtained from a .hashcheck or .lastused file)
        Dim dctFastaFiles = New Dictionary(Of FileInfo, DateTime)

        For Each fiFile In diOrgDbFolder.GetFiles("*.fasta")
            If Not dctFastaFiles.ContainsKey(fiFile) Then
                Dim dtLastUsed As DateTime = DateMax(fiFile.LastWriteTimeUtc, fiFile.CreationTimeUtc)

                ' Look for a .hashcheck file
                Dim lstHashCheckfiles = diOrgDbFolder.GetFiles(fiFile.Name & "*.hashcheck")
                If lstHashCheckfiles.Count > 0 Then
                    dtLastUsed = DateMax(dtLastUsed, lstHashCheckfiles.First.LastWriteTimeUtc)
                End If

                ' Look for a .LastUsed file
                Dim lstLastUsedFiles = diOrgDbFolder.GetFiles(fiFile.Name & ".LastUsed")
                If lstLastUsedFiles.Count > 0 Then
                    dtLastUsed = DateMax(dtLastUsed, lstLastUsedFiles.First.LastWriteTimeUtc)

                    Try
                        ' Read the date stored in the file
                        Using srLastUsedfile = New StreamReader(New FileStream(lstLastUsedFiles.First.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                            If Not srLastUsedfile.EndOfStream Then
                                Dim strLastUseDate = srLastUsedfile.ReadLine()
                                Dim dtLastUsedActual As DateTime
                                If DateTime.TryParse(strLastUseDate, dtLastUsedActual) Then
                                    dtLastUsed = DateMax(dtLastUsed, dtLastUsedActual)
                                End If
                            End If
                        End Using
                    Catch ex As Exception
                        ' Ignore errors here
                    End Try

                End If
                dctFastaFiles.Add(fiFile, dtLastUsed)
            End If
        Next

        Return dctFastaFiles

    End Function

    ''' <summary>
    ''' Reports the amount of free memory on this computer (in MB)
    ''' </summary>
    ''' <returns>Free memory, in MB</returns>
    Public Shared Function GetFreeMemoryMB() As Single

        Static mFreeMemoryPerformanceCounter As PerformanceCounter

        Dim sngFreeMemoryMB As Single = 0
        Dim blnVirtualMachineOnPIC As Boolean = clsGlobal.UsingVirtualMachineOnPIC()

        Try
            If mFreeMemoryPerformanceCounter Is Nothing Then
                mFreeMemoryPerformanceCounter = New PerformanceCounter("Memory", "Available MBytes")
                mFreeMemoryPerformanceCounter.ReadOnly = True
            End If

            Dim intIterations = 0
            sngFreeMemoryMB = 0
            Do While sngFreeMemoryMB < Single.Epsilon AndAlso intIterations <= 3
                sngFreeMemoryMB = mFreeMemoryPerformanceCounter.NextValue()
                If sngFreeMemoryMB < Single.Epsilon Then
                    ' You sometimes have to call .NextValue() several times before it returns a useful number
                    ' Wait 1 second and then try again
                    Thread.Sleep(1000)
                End If
                intIterations += 1
            Loop

        Catch ex As Exception
            ' To avoid seeing this in the logs continually, we will only post this log message between 12 am and 12:30 am
            ' A possible fix for this is to add the user who is running this process to the "Performance Monitor Users" group in "Local Users and Groups" on the machine showing this error.  
            ' Alternatively, add the user to the "Administrators" group.
            ' In either case, you will need to reboot the computer for the change to take effect
            If Not blnVirtualMachineOnPIC AndAlso Date.Now().Hour = 0 AndAlso Date.Now().Minute <= 30 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                                     "Error instantiating the Memory.[Available MBytes] performance counter " &
                                     "(this message is only logged between 12 am and 12:30 am): " + ex.Message)
            End If
        End Try

        Try

            If sngFreeMemoryMB < Single.Epsilon Then
                ' The Performance counters are still reporting a value of 0 for available memory; use an alternate method

                If blnVirtualMachineOnPIC Then
                    ' The Memory performance counters are not available on Windows instances running under VMWare on PIC
                Else
                    If Date.Now().Hour = 15 Then
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO,
                                             "Performance monitor reports 0 MB available; using alternate method: " &
                                             "Devices.ComputerInfo().AvailablePhysicalMemory " &
                                             "(this message is only logged between 3:00 pm and 4:00 pm)")
                    End If
                End If

                sngFreeMemoryMB = CSng(clsGlobal.BytesToMB(CLng(New Devices.ComputerInfo().AvailablePhysicalMemory)))

            End If

        Catch ex As Exception
            If Date.Now().Hour = 15 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                                     "Error determining available memory using Devices.ComputerInfo().AvailablePhysicalMemory " &
                                     "(this message is only logged between 3:00 pm and 4:00 pm): " + ex.Message)
            End If
        End Try

        Return sngFreeMemoryMB

    End Function

    Protected Function GetJobInfoFilePath(intJob As Integer) As String
        Return GetJobInfoFilePath(intJob, m_WorkingDir)
    End Function

    Public Shared Function GetJobInfoFilePath(intJob As Integer, strWorkDirPath As String) As String
        Return Path.Combine(strWorkDirPath, JOB_INFO_FILE_PREFIX & intJob & ".txt")
    End Function

    ''' <summary>
    ''' Converts the given timespan to the total days, hours, minutes, or seconds as a string
    ''' </summary>
    ''' <param name="dtInterval">Timespan to convert</param>
    ''' <returns>Timespan length in human readable form</returns>
    ''' <remarks></remarks>
    Protected Function GetHumanReadableTimeInterval(dtInterval As TimeSpan) As String

        If dtInterval.TotalDays >= 1 Then
            ' Report Days
            Return dtInterval.TotalDays.ToString("0.00") + " days"
        ElseIf dtInterval.TotalHours >= 1 Then
            ' Report hours
            Return dtInterval.TotalHours.ToString("0.00") + " hours"
        ElseIf dtInterval.TotalMinutes >= 1 Then
            ' Report minutes
            Return dtInterval.TotalMinutes.ToString("0.00") + " minutes"
        Else
            ' Report seconds
            Return dtInterval.TotalSeconds.ToString("0.0") + " seconds"
        End If
    End Function

    ''' <summary>
    ''' Get the MSXML Cache Folder path that is appropriate for this job
    ''' </summary>
    ''' <param name="cacheFolderPathBase"></param>
    ''' <param name="jobParams"></param>
    ''' <param name="errorMessage"></param>
    ''' <returns></returns>
    ''' <remarks>Uses job parameter OutputFolderName, which should be something like MSXML_Gen_1_120_275966</remarks>
    Public Shared Function GetMSXmlCacheFolderPath(
      cacheFolderPathBase As String,
      jobParams As IJobParams,
      <Out()> ByRef errorMessage As String) As String

        ' Lookup the output folder; e.g. MSXML_Gen_1_120_275966
        Dim outputFolderName = jobParams.GetJobParameter("OutputFolderName", String.Empty)
        If String.IsNullOrEmpty(outputFolderName) Then
            errorMessage = "OutputFolderName is empty; cannot construct MSXmlCache path"
            Return String.Empty
        End If

        Dim msXmlToolNameVersionFolder As String
        Try
            msXmlToolNameVersionFolder = GetMSXmlToolNameVersionFolder(outputFolderName)
        Catch ex As Exception
            errorMessage = "OutputFolderName is not in the expected form of ToolName_Version_DatasetID (" & outputFolderName & "); cannot construct MSXmlCache path"
            Return String.Empty
        End Try

        Return GetMSXmlCacheFolderPath(cacheFolderPathBase, jobParams, msXmlToolNameVersionFolder, errorMessage)

    End Function

    ''' <summary>
    ''' Get the path to the cache folder; used for retrieving cached .mzML files that are stored in ToolName_Version folders
    ''' </summary>
    ''' <param name="cacheFolderPathBase">Cache folder base, e.g. \\Proto-11\MSXML_Cache</param>
    ''' <param name="jobParams">Job parameters</param>
    ''' <param name="msXmlToolNameVersionFolder">ToolName_Version folder, e.g. MSXML_Gen_1_93</param>
    ''' <param name="errorMessage">Output parameter: error message</param>
    ''' <returns>Path to the cache folder; empty string if an error</returns>
    ''' <remarks>Uses job parameter DatasetStoragePath to determine the Year_Quarter string to append to the end of the path</remarks>
    Public Shared Function GetMSXmlCacheFolderPath(
      cacheFolderPathBase As String,
      jobParams As IJobParams,
      msXmlToolNameVersionFolder As String,
      <Out()> ByRef errorMessage As String) As String

        errorMessage = String.Empty

        Dim strDatasetStoragePath As String = jobParams.GetParam("JobParameters", "DatasetStoragePath")
        If String.IsNullOrEmpty(strDatasetStoragePath) Then
            strDatasetStoragePath = jobParams.GetParam("JobParameters", "DatasetArchivePath")
        End If

        If String.IsNullOrEmpty(strDatasetStoragePath) Then
            errorMessage = "JobParameters does not contain DatasetStoragePath or DatasetArchivePath; cannot construct MSXmlCache path"
            Return String.Empty
        End If

        Dim strYearQuarter As String = GetDatasetYearQuarter(strDatasetStoragePath)
        If String.IsNullOrEmpty(strYearQuarter) Then
            errorMessage = "Unable to extract the dataset Year_Quarter code from " & strDatasetStoragePath & "; cannot construct MSXmlCache path"
            Return String.Empty
        End If

        ' Combine the cache folder path, ToolNameVersion, and the dataset Year_Quarter code
        Dim targetFolderPath = Path.Combine(cacheFolderPathBase, msXmlToolNameVersionFolder, strYearQuarter)

        Return targetFolderPath

    End Function

    ''' <summary>
    ''' Examine a folder of the form MSXML_Gen_1_93_367204 and remove the DatasetID portion
    ''' </summary>
    ''' <param name="toolNameVersionDatasetIDFolder">Shared results folder name</param>
    ''' <returns>The trimmed folder name if a valid folder; throws an exception if the folder name is not the correct format</returns>
    ''' <remarks></remarks>
    Public Shared Function GetMSXmlToolNameVersionFolder(toolNameVersionDatasetIDFolder As String) As String

        ' Remove the dataset ID from the end of the folder name
        Dim reToolNameAndVersion = New Regex("^(.+\d+_\d+)_\d+$")
        Dim reMatch = reToolNameAndVersion.Match(toolNameVersionDatasetIDFolder)
        If Not reMatch.Success Then
            Throw New Exception("Folder name is not in the expected form of ToolName_Version_DatasetID; unable to strip out the dataset ID")
        End If

        Return reMatch.Groups.Item(1).ToString

    End Function

    ''' <summary>
    ''' Retrieve the .mzML or .mzXML file associated with this job (based on Job Parameter MSXMLOutputType)
    ''' </summary>
    ''' <returns>CLOSEOUT_SUCCESS or CLOSEOUT_FAILED</returns>
    ''' <remarks>
    ''' If MSXMLOutputType is not defined, attempts to retrieve a .mzML file
    ''' If the .mzML file is not found, will attempt to create it
    ''' </remarks>
    Protected Function GetMsXmlFile() As IJobParams.CloseOutType
        Dim msXmlOutputType = m_jobParams.GetJobParameter("MSXMLOutputType", String.Empty)

        Dim eResult As IJobParams.CloseOutType

        If msXmlOutputType.ToLower() = "mzxml" Then
            eResult = GetMzXMLFile()
        Else
            eResult = GetMzMLFile()
        End If

        Return eResult

    End Function

    Protected Function GetMzMLFile() As IJobParams.CloseOutType

        ReportStatus("Getting mzML file")

        Dim errorMessage = String.Empty
        Dim fileMissingFromCache = False
        Const unzipFile = True

        Dim success = RetrieveCachedMzMLFile(unzipFile, errorMessage, fileMissingFromCache)
        If Not success Then
            Return HandleMsXmlRetrieveFailure(fileMissingFromCache, errorMessage, DOT_MZML_EXTENSION)
        End If

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    Protected Function GetMzXMLFile() As IJobParams.CloseOutType

        ' Retrieve the .mzXML file for this dataset
        ' Do not use RetrieveMZXmlFile since that function looks for any valid MSXML_Gen folder for this dataset
        ' Instead, use FindAndRetrieveMiscFiles 

        ReportStatus("Getting mzXML file")

        ' Note that capitalization matters for the extension; it must be .mzXML
        Dim FileToGet As String = m_DatasetName & DOT_MZXML_EXTENSION
        If Not FindAndRetrieveMiscFiles(FileToGet, False) Then

            ' Look for a .mzXML file in the cache instead

            Dim errorMessage = String.Empty
            Dim fileMissingFromCache = False
            Const unzipFile = True

            Dim success = RetrieveCachedMzXMLFile(unzipFile, errorMessage, fileMissingFromCache)
            If Not success Then
                Return HandleMsXmlRetrieveFailure(fileMissingFromCache, errorMessage, DOT_MZXML_EXTENSION)
            End If

        End If
        m_jobParams.AddResultFileToSkip(FileToGet)

        If Not m_MyEMSLUtilities.ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders) Then
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    Protected Function GetPBFFile() As IJobParams.CloseOutType

        ReportStatus("Getting PBF file")

        Dim errorMessage = String.Empty
        Dim fileMissingFromCache = False

        Dim success = RetrieveCachedPBFFile(errorMessage, fileMissingFromCache)
        If Not success Then
            Return HandleMsXmlRetrieveFailure(fileMissingFromCache, errorMessage, DOT_PBF_EXTENSION)
        End If

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

    End Function

    Protected Function HandleMsXmlRetrieveFailure(
      fileMissingFromCache As Boolean,
      errorMessage As String,
      msXmlExtension As String) As IJobParams.CloseOutType

        If fileMissingFromCache Then
            If String.IsNullOrEmpty(errorMessage) Then
                errorMessage = "Cached " & msXmlExtension & " file does not exist; will re-generate it"
            End If

            ReportStatus("Warning: " & errorMessage)
            Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_IN_CACHE
        End If

        If String.IsNullOrEmpty(errorMessage) Then
            errorMessage = "Unknown error in RetrieveCached" & msXmlExtension.TrimStart("."c) & "File"
        End If

        ReportStatus(errorMessage, 0, True)
        Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND

    End Function

    ''' <summary>
    ''' Gets fake job information for a dataset that is associated with a data package yet has no analysis jobs associated with the data package
    ''' </summary>
    ''' <param name="udtDatasetInfo"></param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public Shared Function GetPseudoDataPackageJobInfo(udtDatasetInfo As clsDataPackageDatasetInfo) As clsDataPackageJobInfo

        ' Store the negative of the dataset ID as the job number
        Dim jobInfo = New clsDataPackageJobInfo(-udtDatasetInfo.DatasetID, udtDatasetInfo.Dataset) With {
            .DatasetID = udtDatasetInfo.DatasetID,
            .Instrument = udtDatasetInfo.Instrument,
            .InstrumentGroup = udtDatasetInfo.InstrumentGroup,
            .Experiment = udtDatasetInfo.Experiment,
            .Experiment_Reason = udtDatasetInfo.Experiment_Reason,
            .Experiment_Comment = udtDatasetInfo.Experiment_Comment,
            .Experiment_Organism = udtDatasetInfo.Experiment_Organism,
            .Experiment_NEWT_ID = udtDatasetInfo.Experiment_NEWT_ID,
            .Experiment_NEWT_Name = udtDatasetInfo.Experiment_NEWT_Name,
            .Tool = "Dataset info (no tool)",
            .NumberOfClonedSteps = 0,
            .ResultType = "Dataset info (no type)",
            .PeptideHitResultType = clsPHRPReader.ePeptideHitResultType.Unknown,
            .SettingsFileName = String.Empty,
            .ParameterFileName = String.Empty,
            .OrganismDBName = String.Empty,
            .LegacyFastaFileName = String.Empty,
            .ProteinCollectionList = String.Empty,
            .ProteinOptions = String.Empty,
            .ResultsFolderName = String.Empty,
            .DatasetFolderName = udtDatasetInfo.Dataset,
            .SharedResultsFolder = String.Empty,
            .RawDataType = udtDatasetInfo.RawDataType
        }

        Try
            ' Archive storage path and server storage path track the folder just above the dataset folder
            Dim archiveFolder = New DirectoryInfo(udtDatasetInfo.ArchiveStoragePath)
            jobInfo.ArchiveStoragePath = archiveFolder.Parent.FullName
        Catch ex As Exception
            Console.WriteLine("Exception in GetPseudoDataPackageJobInfo determining the parent folder of " & udtDatasetInfo.ArchiveStoragePath)
            jobInfo.ArchiveStoragePath = udtDatasetInfo.ArchiveStoragePath.Replace(" \ " & udtDatasetInfo.Dataset, "")
        End Try

        Try
            Dim storageFolder = New DirectoryInfo(udtDatasetInfo.ServerStoragePath)
            jobInfo.ServerStoragePath = storageFolder.Parent.FullName
        Catch ex As Exception
            Console.WriteLine("Exception in GetPseudoDataPackageJobInfo determining the parent folder of " & udtDatasetInfo.ServerStoragePath)
            jobInfo.ServerStoragePath = udtDatasetInfo.ServerStoragePath.Replace(" \ " & udtDatasetInfo.Dataset, "")
        End Try

        Return jobInfo

    End Function

    Public Shared Function GetRawDataType(strRawDataType As String) As eRawDataTypeConstants

        If String.IsNullOrEmpty(strRawDataType) Then
            Return eRawDataTypeConstants.Unknown
        End If

        Select Case strRawDataType.ToLower()
            Case RAW_DATA_TYPE_DOT_D_FOLDERS
                Return eRawDataTypeConstants.AgilentDFolder
            Case RAW_DATA_TYPE_ZIPPED_S_FOLDERS
                Return eRawDataTypeConstants.ZippedSFolders
            Case RAW_DATA_TYPE_DOT_RAW_FOLDER
                Return eRawDataTypeConstants.MicromassRawFolder
            Case RAW_DATA_TYPE_DOT_RAW_FILES
                Return eRawDataTypeConstants.ThermoRawFile
            Case RAW_DATA_TYPE_DOT_WIFF_FILES
                Return eRawDataTypeConstants.AgilentQStarWiffFile
            Case RAW_DATA_TYPE_DOT_UIMF_FILES
                Return eRawDataTypeConstants.UIMF
            Case RAW_DATA_TYPE_DOT_MZXML_FILES
                Return eRawDataTypeConstants.mzXML
            Case RAW_DATA_TYPE_DOT_MZML_FILES
                Return eRawDataTypeConstants.mzML
            Case RAW_DATA_TYPE_BRUKER_FT_FOLDER
                Return eRawDataTypeConstants.BrukerFTFolder
            Case RAW_DATA_TYPE_BRUKER_MALDI_SPOT
                Return eRawDataTypeConstants.BrukerMALDISpot
            Case RAW_DATA_TYPE_BRUKER_MALDI_IMAGING
                Return eRawDataTypeConstants.BrukerMALDIImaging
            Case RAW_DATA_TYPE_BRUKER_TOF_BAF_FOLDER
                Return eRawDataTypeConstants.BrukerTOFBaf
            Case Else
                Return eRawDataTypeConstants.Unknown
        End Select

    End Function

    Public Shared Function GetRawDataTypeName(jobParams As IJobParams, <Out()> ByRef errorMessage As String) As String

        errorMessage = String.Empty

        Dim msXmlOutputType As String = jobParams.GetParam("MSXMLOutputType")

        If String.IsNullOrWhiteSpace(msXmlOutputType) Then
            Return jobParams.GetParam("RawDataType")
        Else
            Select Case msXmlOutputType.ToLower()
                Case "mzxml"
                    Return RAW_DATA_TYPE_DOT_MZXML_FILES
                Case "mzml"
                    Return RAW_DATA_TYPE_DOT_MZML_FILES
                Case Else
                    Return String.Empty
            End Select
        End If

    End Function

    Protected Function GetRawDataTypeName() As String

        Dim errorMessage As String = Nothing
        Dim rawDataTypeName = GetRawDataTypeName(m_jobParams, errorMessage)

        If String.IsNullOrWhiteSpace(rawDataTypeName) Then
            If String.IsNullOrWhiteSpace(errorMessage) Then
                LogError("Unable to determine the instrument data type using GetRawDataTypeName")
            Else
                LogError(errorMessage)
            End If

            Return String.Empty
        End If

        Return rawDataTypeName

    End Function

    Public Shared Function GetHPCOptions(jobParams As IJobParams, managerName As String) As udtHPCOptionsType

        Dim stepTool = jobParams.GetJobParameter("StepTool", "Unknown_Tool")

        Dim udtHPCOptions = New udtHPCOptionsType

        udtHPCOptions.HeadNode = jobParams.GetJobParameter("HPCHeadNode", "")
        If stepTool.ToLower() = "MSGFPlus_HPC".ToLower() AndAlso String.IsNullOrWhiteSpace(udtHPCOptions.HeadNode) Then
            ' Run this job using HPC, despite the fact that the settings file does not have the HPC settings defined
            udtHPCOptions.HeadNode = "deception2.pnnl.gov"
            udtHPCOptions.UsingHPC = True
        Else
            udtHPCOptions.UsingHPC = Not String.IsNullOrWhiteSpace(udtHPCOptions.HeadNode)
        End If

        udtHPCOptions.ResourceType = jobParams.GetJobParameter("HPCResourceType", "socket")
        ' Obsolete parameter; no longer used: udtHPCOptions.NodeGroup = jobParams.GetJobParameter("HPCNodeGroup", "ComputeNodes")

        ' Share paths used:
        ' \\picfs\projects\DMS
        ' \\winhpcfs\projects\DMS           (this is a Windows File System wrapper to \\picfs, which is an Isilon FS)

        If FORCE_WINHPC_FS Then
            udtHPCOptions.SharePath = jobParams.GetJobParameter("HPCSharePath", "\\winhpcfs\projects\DMS")
        Else
            udtHPCOptions.SharePath = jobParams.GetJobParameter("HPCSharePath", "\\picfs.pnl.gov\projects\DMS")
        End If


        ' Auto-switched the share path to \\winhpcfs starting April 15, 2014
        ' Stopped doing this April 21, 2014, because the drive was low on space
        ' Switched back to \\winhpcfs on April 24, because the connection to picfs is unstable
        '

        If FORCE_WINHPC_FS Then
            If udtHPCOptions.SharePath.StartsWith("\\picfs", StringComparison.CurrentCultureIgnoreCase) Then
                ' Auto switch the share path
                udtHPCOptions.SharePath = clsGlobal.UpdateHostName(udtHPCOptions.SharePath, "\\winhpcfs\")
            End If
        Else
            If udtHPCOptions.SharePath.StartsWith("\\winhpcfs", StringComparison.CurrentCultureIgnoreCase) Then
                ' Auto switch the share path
                udtHPCOptions.SharePath = clsGlobal.UpdateHostName(udtHPCOptions.SharePath, "\\picfs.pnl.gov\")
            End If
        End If

        udtHPCOptions.MinimumMemoryMB = jobParams.GetJobParameter("HPCMinMemoryMB", 0)
        udtHPCOptions.MinimumCores = jobParams.GetJobParameter("HPCMinCores", 0)

        If udtHPCOptions.UsingHPC AndAlso udtHPCOptions.MinimumMemoryMB <= 0 Then
            udtHPCOptions.MinimumMemoryMB = 28000
        End If

        If udtHPCOptions.UsingHPC AndAlso udtHPCOptions.MinimumCores <= 0 Then
            udtHPCOptions.MinimumCores = 16
        End If

        Dim mgrNameClean = String.Empty

        For charIndex = 0 To managerName.Length - 1
            If Path.GetInvalidFileNameChars.Contains(managerName.Chars(charIndex)) Then
                mgrNameClean &= "_"
            Else
                mgrNameClean &= managerName.Chars(charIndex)
            End If
        Next

        ' Example WorkDirPath: 
        ' \\picfs.pnl.gov\projects\DMS\DMS_Work_Dir\Pub-60-3
        ' \\winhpcfs\projects\DMS\DMS_Work_Dir\Pub-60-3
        udtHPCOptions.WorkDirPath = Path.Combine(udtHPCOptions.SharePath, "DMS_Work_Dir", mgrNameClean)

        Return udtHPCOptions

    End Function

    ''' <summary>
    ''' Examines job parameter SharedResultsFolders to construct a list of the shared result folders
    ''' </summary>
    ''' <returns>List of folder names</returns>
    Protected Function GetSharedResultFolderList() As IEnumerable(Of String)

        Dim sharedResultFolderNames As New List(Of String)

        Dim strSharedResultFolders = m_jobParams.GetParam("SharedResultsFolders")

        If strSharedResultFolders.Contains(",") Then

            ' Split on commas and populate sharedResultFolderNames
            For Each strItem As String In strSharedResultFolders.Split(","c)
                Dim strItemTrimmed = strItem.Trim()
                If strItemTrimmed.Length > 0 Then
                    sharedResultFolderNames.Add(strItemTrimmed)
                End If
            Next

            ' Reverse the list so that the last item in strSharedResultFolders is the first item in sharedResultFolderNames
            sharedResultFolderNames.Reverse()
        Else
            ' Just one item in strSharedResultFolders
            sharedResultFolderNames.Add(strSharedResultFolders)
        End If

        Return sharedResultFolderNames

    End Function

    ''' <summary>
    ''' Get the name of the split fasta file to use for this job
    ''' </summary>
    ''' <param name="jobParams"></param>
    ''' <param name="errorMessage">Output parameter: error message</param>
    ''' <returns>The name of the split fasta file to use</returns>
    ''' <remarks>Returns an empty string if an error</remarks>
    Public Shared Function GetSplitFastaFileName(jobParams As IJobParams, <Out()> ByRef errorMessage As String) As String
        Dim numberOfClonedSteps = 0

        Return GetSplitFastaFileName(jobParams, errorMessage, numberOfClonedSteps)

    End Function

    ''' <summary>
    ''' Get the name of the split fasta file to use for this job
    ''' </summary>
    ''' <param name="jobParams"></param>
    ''' <param name="errorMessage">Output parameter: error message</param>
    ''' <param name="numberOfClonedSteps">Output parameter: total number of cloned steps</param>
    ''' <returns>The name of the split fasta file to use</returns>
    ''' <remarks>Returns an empty string if an error</remarks>
    Public Shared Function GetSplitFastaFileName(jobParams As IJobParams, <Out()> ByRef errorMessage As String, <Out()> ByRef numberOfClonedSteps As Integer) As String

        errorMessage = String.Empty
        numberOfClonedSteps = 0

        Dim legacyFastaFileName = jobParams.GetJobParameter("LegacyFastaFileName", "")
        If String.IsNullOrEmpty(legacyFastaFileName) Then
            errorMessage = "Parameter LegacyFastaFileName is empty for the job; cannot determine the SplitFasta file name for this job step"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, errorMessage)
            Console.WriteLine(errorMessage)
            Return String.Empty
        End If

        numberOfClonedSteps = jobParams.GetJobParameter("NumberOfClonedSteps", 0)
        If numberOfClonedSteps = 0 Then
            errorMessage = "Settings file is missing parameter NumberOfClonedSteps; cannot determine the SplitFasta file name for this job step"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, errorMessage)
            Console.WriteLine(errorMessage)
            Return String.Empty
        End If

        Dim iteration = GetSplitFastaIteration(jobParams, errorMessage)
        If iteration < 1 Then
            Dim toolName = jobParams.GetJobParameter("ToolName", String.Empty)
            If clsGlobal.IsMatch(toolName, "Mz_Refinery") Then
                ' Running MzRefinery
                ' Override iteration to be 1
                iteration = 1
            Else
                If String.IsNullOrEmpty(errorMessage) Then
                    errorMessage = "GetSplitFastaIteration computed an iteration value of " & iteration & "; cannot determine the SplitFasta file name for this job step"
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, errorMessage)
                    Console.WriteLine(errorMessage)
                End If
                Return String.Empty
            End If
        End If

        Dim fastaNameBase = Path.GetFileNameWithoutExtension(legacyFastaFileName)
        Dim splitFastaName = fastaNameBase & "_" & numberOfClonedSteps.ToString() & "x_"

        If numberOfClonedSteps < 10 Then
            splitFastaName &= iteration.ToString("0") & ".fasta"
        ElseIf numberOfClonedSteps < 100 Then
            splitFastaName &= iteration.ToString("00") & ".fasta"
        Else
            splitFastaName &= iteration.ToString("000") & ".fasta"
        End If

        Return splitFastaName

    End Function

    Public Shared Function GetSplitFastaIteration(jobParams As IJobParams, <Out()> ByRef errorMessage As String) As Integer

        errorMessage = String.Empty

        Dim cloneStepRenumStart = jobParams.GetJobParameter("CloneStepRenumberStart", 0)
        If cloneStepRenumStart = 0 Then
            errorMessage = "Settings file is missing parameter CloneStepRenumberStart; cannot determine the SplitFasta iteration value for this job step"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, errorMessage)
            Console.WriteLine(errorMessage)
            Return 0
        End If

        Dim stepNumber = jobParams.GetJobParameter("StepParameters", "Step", 0)
        If stepNumber = 0 Then
            errorMessage = "Job parameter Step is missing; cannot determine the SplitFasta iteration value for this job step"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, errorMessage)
            Console.WriteLine(errorMessage)
            Return 0
        End If

        Return stepNumber - cloneStepRenumStart + 1

    End Function

    ''' <summary>
    ''' Get the input or output transfer folder path specific to this job step
    ''' </summary>
    ''' <param name="useInputFolder">True to use "InputFolderName", False to use "OutputFolderName"</param>
    ''' <returns></returns>
    Protected Function GetTransferFolderPathForJobStep(useInputFolder As Boolean) As String

        Dim transferFolderPathBase = m_jobParams.GetParam("transferFolderPath")
        If String.IsNullOrEmpty(transferFolderPathBase) Then
            ' Transfer folder parameter is empty; return an empty string
            Return String.Empty
        End If

        ' Append the dataset folder name to the transfer folder path
        Dim datasetFolderName = m_jobParams.GetParam("StepParameters", "DatasetFolderName")
        If String.IsNullOrWhiteSpace(datasetFolderName) Then datasetFolderName = m_DatasetName

        Dim folderName As String

        If useInputFolder Then
            folderName = m_jobParams.GetParam("InputFolderName")
        Else
            folderName = m_jobParams.GetParam("OutputFolderName")
        End If

        If String.IsNullOrEmpty(folderName) Then
            ' Input (or outuput) folder parameter is empty; return an empty string
            Return String.Empty
        End If

        Dim transferFolderPath = Path.Combine(transferFolderPathBase, datasetFolderName, folderName)

        Return transferFolderPath

    End Function

    ''' <summary>
    ''' Looks up dataset information for a data package
    ''' </summary>
    ''' <param name="dctDataPackageDatasets"></param>
    ''' <returns>True if a data package is defined and it has datasets associated with it</returns>
    ''' <remarks></remarks>
    Protected Function LoadDataPackageDatasetInfo(<Out> ByRef dctDataPackageDatasets As Dictionary(Of Integer, clsDataPackageDatasetInfo)) As Boolean

        ' Gigasax.DMS_Pipeline
        Dim connectionString As String = m_mgrParams.GetParam("brokerconnectionstring")

        Dim dataPackageID As Integer = m_jobParams.GetJobParameter("DataPackageID", -1)

        If dataPackageID < 0 Then
            dctDataPackageDatasets = New Dictionary(Of Integer, clsDataPackageDatasetInfo)
            Return False
        Else
            Return LoadDataPackageDatasetInfo(connectionString, dataPackageID, dctDataPackageDatasets)
        End If
    End Function

    ''' <summary>
    ''' Looks up dataset information for a data package
    ''' </summary>
    ''' <param name="ConnectionString">Database connection string (DMS_Pipeline DB, aka the broker DB)</param>
    ''' <param name="DataPackageID">Data Package ID</param>
    ''' <param name="dctDataPackageDatasets">Datasets associated with the given data package</param>
    ''' <returns>True if a data package is defined and it has datasets associated with it</returns>
    ''' <remarks></remarks>
    Public Shared Function LoadDataPackageDatasetInfo(
      connectionString As String,
      dataPackageID As Integer,
      <Out> ByRef dctDataPackageDatasets As Dictionary(Of Integer, clsDataPackageDatasetInfo)) As Boolean

        ' Obtains the dataset information for a data package
        Const RETRY_COUNT As Short = 3

        dctDataPackageDatasets = New Dictionary(Of Integer, clsDataPackageDatasetInfo)

        Dim sqlStr = New Text.StringBuilder

        ' Note that this queries view V_DMS_Data_Package_Datasets in the DMS_Pipeline database
        ' That view references   view V_DMS_Data_Package_Aggregation_Datasets in the DMS_Data_Package database

        sqlStr.Append(" SELECT Dataset, DatasetID, Instrument, InstrumentGroup, ")
        sqlStr.Append("        Experiment, Experiment_Reason, Experiment_Comment, Organism, Experiment_NEWT_ID, Experiment_NEWT_Name, ")
        sqlStr.Append("        Dataset_Folder_Path, Archive_Folder_Path, RawDataType")
        sqlStr.Append(" FROM V_DMS_Data_Package_Datasets")
        sqlStr.Append(" WHERE Data_Package_ID = " + dataPackageID.ToString())
        sqlStr.Append(" ORDER BY Dataset")

        Dim resultSet As DataTable = Nothing

        ' Get a table to hold the results of the query
        Dim success = clsGlobal.GetDataTableByQuery(sqlStr.ToString(), connectionString, "LoadDataPackageDatasetInfo", RETRY_COUNT, resultSet)

        If Not success Then
            Dim errorMessage = "LoadDataPackageDatasetInfo; Excessive failures attempting to retrieve data package dataset info from database"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, errorMessage)
            Console.WriteLine(errorMessage)
            resultSet.Dispose()
            Return False
        End If

        ' Verify at least one row returned
        If resultSet.Rows.Count < 1 Then
            ' No data was returned
            Dim warningMessage = "LoadDataPackageDatasetInfo; No datasets were found for data package " & dataPackageID.ToString()
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, warningMessage)
            Console.WriteLine(warningMessage)
            Return False
        End If

        For Each curRow As DataRow In resultSet.Rows
            Dim udtDatasetInfo = ParseDataPackageDatasetInfoRow(curRow)

            If Not dctDataPackageDatasets.ContainsKey(udtDatasetInfo.DatasetID) Then
                dctDataPackageDatasets.Add(udtDatasetInfo.DatasetID, udtDatasetInfo)
            End If
        Next

        resultSet.Dispose()
        Return True

    End Function

    ''' <summary>
    ''' Looks up dataset information for the data package associated with this analysis job
    ''' </summary>
    ''' <param name="dctDataPackageJobs"></param>
    ''' <returns>True if a data package is defined and it has analysis jobs associated with it</returns>
    ''' <remarks></remarks>
    Protected Function LoadDataPackageJobInfo(<Out> ByRef dctDataPackageJobs As Dictionary(Of Integer, clsDataPackageJobInfo)) As Boolean

        ' Gigasax.DMS_Pipeline
        Dim connectionString As String = m_mgrParams.GetParam("brokerconnectionstring")

        Dim dataPackageID As Integer = m_jobParams.GetJobParameter("DataPackageID", -1)

        If dataPackageID < 0 Then
            dctDataPackageJobs = New Dictionary(Of Integer, clsDataPackageJobInfo)
            Return False
        Else
            Return LoadDataPackageJobInfo(connectionString, dataPackageID, dctDataPackageJobs)
        End If
    End Function

    ''' <summary>
    ''' Looks up job information for a data package
    ''' </summary>
    ''' <param name="ConnectionString">Database connection string (DMS_Pipeline DB, aka the broker DB)</param>
    ''' <param name="DataPackageID">Data Package ID</param>
    ''' <param name="dctDataPackageJobs">Jobs associated with the given data package</param>
    ''' <returns>True if a data package is defined and it has analysis jobs associated with it</returns>
    ''' <remarks></remarks>
    Public Shared Function LoadDataPackageJobInfo(
      ConnectionString As String,
      DataPackageID As Integer,
      <Out> ByRef dctDataPackageJobs As Dictionary(Of Integer, clsDataPackageJobInfo)) As Boolean

        ' Obtains the job information for a data package
        Const RETRY_COUNT As Short = 3

        dctDataPackageJobs = New Dictionary(Of Integer, clsDataPackageJobInfo)

        Dim sqlStr = New Text.StringBuilder

        ' Note that this queries view V_DMS_Data_Package_Aggregation_Jobs in the DMS_Pipeline database
        ' That view references   view V_DMS_Data_Package_Aggregation_Jobs in the DMS_Data_Package database
        ' The two views have the same name, but some columns differ

        sqlStr.Append(" SELECT Job, Dataset, DatasetID, Instrument, InstrumentGroup, ")
        sqlStr.Append("        Experiment, Experiment_Reason, Experiment_Comment, Organism, Experiment_NEWT_ID, Experiment_NEWT_Name, ")
        sqlStr.Append("        Tool, ResultType, SettingsFileName, ParameterFileName, ")
        sqlStr.Append("        OrganismDBName, ProteinCollectionList, ProteinOptions,")
        sqlStr.Append("        ServerStoragePath, ArchiveStoragePath, ResultsFolder, DatasetFolder, SharedResultsFolder, RawDataType")
        sqlStr.Append(" FROM V_DMS_Data_Package_Aggregation_Jobs")
        sqlStr.Append(" WHERE Data_Package_ID = " + DataPackageID.ToString())
        sqlStr.Append(" ORDER BY Dataset, Tool")

        Dim resultSet As DataTable = Nothing

        ' Get a table to hold the results of the query
        Dim success = clsGlobal.GetDataTableByQuery(sqlStr.ToString(), ConnectionString, "LoadDataPackageJobInfo", RETRY_COUNT, resultSet)

        If Not success Then
            Dim errorMessage = "LoadDataPackageJobInfo; Excessive failures attempting to retrieve data package job info from database"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, errorMessage)
            Console.WriteLine(errorMessage)
            resultSet.Dispose()
            Return False
        End If

        ' Verify at least one row returned
        If resultSet.Rows.Count < 1 Then
            ' No data was returned
            Dim warningMessage As String

            ' If the data package exists and has datasets associated with it, then Log this as a warning but return true
            ' Otherwise, log an error and return false

            sqlStr.Clear()
            sqlStr.Append(" SELECT Count(*) AS Datasets")
            sqlStr.Append(" FROM S_V_DMS_Data_Package_Aggregation_Datasets")
            sqlStr.Append(" WHERE Data_Package_ID = " + DataPackageID.ToString())

            ' Get a table to hold the results of the query
            success = clsGlobal.GetDataTableByQuery(sqlStr.ToString(), ConnectionString, "LoadDataPackageJobInfo", RETRY_COUNT, resultSet)
            If success AndAlso resultSet.Rows.Count > 0 Then
                For Each curRow As DataRow In resultSet.Rows
                    Dim datasetCount = clsGlobal.DbCInt(curRow(0))

                    If datasetCount > 0 Then
                        warningMessage = "LoadDataPackageJobInfo; No jobs were found for data package " & DataPackageID &
                            ", but it does have " & datasetCount & " dataset"
                        If datasetCount > 1 Then warningMessage &= "s"
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, warningMessage)
                        Console.WriteLine(warningMessage)
                        Return True
                    End If
                Next
            End If

            warningMessage = "LoadDataPackageJobInfo; No jobs were found for data package " & DataPackageID.ToString()
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, warningMessage)
            Console.WriteLine(warningMessage)
            Return False
        End If

        For Each curRow As DataRow In resultSet.Rows
            Dim dataPkgJob = ParseDataPackageJobInfoRow(curRow)

            If Not dctDataPackageJobs.ContainsKey(dataPkgJob.Job) Then
                dctDataPackageJobs.Add(dataPkgJob.Job, dataPkgJob)
            End If
        Next

        resultSet.Dispose()

        Return True

    End Function

    Private Sub LogDebugMessage(debugMessage As String)
        Console.WriteLine(debugMessage)
        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, debugMessage)
    End Sub

    Private Shared Sub LogDebugMessage(debugMessage As String, statusTools As IStatusFile)
        Console.WriteLine(debugMessage)
        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, debugMessage)

        If Not statusTools Is Nothing Then
            statusTools.CurrentOperation = debugMessage
            statusTools.UpdateAndWrite(0)
        End If
    End Sub

    ''' <summary>
    ''' Retrieve the information for the specified analysis job
    ''' </summary>
    ''' <param name="jobNumber">Job number</param>
    ''' <param name="jobInfo">Output parameter: Job Info</param>
    ''' <returns>True if success; false if an error</returns>
    ''' <remarks>This procedure is used by clsAnalysisResourcesQCART</remarks>
    Protected Function LookupJobInfo(jobNumber As Integer, <Out> ByRef jobInfo As clsDataPackageJobInfo) As Boolean

        Const RETRY_COUNT = 3

        Dim sqlStr = New Text.StringBuilder

        ' This query uses view V_Analysis_Job_Export_DataPkg in the DMS5 database
        sqlStr.Append("SELECT Job, Dataset, DatasetID, InstrumentName As Instrument, InstrumentGroup,")
        sqlStr.Append("        Experiment, Experiment_Reason, Experiment_Comment, Organism, Experiment_NEWT_ID, Experiment_NEWT_Name, ")
        sqlStr.Append("        Tool, ResultType, SettingsFileName, ParameterFileName,")
        sqlStr.Append("        OrganismDBName, ProteinCollectionList, ProteinOptions,")
        sqlStr.Append("        ServerStoragePath, ArchiveStoragePath, ResultsFolder, DatasetFolder, '' AS SharedResultsFolder, RawDataType ")
        sqlStr.Append("FROM V_Analysis_Job_Export_DataPkg ")
        sqlStr.Append("WHERE Job = " & jobNumber)

        Dim resultSet As DataTable = Nothing
        jobInfo = New clsDataPackageJobInfo(0, String.Empty)

        ' Gigasax.DMS5
        Dim dmsConnectionString = m_mgrParams.GetParam("connectionstring")

        ' Get a table to hold the results of the query
        Dim success = clsGlobal.GetDataTableByQuery(sqlStr.ToString(), dmsConnectionString, "LookupJobInfo", RETRY_COUNT, resultSet)

        If Not success Then
            Dim errorMessage = "LookupJobInfo; Excessive failures attempting to retrieve data package job info from database"
            ReportStatus(errorMessage, 0, True)
            resultSet.Dispose()
            Return False
        End If

        ' Verify at least one row returned
        If resultSet.Rows.Count < 1 Then
            ' No data was returned
            LogError("Job " & jobNumber & " not found in view V_Analysis_Job_Export_DataPkg")
            Return False
        End If

        jobInfo = ParseDataPackageJobInfoRow(resultSet.Rows.Item(0))

        Return True

    End Function

    ''' <summary>
    ''' Retrieve the job parameters from the pipeline database for the given analysis job
    ''' The analysis job must have completed successfully, since the parameters 
    ''' are retrieved from tables T_Jobs_History, T_Job_Steps_History, and T_Job_Parameters_History
    ''' </summary>
    ''' <param name="brokerConnection">DMS_Pipline database connection (must already be open)</param>
    ''' <param name="jobNumber">Job number</param>
    ''' <param name="jobParameters">Output parameter: Dictionary of job parameters where keys are parameter names (section names are ignored)</param>
    ''' <returns>True if success; false if an error</returns>
    ''' <remarks>This procedure is used by clsAnalysisToolRunnerPRIDEConverter</remarks>
    Private Shared Function LookupJobParametersFromHistory(
      brokerConnection As SqlConnection,
      jobNumber As Integer,
      <Out()> ByRef jobParameters As Dictionary(Of String, String),
      <Out()> ByRef errorMsg As String) As Boolean

        Const RETRY_COUNT = 3
        Const TIMEOUT_SECONDS = 30

        jobParameters = New Dictionary(Of String, String)(StringComparison.InvariantCultureIgnoreCase)
        errorMsg = String.Empty

        ' Throttle the calls to this function to avoid overloading the database for data packages with hundreds of jobs
        While Date.UtcNow.Subtract(mLastJobParameterFromHistoryLookup).TotalMilliseconds < 50
            Thread.Sleep(25)
        End While

        mLastJobParameterFromHistoryLookup = Date.UtcNow

        Try

            Dim myCmd = New SqlCommand("GetJobStepParamsAsTableUseHistory") With {
                .CommandType = CommandType.StoredProcedure,
                .Connection = brokerConnection,
                .CommandTimeout = TIMEOUT_SECONDS
            }

            myCmd.Parameters.Add(New SqlParameter("@Return", SqlDbType.Int)).Direction = ParameterDirection.ReturnValue
            myCmd.Parameters.Add(New SqlParameter("@jobNumber", SqlDbType.Int)).Value = jobNumber
            myCmd.Parameters.Add(New SqlParameter("@stepNumber", SqlDbType.Int)).Value = 1

            ' Execute the SP

            Dim resultSet As DataTable = Nothing
            Dim retryCount = RETRY_COUNT
            Dim success = False

            While retryCount > 0 And Not success
                Try
                    Using Da = New SqlDataAdapter(myCmd)
                        Using Ds = New DataSet
                            Da.Fill(Ds)
                            resultSet = Ds.Tables(0)
                        End Using
                    End Using

                    success = True
                Catch ex As Exception
                    retryCount -= 1S
                    Dim msg = "Exception running stored procedure " & myCmd.CommandText & ": " + ex.Message + "; RetryCount = " + retryCount.ToString

                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg)
                    Console.WriteLine(msg)

                    Thread.Sleep(5000)              ' Delay for 5 second before trying again
                End Try
            End While

            If Not success Then
                errorMsg = "Unable to retrieve job parameters from history for job " & jobNumber
                Return False
            End If

            ' Verify at least one row returned
            If resultSet.Rows.Count < 1 Then
                ' No data was returned
                ' Log an error

                errorMsg = "Historical parameters were not found for job " & jobNumber
                Return False
            End If

            For Each curRow As DataRow In resultSet.Rows
                ' Dim section = clsGlobal.DbCStr(curRow(0))
                Dim parameter = clsGlobal.DbCStr(curRow(1))
                Dim value = clsGlobal.DbCStr(curRow(2))

                If jobParameters.ContainsKey(parameter) Then
                    Dim msg = "Job " & jobNumber & " has multiple values for parameter " & parameter & "; only using the first occurrence"
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, msg)
                    Console.WriteLine(msg)
                Else
                    jobParameters.Add(parameter, value)
                End If
            Next

            resultSet.Dispose()

            Return True

        Catch ex As Exception
            errorMsg = "Exception retrieving parameters from history for job " & jobNumber
            Dim detailedMsg = "Exception retrieving parameters from history for job " & jobNumber & ": " & ex.Message
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, detailedMsg)
            Console.WriteLine(detailedMsg)
            Return False
        End Try

    End Function

    ''' <summary>
    ''' Estimate the amount of disk space required for the FASTA file associated with this analysis job
    ''' </summary>
    ''' <param name="proteinCollectionInfo">Collection info object</param>
    ''' <returns>Space required, in MB</returns>
    ''' <remarks>Uses both m_jobParams and m_mgrParams; returns 0 if a problem (e.g. the legacy fasta file is not listed in V_Organism_DB_File_Export)</remarks>
    Public Function LookupLegacyDBDiskSpaceRequiredMB(proteinCollectionInfo As clsProteinCollectionInfo) As Double

        Try

            Dim dmsConnectionString = m_mgrParams.GetParam("connectionstring")
            If String.IsNullOrWhiteSpace(dmsConnectionString) Then
                LogError("Error in LookupLegacyDBSizeWithIndices: manager parameter connectionstring is not defined")
                Return 0
            End If

            Dim legacyFastaName As String
            If proteinCollectionInfo.UsingSplitFasta Then
                Dim errorMessage As String = String.Empty
                legacyFastaName = GetSplitFastaFileName(m_jobParams, errorMessage)
            Else
                legacyFastaName = proteinCollectionInfo.LegacyFastaName
            End If

            Dim sqlQuery As String = "SELECT File_Size_KB FROM V_Organism_DB_File_Export WHERE (FileName = '" & legacyFastaName & "')"

            ' Results, as a list of columns (first row only if multiple rows)
            Dim lstResults = New List(Of String)

            Dim success = clsGlobal.GetQueryResultsTopRow(sqlQuery, dmsConnectionString, lstResults, "LookupLegacyDBSizeWithIndices")

            If Not success OrElse lstResults Is Nothing OrElse lstResults.Count = 0 Then
                ' Empty query results
                Dim statusMessage = "Warning: Could not determine the legacy fasta file's size for job " & m_JobNum & ", file " & legacyFastaName
                If proteinCollectionInfo.UsingSplitFasta Then
                    ' Likely the FASTA file has not yet been split
                    ReportStatus(statusMessage & "; likely the split fasta file has not yet been created", 0, False)
                Else
                    ReportStatus(statusMessage, 0, True)
                End If

                Return 0
            End If

            Dim fileSizeKB As Integer
            If Not Integer.TryParse(lstResults.First(), fileSizeKB) Then
                ReportStatus("Legacy fasta file size is not numeric, job " & m_JobNum & ", file " & legacyFastaName & ": " & lstResults.First(), 0, True)
                Return 0
            End If

            ' Assume that the MSGF+ index files will be 15 times larger than the legacy FASTA file itself
            Dim fileSizeMB = (fileSizeKB + fileSizeKB * 15) / 1024.0

            ' Pad the expected size by an additional 15%
            Return fileSizeMB * 1.15

        Catch ex As Exception
            ReportStatus("Error in LookupLegacyDBSizeWithIndices", ex)
            Return 0
        End Try

    End Function

    ''' <summary>
    ''' Moves a file from one folder to another folder
    ''' </summary>
    ''' <param name="diSourceFolder"></param>
    ''' <param name="diTargetFolder"></param>
    ''' <param name="sourceFileName"></param>
    ''' <remarks></remarks>
    Protected Sub MoveFileToFolder(diSourceFolder As DirectoryInfo, diTargetFolder As DirectoryInfo, sourceFileName As String)
        Dim fiSourceFile = New FileInfo(Path.Combine(diSourceFolder.FullName, sourceFileName))
        Dim targetFilePath = Path.Combine(diTargetFolder.FullName, sourceFileName)
        fiSourceFile.MoveTo(targetFilePath)
    End Sub


    ''' <summary>
    ''' Override current job information, including dataset name, dataset ID, storage paths, Organism Name, Protein Collection, and protein options
    ''' </summary>
    ''' <param name="dataPkgJob"></param>
    ''' <returns></returns>
    ''' <remarks> Does not override the job number</remarks>
    Protected Function OverrideCurrentDatasetAndJobInfo(dataPkgJob As clsDataPackageJobInfo) As Boolean

        Dim blnAggregationJob = False

        If String.IsNullOrEmpty(dataPkgJob.Dataset) Then
            LogError("OverrideCurrentDatasetAndJobInfo; Column 'Dataset' not defined for job " & dataPkgJob.Job & " in the data package")
            Return False
        End If

        If clsGlobal.IsMatch(dataPkgJob.Dataset, "Aggregation") Then
            blnAggregationJob = True
        End If

        If Not blnAggregationJob Then
            ' Update job params to have the details for the current dataset
            ' This is required so that we can use FindDataFile to find the desired files
            If String.IsNullOrEmpty(dataPkgJob.ServerStoragePath) Then
                LogError("OverrideCurrentDatasetAndJobInfo; Column 'ServerStoragePath' not defined for job " & dataPkgJob.Job & " in the data package")
                Return False
            End If

            If String.IsNullOrEmpty(dataPkgJob.ArchiveStoragePath) Then
                LogError("OverrideCurrentDatasetAndJobInfo; Column 'ArchiveStoragePath' not defined for job " & dataPkgJob.Job & " in the data package")
                Return False
            End If

            If String.IsNullOrEmpty(dataPkgJob.ResultsFolderName) Then
                LogError("OverrideCurrentDatasetAndJobInfo; Column 'ResultsFolderName' not defined for job " & dataPkgJob.Job & " in the data package")
                Return False
            End If

            If String.IsNullOrEmpty(dataPkgJob.DatasetFolderName) Then
                LogError("OverrideCurrentDatasetAndJobInfo; Column 'DatasetFolderName' not defined for job " & dataPkgJob.Job & " in the data package")
                Return False
            End If
        End If


        With dataPkgJob

            m_jobParams.AddDatasetInfo(.Dataset, .DatasetID)
            m_DatasetName = String.Copy(.Dataset)

            m_jobParams.AddAdditionalParameter("JobParameters", "DatasetNum", .Dataset)
            m_jobParams.AddAdditionalParameter("JobParameters", "DatasetID", .DatasetID.ToString())

            m_jobParams.AddAdditionalParameter("JobParameters", "Instrument", .Instrument)
            m_jobParams.AddAdditionalParameter("JobParameters", "InstrumentGroup", .InstrumentGroup)

            m_jobParams.AddAdditionalParameter("JobParameters", "ToolName", .Tool)
            m_jobParams.AddAdditionalParameter("JobParameters", "ResultType", .ResultType)
            m_jobParams.AddAdditionalParameter("JobParameters", "SettingsFileName", .SettingsFileName)

            m_jobParams.AddAdditionalParameter("PeptideSearch", "ParmFileName", .ParameterFileName)

            If String.IsNullOrWhiteSpace(.OrganismDBName) Then
                m_jobParams.AddAdditionalParameter("PeptideSearch", "generatedFastaName", "na")
            Else
                m_jobParams.AddAdditionalParameter("PeptideSearch", "generatedFastaName", .OrganismDBName)
            End If

            If String.IsNullOrWhiteSpace(.ProteinCollectionList) OrElse .ProteinCollectionList = "na" Then
                m_jobParams.AddAdditionalParameter("PeptideSearch", "legacyFastaFileName", .OrganismDBName)
            Else
                m_jobParams.AddAdditionalParameter("PeptideSearch", "legacyFastaFileName", "na")
            End If

            m_jobParams.AddAdditionalParameter("PeptideSearch", "ProteinCollectionList", .ProteinCollectionList)
            m_jobParams.AddAdditionalParameter("PeptideSearch", "ProteinOptions", .ProteinOptions)

            m_jobParams.AddAdditionalParameter("JobParameters", "DatasetStoragePath", .ServerStoragePath)
            m_jobParams.AddAdditionalParameter("JobParameters", "DatasetArchivePath", .ArchiveStoragePath)
            m_jobParams.AddAdditionalParameter("JobParameters", "inputFolderName", .ResultsFolderName)
            m_jobParams.AddAdditionalParameter("JobParameters", "DatasetFolderName", .DatasetFolderName)
            m_jobParams.AddAdditionalParameter("JobParameters", "SharedResultsFolders", .SharedResultsFolder)
            m_jobParams.AddAdditionalParameter("JobParameters", "RawDataType", .RawDataType)

        End With

        Return True

    End Function

    Private Shared Function ParseDataPackageDatasetInfoRow(curRow As DataRow) As clsDataPackageDatasetInfo

        Dim datasetName = clsGlobal.DbCStr(curRow("Dataset"))
        Dim datasetId = clsGlobal.DbCInt(curRow("DatasetID"))

        Dim datasetInfo = New clsDataPackageDatasetInfo(datasetName, datasetId)

        datasetInfo.Instrument = clsGlobal.DbCStr(curRow("Instrument"))
        datasetInfo.InstrumentGroup = clsGlobal.DbCStr(curRow("InstrumentGroup"))
        datasetInfo.Experiment = clsGlobal.DbCStr(curRow("Experiment"))
        datasetInfo.Experiment_Reason = clsGlobal.DbCStr(curRow("Experiment_Reason"))
        datasetInfo.Experiment_Comment = clsGlobal.DbCStr(curRow("Experiment_Comment"))
        datasetInfo.Experiment_Organism = clsGlobal.DbCStr(curRow("Organism"))
        datasetInfo.Experiment_NEWT_ID = clsGlobal.DbCInt(curRow("Experiment_NEWT_ID"))
        datasetInfo.Experiment_NEWT_Name = clsGlobal.DbCStr(curRow("Experiment_NEWT_Name"))
        datasetInfo.ServerStoragePath = clsGlobal.DbCStr(curRow("Dataset_Folder_Path"))
        datasetInfo.ArchiveStoragePath = clsGlobal.DbCStr(curRow("Archive_Folder_Path"))
        datasetInfo.RawDataType = clsGlobal.DbCStr(curRow("RawDataType"))

        Return datasetInfo

    End Function

    Private Shared Function ParseDataPackageJobInfoRow(curRow As DataRow) As clsDataPackageJobInfo

        Dim dataPkgJob = clsGlobal.DbCInt(curRow("Job"))
        Dim dataPkgDataset = clsGlobal.DbCStr(curRow("Dataset"))

        Dim jobInfo = New clsDataPackageJobInfo(dataPkgJob, dataPkgDataset)

        jobInfo.DatasetID = clsGlobal.DbCInt(curRow("DatasetID"))
        jobInfo.Instrument = clsGlobal.DbCStr(curRow("Instrument"))
        jobInfo.InstrumentGroup = clsGlobal.DbCStr(curRow("InstrumentGroup"))
        jobInfo.Experiment = clsGlobal.DbCStr(curRow("Experiment"))
        jobInfo.Experiment_Reason = clsGlobal.DbCStr(curRow("Experiment_Reason"))
        jobInfo.Experiment_Comment = clsGlobal.DbCStr(curRow("Experiment_Comment"))
        jobInfo.Experiment_Organism = clsGlobal.DbCStr(curRow("Organism"))
        jobInfo.Experiment_NEWT_ID = clsGlobal.DbCInt(curRow("Experiment_NEWT_ID"))
        jobInfo.Experiment_NEWT_Name = clsGlobal.DbCStr(curRow("Experiment_NEWT_Name"))
        jobInfo.Tool = clsGlobal.DbCStr(curRow("Tool"))
        jobInfo.ResultType = clsGlobal.DbCStr(curRow("ResultType"))
        jobInfo.PeptideHitResultType = clsPHRPReader.GetPeptideHitResultType(jobInfo.ResultType)
        jobInfo.SettingsFileName = clsGlobal.DbCStr(curRow("SettingsFileName"))
        jobInfo.ParameterFileName = clsGlobal.DbCStr(curRow("ParameterFileName"))
        jobInfo.OrganismDBName = clsGlobal.DbCStr(curRow("OrganismDBName"))
        jobInfo.ProteinCollectionList = clsGlobal.DbCStr(curRow("ProteinCollectionList"))
        jobInfo.ProteinOptions = clsGlobal.DbCStr(curRow("ProteinOptions"))

        ' This will be updated later for SplitFasta jobs (using function LookupJobParametersFromHistory)
        jobInfo.NumberOfClonedSteps = 0

        If String.IsNullOrWhiteSpace(jobInfo.ProteinCollectionList) OrElse jobInfo.ProteinCollectionList = "na" Then
            jobInfo.LegacyFastaFileName = String.Copy(jobInfo.OrganismDBName)
        Else
            jobInfo.LegacyFastaFileName = "na"
        End If

        jobInfo.ServerStoragePath = clsGlobal.DbCStr(curRow("ServerStoragePath"))
        jobInfo.ArchiveStoragePath = clsGlobal.DbCStr(curRow("ArchiveStoragePath"))
        jobInfo.ResultsFolderName = clsGlobal.DbCStr(curRow("ResultsFolder"))
        jobInfo.DatasetFolderName = clsGlobal.DbCStr(curRow("DatasetFolder"))
        jobInfo.SharedResultsFolder = clsGlobal.DbCStr(curRow("SharedResultsFolder"))
        jobInfo.RawDataType = clsGlobal.DbCStr(curRow("RawDataType"))

        Return jobInfo

    End Function

    ''' <summary>
    ''' Download any
    ''' </summary>
    ''' <param name="downloadFolderPath"></param>
    ''' <param name="folderLayout"></param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Protected Function ProcessMyEMSLDownloadQueue(downloadFolderPath As String, folderLayout As Downloader.DownloadFolderLayout) As Boolean
        Dim success = m_MyEMSLUtilities.ProcessMyEMSLDownloadQueue(downloadFolderPath, folderLayout)
        Return success
    End Function

    ''' <summary>
    ''' Delete the specified FASTA file and its associated files
    ''' </summary>
    ''' <param name="diOrgDbFolder"></param>
    ''' <param name="fiFileToPurge"></param>
    ''' <param name="legacyFastaFileBaseName"></param>
    ''' <returns>Number of bytes deleted</returns>
    Private Function PurgeFastaFiles(diOrgDbFolder As DirectoryInfo, fiFileToPurge As FileInfo, legacyFastaFileBaseName As String) As Long

        Dim baseName = Path.GetFileNameWithoutExtension(fiFileToPurge.Name)

        If Not String.IsNullOrWhiteSpace(legacyFastaFileBaseName) AndAlso baseName.StartsWith(legacyFastaFileBaseName) Then
            ' The current job needs this file; do not delete it
            Return 0
        End If

        ' Delete all files associated with this fasta file
        Dim lstFilesToDelete = New List(Of FileInfo)
        lstFilesToDelete.AddRange(diOrgDbFolder.GetFiles(baseName & ".*"))

        If m_DebugLevel >= 1 Then
            Dim fileText = String.Format("{0,2} file", lstFilesToDelete.Count)
            If lstFilesToDelete.Count <> 1 Then
                fileText &= "s"
            End If
            LogDebugMessage("Deleting " & fileText & " associated with " & fiFileToPurge.FullName)
        End If

        Dim bytesDeleted As Long = 0

        Try
            For Each fiFileToDelete In lstFilesToDelete
                Dim fileSizeBytes = fiFileToDelete.Length
                fiFileToDelete.Delete()
                bytesDeleted += fileSizeBytes
            Next
        Catch ex As Exception
            ReportStatus("Error in PurgeFastaFiles", ex)
        End Try

        Return bytesDeleted

    End Function

    ''' <summary>
    ''' Purges old fasta files (and related suffix array files) from localOrgDbFolder
    ''' </summary>
    ''' <param name="localOrgDbFolder"></param>
    ''' <param name="freeSpaceThresholdPercent">Value between 1 and 50</param>
    ''' <param name="requiredFreeSpaceMB">If greater than 0, the free space that we anticipate will be needed for the given fasta file</param>
    ''' <param name="legacyFastaFileBaseName">
    ''' Legacy fasta file name (without .fasta)
    ''' For split fasta jobs, should not include the splitcount and segment number, e.g. should not include _25x_07 or _25x_08
    ''' </param>
    ''' <remarks></remarks>
    Protected Sub PurgeFastaFilesIfLowFreeSpace(
      localOrgDbFolder As String,
      freeSpaceThresholdPercent As Integer,
      requiredFreeSpaceMB As Double,
      legacyFastaFileBaseName As String)

        If freeSpaceThresholdPercent < 1 Then freeSpaceThresholdPercent = 1
        If freeSpaceThresholdPercent > 50 Then freeSpaceThresholdPercent = 50

        Try

            Dim diOrgDbFolder = New DirectoryInfo(localOrgDbFolder)
            If diOrgDbFolder.FullName.Length <= 2 Then
                ReportStatus("Warning: Org DB folder length is less than 3 characters; this is unexpected: " & diOrgDbFolder.FullName)
                Exit Sub
            End If

            ' Look for file MaxDirSize.txt which defines the maximum space that the files can use
            Dim fiMaxDirSize = New FileInfo(Path.Combine(diOrgDbFolder.FullName, "MaxDirSize.txt"))

            Dim driveLetter = diOrgDbFolder.FullName.Substring(0, 2)

            If (Not driveLetter.EndsWith(":")) Then
                ' The folder is not local to this computer
                If Not fiMaxDirSize.Exists Then
                    LogError("Warning: Orb DB folder path does not have a colon and could not find file " & fiMaxDirSize.Name &
                             "; cannot manage drive space usage: " & diOrgDbFolder.FullName)
                    Exit Sub
                End If

                ' MaxDirSize.txt file found; delete the older FASTA files to free up space
                PurgeFastaFilesUsingSpaceUsedThreshold(fiMaxDirSize, legacyFastaFileBaseName)
                Exit Sub
            End If

            Dim localDriveInfo = New DriveInfo(driveLetter)
            Dim percentFreeSpaceAtStart As Double = localDriveInfo.AvailableFreeSpace / CDbl(localDriveInfo.TotalSize) * 100

            If (percentFreeSpaceAtStart >= freeSpaceThresholdPercent) Then
                If m_DebugLevel >= 2 Then
                    Dim freeSpaceGB = clsGlobal.BytesToGB(localDriveInfo.AvailableFreeSpace)
                    ReportStatus(String.Format("Free space on {0} ({1:F1} GB) is over {2}% of the total space; purge not required",
                                               localDriveInfo.Name, freeSpaceGB, freeSpaceThresholdPercent))
                End If
            Else
                PurgeFastaFilesUsingFreeSpaceThreshold(localDriveInfo, diOrgDbFolder, legacyFastaFileBaseName,
                                                       freeSpaceThresholdPercent, requiredFreeSpaceMB, percentFreeSpaceAtStart)
            End If

            If fiMaxDirSize.Exists Then
                ' MaxDirSize.txt file exists; possibly delete additional FASTA files to free up space
                PurgeFastaFilesUsingSpaceUsedThreshold(fiMaxDirSize, legacyFastaFileBaseName)
            End If

        Catch ex As Exception
            ReportStatus("Error in PurgeFastaFilesIfLowFreeSpace", ex)
        End Try

    End Sub

    Private Sub PurgeFastaFilesUsingFreeSpaceThreshold(
      localDriveInfo As DriveInfo,
      diOrgDbFolder As DirectoryInfo,
      legacyFastaFileBaseName As String,
      freeSpaceThresholdPercent As Integer,
      requiredFreeSpaceMB As Double,
      percentFreeSpaceAtStart As Double)

        Dim freeSpaceGB = clsGlobal.BytesToGB(localDriveInfo.AvailableFreeSpace)

        Dim logInfoMessages =
            m_DebugLevel >= 1 AndAlso freeSpaceGB < 100 OrElse
            m_DebugLevel >= 2 AndAlso freeSpaceGB < 250 OrElse
            m_DebugLevel >= 3

        If logInfoMessages Then
            ReportStatus(String.Format("Free space on {0} ({1:F1} GB) is {2:F1}% of the total space; purge required since less than threshold of {3}%",
                                       localDriveInfo.Name, freeSpaceGB, percentFreeSpaceAtStart, freeSpaceThresholdPercent))
        End If

        ' Obtain a dictionary of FASTA files where Keys are FileInfo and values are last usage date
        Dim dctFastaFiles As Dictionary(Of FileInfo, DateTime) = GetFastaFilesByLastUse(diOrgDbFolder)

        Dim lstFastaFilesByLastUse = From item In dctFastaFiles Order By item.Value Select item.Key
        Dim totalBytesPurged As Long = 0

        For Each fiFileToPurge In lstFastaFilesByLastUse
            ' Abort this process if the LastUsed date of this file is less than 5 days old
            Dim dtLastUsed As DateTime
            If dctFastaFiles.TryGetValue(fiFileToPurge, dtLastUsed) Then
                If Date.UtcNow.Subtract(dtLastUsed).TotalDays < 5 Then
                    If logInfoMessages Then
                        ReportStatus("All fasta files in " & diOrgDbFolder.FullName & " are less than 5 days old; " &
                                     "will not purge any more files to free disk space")
                    End If
                    Exit For
                End If
            End If

            ' Delete all files associated with this fasta file
            ' However, do not delete it if the name starts with legacyFastaFileBaseName
            Dim bytesDeleted = PurgeFastaFiles(diOrgDbFolder, fiFileToPurge, legacyFastaFileBaseName)
            totalBytesPurged += bytesDeleted

            ' Re-check the disk free space
            Dim percentFreeSpace = localDriveInfo.AvailableFreeSpace / CDbl(localDriveInfo.TotalSize) * 100
            Dim updatedFreeSpaceGB = clsGlobal.BytesToGB(localDriveInfo.AvailableFreeSpace)

            If requiredFreeSpaceMB > 0 AndAlso updatedFreeSpaceGB * 1024.0 < requiredFreeSpaceMB Then
                ' Required free space is known, and we're not yet there
                ' Keep deleting files
                If m_DebugLevel >= 2 Then
                    LogDebugMessage(String.Format("Free space on {0} ({1:F1} GB) is now {2:F1}% of the total space",
                                                  localDriveInfo.Name, updatedFreeSpaceGB, percentFreeSpace))
                End If
            Else
                ' Either required free space is not known, or we have more than enough free space

                If (percentFreeSpace >= freeSpaceThresholdPercent) Then
                    ' Target threshold reached
                    If m_DebugLevel >= 1 Then
                        ReportStatus(String.Format("Free space on {0} ({1:F1} GB) is now over {2}% of the total space; " &
                                                   "deleted {3:F1} GB of cached files",
                                                   localDriveInfo.Name, updatedFreeSpaceGB, freeSpaceThresholdPercent, clsGlobal.BytesToGB(totalBytesPurged)))
                    End If
                    Exit For
                ElseIf m_DebugLevel >= 2 Then
                    ' Keep deleting until we reach the target threshold for free space
                    LogDebugMessage(String.Format("Free space on {0} ({1:F1} GB) is now {2:F1}% of the total space",
                                                  localDriveInfo.Name, updatedFreeSpaceGB, percentFreeSpace))
                End If
            End If
        Next

        ' We have deleted all of the files that can be deleted
        Dim finalFreeSpaceGB = clsGlobal.BytesToGB(localDriveInfo.AvailableFreeSpace)
        If requiredFreeSpaceMB > 0 AndAlso finalFreeSpaceGB * 1024.0 < requiredFreeSpaceMB Then

            ReportStatus(String.Format("Warning: unable to delete enough files to free up the required space on {0} ({1:F1} GB vs. {2:F1} GB); " &
                                       "deleted {3:F1} GB of cached files",
                                       localDriveInfo.Name, finalFreeSpaceGB, requiredFreeSpaceMB / 1024.0, clsGlobal.BytesToGB(totalBytesPurged)))
        End If

    End Sub

    ''' <summary>
    ''' Use the space usage defined in MaxDirSize.txt to decide if any FASTA files need to be deleted
    ''' </summary>
    ''' <param name="fiMaxDirSize">MaxDirSize.txt file in the local organism DB folder</param>
    ''' <param name="legacyFastaFileBaseName">Base FASTA file name for the current analysis job</param>
    Private Sub PurgeFastaFilesUsingSpaceUsedThreshold(fiMaxDirSize As FileInfo, legacyFastaFileBaseName As String)

        Try
            Dim diOrgDbFolder As DirectoryInfo = fiMaxDirSize.Directory
            Dim errorSuffix = "; cannot manage drive space usage: " & diOrgDbFolder.FullName
            Dim maxSizeGB = 0

            Using reader = New StreamReader(New FileStream(fiMaxDirSize.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                While Not reader.EndOfStream
                    Dim dataLine = reader.ReadLine
                    If String.IsNullOrEmpty(dataLine) OrElse dataLine.StartsWith("#") Then
                        Continue While
                    End If

                    Dim lineParts = dataLine.Split("="c)
                    If lineParts.Count < 2 Then
                        Continue While
                    End If

                    If String.Equals(lineParts(0), "MaxSizeGB", StringComparison.InvariantCultureIgnoreCase) Then
                        If Not Integer.TryParse(lineParts(1), maxSizeGB) Then
                            LogError("MaxSizeGB line does not contain an integer in " & fiMaxDirSize.FullName & errorSuffix)
                            Exit Sub
                        End If
                        Exit While
                    End If

                End While
            End Using

            If maxSizeGB = 0 Then
                LogError("MaxSizeGB line not found in " & fiMaxDirSize.FullName & errorSuffix)
                Exit Sub
            End If

            Dim spaceUsageBytes As Long = 0

            For Each fiFile In diOrgDbFolder.GetFiles("*", SearchOption.TopDirectoryOnly)
                spaceUsageBytes += fiFile.Length
            Next

            Dim spaceUsageGB = clsGlobal.BytesToGB(spaceUsageBytes)
            If spaceUsageGB <= maxSizeGB Then
                ' Space usage is under the threshold
                Dim statusMessage = String.Format("Space usage in {0} is {1:F1} GB, which is below the threshold of {2} GB; nothing to purge",
                                                  diOrgDbFolder.FullName, spaceUsageGB, maxSizeGB)
                ReportStatus(statusMessage, 3)
                Exit Sub
            End If

            ' Space usage is too high; need to purge some files
            ' Obtain a dictionary of FASTA files where Keys are FileInfo and values are last usage date
            Dim dctFastaFiles As Dictionary(Of FileInfo, DateTime) = GetFastaFilesByLastUse(diOrgDbFolder)

            Dim lstFastaFilesByLastUse = From item In dctFastaFiles Order By item.Value Select item.Key

            Dim bytesToPurge = CLng(spaceUsageBytes - maxSizeGB * 1024.0 * 1024 * 1024)
            Dim totalBytesPurged As Long = 0

            For Each fiFileToPurge In lstFastaFilesByLastUse
                ' Abort this process if the LastUsed date of this file is less than 5 days old
                Dim dtLastUsed As DateTime
                If dctFastaFiles.TryGetValue(fiFileToPurge, dtLastUsed) Then
                    If Date.UtcNow.Subtract(dtLastUsed).TotalDays < 5 Then
                        ReportStatus("All fasta files in " & diOrgDbFolder.FullName & " are less than 5 days old; " &
                                     "will not purge any more files to free disk space")
                        Exit For
                    End If
                End If

                ' Delete all files associated with this fasta file
                ' However, do not delete it if the name starts with legacyFastaFileBaseName
                Dim bytesDeleted = PurgeFastaFiles(diOrgDbFolder, fiFileToPurge, legacyFastaFileBaseName)
                totalBytesPurged += bytesDeleted

                If totalBytesPurged < bytesToPurge Then
                    ' Keep deleting files
                    If m_DebugLevel >= 2 Then
                        LogDebugMessage(String.Format("Purging FASTA files: {0:F1} / {1:F1} MB deleted",
                                                      clsGlobal.BytesToMB(totalBytesPurged), clsGlobal.BytesToMB(bytesToPurge)))
                    End If
                Else
                    ' Enough files have been deleted
                    ReportStatus(String.Format("Space usage in {0} is now below {1} GB; deleted {2:F1} GB of cached files",
                                               diOrgDbFolder.FullName, maxSizeGB, clsGlobal.BytesToGB(totalBytesPurged)))
                    Exit Sub
                End If
            Next

            If totalBytesPurged < bytesToPurge Then
                ReportStatus(String.Format("Warning: unable to delete enough files to lower the space usage in {0} to below {1} GB; " &
                                           "deleted {2:F1} GB of cached files",
                                           diOrgDbFolder.FullName, maxSizeGB, clsGlobal.BytesToGB(totalBytesPurged)))
            End If

        Catch ex As Exception
            ReportStatus("Error in PurgeFastaFilesUsingSpaceUsedThreshold", ex)
        End Try

    End Sub

    Protected Function RenameDuplicatePHRPFile(SourceFolderPath As String, SourceFilename As String, TargetFolderPath As String, strPrefixToAdd As String, intJob As Integer) As Boolean
        Try
            Dim fiFileToRename = New FileInfo(Path.Combine(SourceFolderPath, SourceFilename))
            Dim strFilePathWithPrefix As String = Path.Combine(TargetFolderPath, strPrefixToAdd & fiFileToRename.Name)

            Thread.Sleep(100)
            fiFileToRename.MoveTo(strFilePathWithPrefix)

            m_jobParams.AddResultFileToSkip(Path.GetFileName(strFilePathWithPrefix))

        Catch ex As Exception
            LogError("Exception renaming PHRP file " & SourceFilename & " for job " & intJob & " (data package has multiple jobs for the same dataset)", ex)
            Return False
        End Try

        Return True

    End Function

    ''' <summary>
    ''' Looks for the specified file in the given folder
    ''' If present, returns the full path to the file
    ''' If not present, looks for a file named FileName_StoragePathInfo.txt; if that file is found, opens the file and reads the path
    ''' If the file isn't found (and the _StoragePathInfo.txt file isn't present), then returns an empty string
    ''' </summary>
    ''' <param name="FolderPath">The folder to look in</param>
    ''' <param name="FileName">The file name to find</param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public Shared Function ResolveStoragePath(FolderPath As String, FileName As String) As String

        Dim strPhysicalFilePath As String = String.Empty
        Dim strFilePath As String

        Dim strLineIn As String

        strFilePath = Path.Combine(FolderPath, FileName)

        If File.Exists(strFilePath) Then
            ' The desired file is located in folder FolderPath
            strPhysicalFilePath = strFilePath
        Else
            ' The desired file was not found
            strFilePath &= STORAGE_PATH_INFO_FILE_SUFFIX

            If File.Exists(strFilePath) Then
                ' The _StoragePathInfo.txt file is present
                ' Open that file to read the file path on the first line of the file

                Using srInFile = New StreamReader(New FileStream(strFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                    strLineIn = srInFile.ReadLine
                    strPhysicalFilePath = strLineIn
                End Using

            End If
        End If

        Return strPhysicalFilePath

    End Function

    ''' <summary>
    ''' Looks for the STORAGE_PATH_INFO_FILE_SUFFIX file in the working folder
    ''' If present, looks for a file named _StoragePathInfo.txt; if that file is found, opens the file and reads the path
    ''' If the file named _StoragePathInfo.txt isn't found, then looks for a ser file in the specified folder
    ''' If found, returns the path to the ser file
    ''' If not found, then looks for a 0.ser folder in the specified folder
    ''' If found, returns the path to the 0.ser folder
    ''' Otherwise, returns an empty string
    ''' </summary>
    ''' <param name="FolderPath">The folder to look in</param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public Shared Function ResolveSerStoragePath(FolderPath As String) As String

        Dim diFolder As DirectoryInfo
        Dim fiFile As FileInfo

        Dim strPhysicalFilePath As String
        Dim strFilePath As String

        Dim strLineIn As String

        strFilePath = Path.Combine(FolderPath, STORAGE_PATH_INFO_FILE_SUFFIX)

        If File.Exists(strFilePath) Then
            ' The desired file is located in folder FolderPath
            ' The _StoragePathInfo.txt file is present
            ' Open that file to read the file path on the first line of the file

            Using srInFile = New StreamReader(New FileStream(strFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))

                strLineIn = srInFile.ReadLine
                strPhysicalFilePath = strLineIn

            End Using
        Else
            ' The desired file was not found

            ' Look for a ser file in the dataset folder
            strPhysicalFilePath = Path.Combine(FolderPath, BRUKER_SER_FILE)
            fiFile = New FileInfo(strPhysicalFilePath)

            If Not fiFile.Exists Then
                ' See if a folder named 0.ser exists in FolderPath
                strPhysicalFilePath = Path.Combine(FolderPath, BRUKER_ZERO_SER_FOLDER)
                diFolder = New DirectoryInfo(strPhysicalFilePath)
                If Not diFolder.Exists Then
                    strPhysicalFilePath = String.Empty
                End If
            End If

        End If

        Return strPhysicalFilePath

    End Function

    ''' <summary>
    ''' Retrieve the files specified by the file processing options parameter
    ''' </summary>
    ''' <param name="fileSpecList">
    ''' File processing options, examples:
    ''' sequest:_syn.txt:nocopy,sequest:_fht.txt:nocopy,sequest:_dta.zip:nocopy,sequest:_syn_ModSummary.txt:nocopy,masic_finnigan:_ScanStatsEx.txt:nocopy
    ''' sequest:_syn.txt,sequest:_syn_MSGF.txt,sequest:_fht.txt,sequest:_fht_MSGF.txt,sequest:_dta.zip,sequest:_syn_ModSummary.txt
    ''' MSGFPlus:_msgfplus_syn.txt,MSGFPlus:_msgfplus_fht.txt,MSGFPlus:_dta.zip,MSGFPlus:_syn_ModSummary.txt,masic_finnigan:_ScanStatsEx.txt,masic_finnigan:_ReporterIons.txt:copy
    ''' MSGFPlus:_msgfplus_syn.txt,MSGFPlus:_msgfplus_syn_ModSummary.txt,MSGFPlus:_dta.zip
    ''' </param>
    ''' <param name="fileRetrievalMode">Used by plugins to indicate the types of files that are required (in case fileSpecList is not configured correctly for a given data package job)</param>
    ''' <returns>True if success, false if a problem</returns>
    ''' <remarks>
    ''' This function is used by plugins PhosphoFDRAggregator and PRIDEMzXML
    ''' However, PrideMzXML is dormant as of September 2013
    ''' </remarks>
    Protected Function RetrieveAggregateFiles(
      fileSpecList As List(Of String),
      fileRetrievalMode As DataPackageFileRetrievalModeConstants,
      <Out> ByRef dctDataPackageJobs As Dictionary(Of Integer, clsDataPackageJobInfo)) As Boolean

        Dim blnSuccess As Boolean

        Try
            If Not LoadDataPackageJobInfo(dctDataPackageJobs) Then
                m_message = "Error looking up datasets and jobs using LoadDataPackageJobInfo"
                dctDataPackageJobs = Nothing
                Return False
            End If
        Catch ex As Exception
            ReportStatus("RetrieveAggregateFiles; Exception calling LoadDataPackageJobInfo", ex)
            dctDataPackageJobs = Nothing
            Return False
        End Try

        Try
            Dim diWorkingDirectory = New DirectoryInfo(m_WorkingDir)

            ' Cache the current dataset and job info
            Dim currentDatasetAndJobInfo = GetCurrentDatasetAndJobInfo()

            For Each dataPkgJob As KeyValuePair(Of Integer, clsDataPackageJobInfo) In dctDataPackageJobs

                If Not OverrideCurrentDatasetAndJobInfo(dataPkgJob.Value) Then
                    Return False
                End If

                ' See if this job matches any of the entries in fileSpecList
                Dim fileSpecListCurrent = New List(Of String)

                For Each fileSpec As String In fileSpecList
                    Dim fileSpecTerms = fileSpec.Trim().Split(":"c).ToList()
                    If dataPkgJob.Value.Tool.ToLower().StartsWith(fileSpecTerms(0).Trim().ToLower()) Then
                        fileSpecListCurrent = fileSpecList
                        Exit For
                    End If
                Next

                If fileSpecListCurrent.Count = 0 Then
                    Select Case fileRetrievalMode
                        Case DataPackageFileRetrievalModeConstants.Ascore

                            If dataPkgJob.Value.Tool.ToLower().StartsWith("msgf") Then
                                ' MSGF+
                                fileSpecListCurrent = New List(Of String) From {
                                    "MSGFPlus:_msgfplus_syn.txt",
                                    "MSGFPlus:_msgfplus_syn_ModSummary.txt",
                                    "MSGFPlus:_dta.zip"}

                            End If

                            If dataPkgJob.Value.Tool.ToLower().StartsWith("sequest") Then
                                ' Sequest
                                fileSpecListCurrent = New List(Of String) From {
                                    "sequest:_syn.txt",
                                    "sequest:_syn_MSGF.txt",
                                    "sequest:_syn_ModSummary.txt",
                                    "sequest:_dta.zip"}

                            End If

                            If dataPkgJob.Value.Tool.ToLower().StartsWith("xtandem") Then
                                ' XTandem
                                fileSpecListCurrent = New List(Of String) From {
                                    "xtandem:_xt_syn.txt",
                                    "xtandem:_xt_syn_ModSummary.txt",
                                    "xtandem:_dta.zip"}

                            End If

                    End Select
                End If

                If fileSpecListCurrent.Count = 0 Then
                    Continue For
                End If

                Dim spectraFileKey = "Job" & dataPkgJob.Key & DATA_PACKAGE_SPECTRA_FILE_SUFFIX

                For Each fileSpec As String In fileSpecListCurrent
                    Dim fileSpecTerms = fileSpec.Trim().Split(":"c).ToList()
                    Dim sourceFileName = dataPkgJob.Value.Dataset & fileSpecTerms(1).Trim()
                    Dim sourceFolderPath = "??"

                    Dim saveMode = "nocopy"
                    If fileSpecTerms.Count > 2 Then
                        saveMode = fileSpecTerms(2).Trim()
                    End If

                    Try

                        If Not dataPkgJob.Value.Tool.ToLower().StartsWith(fileSpecTerms(0).Trim().ToLower()) Then
                            Continue For
                        End If

                        ' To avoid collisions, files for this job will be placed in a subfolder based on the Job number
                        Dim diTargetFolder = New DirectoryInfo(Path.Combine(m_WorkingDir, "Job" & dataPkgJob.Key))
                        If Not diTargetFolder.Exists Then diTargetFolder.Create()

                        If sourceFileName.ToLower().EndsWith("_dta.zip") AndAlso dataPkgJob.Value.Tool.ToLower().EndsWith("_mzml") Then
                            ' This is a .mzML job; it is not going to have a _dta.zip file
                            ' Setting sourceFolderPath to an empty string so that GetMzMLFile will get called below
                            sourceFolderPath = String.Empty
                        Else
                            sourceFolderPath = FindDataFile(sourceFileName)

                            If String.IsNullOrEmpty(sourceFolderPath) Then
                                ' Source file not found

                                Dim alternateSourceFileName As String = String.Empty

                                If sourceFileName.ToLower().Contains("_msgfdb") Then
                                    ' Auto-look for the _msgfplus version of this file
                                    alternateSourceFileName = clsGlobal.ReplaceIgnoreCase(sourceFileName, "_msgfdb", "_msgfplus")
                                ElseIf sourceFileName.ToLower().Contains("_msgfplus") Then
                                    ' Auto-look for the _msgfdb version of this file
                                    alternateSourceFileName = clsGlobal.ReplaceIgnoreCase(sourceFileName, "_msgfplus", "_msgfdb")
                                End If

                                If Not String.IsNullOrEmpty(alternateSourceFileName) Then
                                    sourceFolderPath = FindDataFile(alternateSourceFileName)
                                    If Not String.IsNullOrEmpty(sourceFolderPath) Then
                                        sourceFileName = alternateSourceFileName
                                    End If
                                End If
                            End If

                        End If

                        If String.IsNullOrEmpty(sourceFolderPath) Then

                            If sourceFileName.ToLower().EndsWith("_dta.zip") Then
                                ' Look for a mzML.gz file instead

                                Dim errorMessage As String = String.Empty
                                Dim fileMissingFromCache As Boolean

                                Dim success = RetrieveCachedMSXMLFile(DOT_MZML_EXTENSION, False, errorMessage, fileMissingFromCache)

                                If Not success Then
                                    If String.IsNullOrWhiteSpace(errorMessage) Then
                                        errorMessage = "Unknown error looking for the .mzML file for " & dataPkgJob.Value.Dataset & ", job " & dataPkgJob.Key
                                    End If

                                    LogError(errorMessage)
                                    Return False
                                End If

                                sourceFileName = dataPkgJob.Value.Dataset & DOT_MZML_EXTENSION & DOT_GZ_EXTENSION
                                m_jobParams.AddAdditionalParameter("DataPackageMetadata", spectraFileKey, sourceFileName)
                                m_jobParams.AddResultFileExtensionToSkip(DOT_GZ_EXTENSION)

                                MoveFileToFolder(diWorkingDirectory, diTargetFolder, sourceFileName)

                                If m_DebugLevel >= 1 Then
                                    ReportStatus("Retrieved the .mzML file for " & dataPkgJob.Value.Dataset & ", job " & dataPkgJob.Key)
                                End If

                                Continue For
                            End If

                            m_message = "Could not find a valid folder with file " & sourceFileName & " for job " & dataPkgJob.Key
                            If m_DebugLevel >= 1 Then
                                ReportStatus(m_message, 0, True)
                            End If
                            Return False
                        End If

                        If Not CopyFileToWorkDir(sourceFileName, sourceFolderPath, m_WorkingDir, clsLogTools.LogLevels.ERROR) Then
                            m_message = "CopyFileToWorkDir returned False for " & sourceFileName & " using folder " & sourceFolderPath & " for job " & dataPkgJob.Key
                            If m_DebugLevel >= 1 Then
                                ReportStatus(m_message, 0, True)
                            End If
                            Return False
                        End If

                        If sourceFileName.EndsWith("_dta.zip") Then
                            m_jobParams.AddAdditionalParameter("DataPackageMetadata", spectraFileKey, sourceFileName)
                        End If

                        If saveMode.ToLower() <> "copy" Then
                            m_jobParams.AddResultFileToSkip(sourceFileName)
                        End If

                        MoveFileToFolder(diWorkingDirectory, diTargetFolder, sourceFileName)

                        If m_DebugLevel >= 1 Then
                            ReportStatus("Copied " + sourceFileName + " from folder " + sourceFolderPath)
                        End If

                    Catch ex As Exception
                        ReportStatus("RetrieveAggregateFiles; Exception during copy of file: " &
                                      sourceFileName & " from folder " & sourceFolderPath & " for job " & dataPkgJob.Key, ex)
                        Return False

                    End Try

                Next
            Next

            If Not m_MyEMSLUtilities.ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders) Then
                Return False
            End If

            ' Restore the dataset and job info for this aggregation job
            OverrideCurrentDatasetAndJobInfo(currentDatasetAndJobInfo)

            blnSuccess = True

        Catch ex As Exception
            ReportStatus("Exception in RetrieveAggregateFiles", ex)
            blnSuccess = False
        End Try

        Return blnSuccess

    End Function

    ''' <summary>
    ''' Retrieve the dataset's cached .mzML file from the MsXML Cache
    ''' </summary>
    ''' <param name="unzip">True to unzip; otherwise, will remain as a .gzip file</param>
    ''' <param name="errorMessage">Output parameter: Error message</param>
    ''' <param name="fileMissingFromCache">Output parameter: will be True if the file was not found in the cache</param>
    ''' <returns>True if success, false if an error or file not found</returns>
    ''' <remarks>
    ''' Uses the jobs InputFolderName parameter to dictate which subfolder to search at \\Proto-11\MSXML_Cache
    ''' InputFolderName should be in the form MSXML_Gen_1_93_367204
    ''' </remarks>
    Protected Function RetrieveCachedMzMLFile(unzip As Boolean, <Out()> ByRef errorMessage As String, <Out()> ByRef fileMissingFromCache As Boolean) As Boolean
        Return RetrieveCachedMSXMLFile(DOT_MZML_EXTENSION, unzip, errorMessage, fileMissingFromCache)
    End Function


    ''' <summary>
    ''' Retrieve the dataset's cached .PBF file from the MsXML Cache
    ''' </summary>
    ''' <param name="errorMessage">Output parameter: Error message</param>
    ''' <param name="fileMissingFromCache">Output parameter: will be True if the file was not found in the cache</param>
    ''' <returns>True if success, false if an error or file not found</returns>
    ''' <remarks>
    ''' Uses the jobs InputFolderName parameter to dictate which subfolder to search at \\Proto-11\MSXML_Cache
    ''' InputFolderName should be in the form MSXML_Gen_1_93_367204
    ''' </remarks>
    Protected Function RetrieveCachedPBFFile(<Out()> ByRef errorMessage As String, <Out()> ByRef fileMissingFromCache As Boolean) As Boolean
        Const unzip = False
        Return RetrieveCachedMSXMLFile(DOT_PBF_EXTENSION, unzip, errorMessage, fileMissingFromCache)
    End Function


    ''' <summary>
    ''' Retrieve the dataset's cached .mzXML file from the MsXML Cache
    ''' </summary>
    ''' <param name="unzip">True to unzip; otherwise, will remain as a .gzip file</param>
    ''' <param name="errorMessage">Output parameter: Error message</param>
    ''' <param name="fileMissingFromCache">Output parameter: will be True if the file was not found in the cache</param>
    ''' <returns>True if success, false if an error or file not found</returns>
    ''' <remarks>
    ''' Uses the jobs InputFolderName parameter to dictate which subfolder to search at \\Proto-11\MSXML_Cache
    ''' InputFolderName should be in the form MSXML_Gen_1_105_367204
    ''' </remarks>
    Protected Function RetrieveCachedMzXMLFile(unzip As Boolean, <Out()> ByRef errorMessage As String, <Out()> ByRef fileMissingFromCache As Boolean) As Boolean
        Return RetrieveCachedMSXMLFile(DOT_MZXML_EXTENSION, unzip, errorMessage, fileMissingFromCache)
    End Function

    ''' <summary>
    ''' Retrieve the dataset's cached .mzXML or .mzML file from the MsXML Cache (assumes the file is gzipped)
    ''' </summary>
    ''' <param name="resultFileExtension">File extension to retrieve (.mzXML or .mzML)</param>
    ''' <param name="unzip">True to unzip; otherwise, will remain as a .gzip file</param>
    ''' <param name="errorMessage">Output parameter: Error message</param>
    ''' <param name="fileMissingFromCache">Output parameter: will be True if the file was not found in the cache</param>
    ''' <returns>True if success, false if an error or file not found</returns>
    ''' <remarks>
    ''' Uses the job's InputFolderName parameter to dictate which subfolder to search at \\Proto-11\MSXML_Cache
    ''' InputFolderName should be in the form MSXML_Gen_1_93_367204
    ''' </remarks>
    Protected Function RetrieveCachedMSXMLFile(
      resultFileExtension As String,
      unzip As Boolean,
      <Out()> ByRef errorMessage As String,
      <Out()> ByRef fileMissingFromCache As Boolean) As Boolean

        Dim msXMLCacheFolderPath As String = m_mgrParams.GetParam("MSXMLCacheFolderPath", String.Empty)
        Dim diMSXmlCacheFolder = New DirectoryInfo(msXMLCacheFolderPath)

        errorMessage = String.Empty
        fileMissingFromCache = False

        If String.IsNullOrEmpty(resultFileExtension) Then
            errorMessage = "resultFileExtension is empty; should be .mzXML or .mzML"
            Return False
        End If

        If Not diMSXmlCacheFolder.Exists Then
            errorMessage = "MSXmlCache folder not found: " & msXMLCacheFolderPath
            Return False
        End If

        Dim foldersToSearch = New List(Of String)
        foldersToSearch.Add(m_jobParams.GetJobParameter("InputFolderName", String.Empty))
        If foldersToSearch(0).Length = 0 Then
            foldersToSearch.Clear()
        End If

        For Each sharedResultFolder In GetSharedResultFolderList()
            If sharedResultFolder.Trim().Length = 0 Then Continue For
            If Not foldersToSearch.Contains(sharedResultFolder) Then
                foldersToSearch.Add(sharedResultFolder)
            End If
        Next

        If foldersToSearch.Count = 0 Then
            errorMessage = "Job parameters InputFolderName and SharedResultsFolders are empty; cannot retrieve the " & resultFileExtension & " file"
            Return False
        End If

        Dim msXmlToolNameVersionFolders As New List(Of String)

        For Each folderName In foldersToSearch
            Try
                Dim msXmlToolNameVersionFolder = GetMSXmlToolNameVersionFolder(folderName)
                msXmlToolNameVersionFolders.Add(msXmlToolNameVersionFolder)
            Catch ex As Exception
                errorMessage = "InputFolderName is not in the expected form of ToolName_Version_DatasetID (" & folderName & "); " &
                    "cannot retrieve the " & resultFileExtension & " File"
                ReportStatus(errorMessage, 0, True)
            End Try
        Next

        If msXmlToolNameVersionFolders.Count = 0 Then
            If String.IsNullOrEmpty(errorMessage) Then
                errorMessage = "The input folder and shared results folder(s) were not in the expected form of ToolName_Version_DatasetID"
                ReportStatus(errorMessage, 0, True)
            End If
            Return False
        Else
            errorMessage = String.Empty
        End If

        Dim diSourceFolder As DirectoryInfo = Nothing

        For Each toolNameVersionFolder In msXmlToolNameVersionFolders
            Dim sourceFolder = GetMSXmlCacheFolderPath(diMSXmlCacheFolder.FullName, m_jobParams, toolNameVersionFolder, errorMessage)
            If Not String.IsNullOrEmpty(errorMessage) Then
                Continue For
            End If

            diSourceFolder = New DirectoryInfo(sourceFolder)
            If diSourceFolder.Exists Then
                Exit For
            End If

            If String.IsNullOrEmpty(errorMessage) Then
                errorMessage = "Cache folder does not exist (" & sourceFolder
            Else
                errorMessage &= " or " & sourceFolder
            End If

        Next

        If diSourceFolder Is Nothing Then
            errorMessage &= "); will re-generate the " & resultFileExtension & " file"
            ReportStatus(errorMessage)
            fileMissingFromCache = True
            Return False
        End If

        Dim sourceFilePath = Path.Combine(diSourceFolder.FullName, m_DatasetName & resultFileExtension)
        Dim expectedFileDescription = resultFileExtension
        If resultFileExtension <> DOT_PBF_EXTENSION Then
            sourceFilePath &= DOT_GZ_EXTENSION
            expectedFileDescription &= DOT_GZ_EXTENSION
        End If

        Dim fiSourceFile = New FileInfo(sourceFilePath)
        If Not fiSourceFile.Exists Then
            errorMessage = "Cached " & expectedFileDescription & " file does not exist in " & diSourceFolder.FullName & "; will re-generate it"
            fileMissingFromCache = True
            Return False
        End If

        ' Match found; confirm that it has a .hashcheck file and that the information in the .hashcheck file matches the file

        Dim hashcheckFilePath = fiSourceFile.FullName & clsGlobal.SERVER_CACHE_HASHCHECK_FILE_SUFFIX

        errorMessage = String.Empty
        If Not clsGlobal.ValidateFileVsHashcheck(fiSourceFile.FullName, hashcheckFilePath, errorMessage) Then
            errorMessage = "Cached " & resultFileExtension & " file does not match the hashcheck file in " & diSourceFolder.FullName & "; will re-generate it"
            fileMissingFromCache = True
            Return False
        End If

        If Not CopyFileToWorkDir(fiSourceFile.Name, fiSourceFile.Directory.FullName, m_WorkingDir, clsLogTools.LogLevels.ERROR) Then
            errorMessage = "Error copying " + fiSourceFile.Name
            Return False
        End If

        If fiSourceFile.Extension.ToLower() = DOT_GZ_EXTENSION Then
            ' Do not skip all .gz files because we compress MSGF+ results using .gz and we want to keep those

            m_jobParams.AddResultFileToSkip(fiSourceFile.Name)
            m_jobParams.AddResultFileToSkip(fiSourceFile.Name.Substring(0, fiSourceFile.Name.Length - DOT_GZ_EXTENSION.Length))

            If unzip Then
                Dim localZippedFile = Path.Combine(m_WorkingDir, fiSourceFile.Name)

                If Not m_IonicZipTools.GUnzipFile(localZippedFile) Then
                    errorMessage = m_IonicZipTools.Message
                    Return False
                End If
            End If

        End If

        Return True

    End Function

    ''' <summary>
    ''' Lookup the Peptide Hit jobs associated with the current job
    ''' </summary>
    ''' <param name="DataPackageID">Data package ID</param>
    ''' <returns>Peptide Hit Jobs (e.g. MSGF+ or Sequest)</returns>
    ''' <remarks></remarks>
    Protected Function RetrieveDataPackagePeptideHitJobInfo(<Out()> ByRef DataPackageID As Integer) As List(Of clsDataPackageJobInfo)

        Dim lstAdditionalJobs = New List(Of clsDataPackageJobInfo)
        Return RetrieveDataPackagePeptideHitJobInfo(DataPackageID, lstAdditionalJobs)

    End Function

    ''' <summary>
    ''' Lookup the Peptide Hit jobs associated with the current job
    ''' </summary>
    ''' <param name="DataPackageID">Data package ID</param>
    ''' <param name="lstAdditionalJobs">Non Peptide Hit jobs (e.g. DeconTools or MASIC)</param>
    ''' <returns>Peptide Hit Jobs (e.g. MSGF+ or Sequest)</returns>
    ''' <remarks></remarks>
    Protected Function RetrieveDataPackagePeptideHitJobInfo(
      <Out()> ByRef DataPackageID As Integer,
      <Out()> ByRef lstAdditionalJobs As List(Of clsDataPackageJobInfo)) As List(Of clsDataPackageJobInfo)

        ' Gigasax.DMS_Pipeline
        Dim connectionString As String = m_mgrParams.GetParam("brokerconnectionstring")

        DataPackageID = m_jobParams.GetJobParameter("DataPackageID", -1)

        If DataPackageID < 0 Then
            LogError("DataPackageID is not defined for this analysis job")
            lstAdditionalJobs = New List(Of clsDataPackageJobInfo)
            Return New List(Of clsDataPackageJobInfo)
        Else
            Dim errorMsg As String = String.Empty
            Dim lstDataPackagePeptideHitJobs = RetrieveDataPackagePeptideHitJobInfo(connectionString, DataPackageID, lstAdditionalJobs, errorMsg)

            If Not String.IsNullOrWhiteSpace(errorMsg) Then
                LogError(errorMsg)
            End If

            Return lstDataPackagePeptideHitJobs
        End If

    End Function

    ''' <summary>
    ''' Lookup the Peptide Hit jobs associated with the current job
    ''' </summary>
    ''' <param name="connectionString">Connection string to the DMS_Pipeline database</param>
    ''' <param name="dataPackageID">Data package ID</param>
    ''' <param name="errorMsg">Output: error message</param>
    ''' <returns>Peptide Hit Jobs (e.g. MSGF+ or Sequest)</returns>
    ''' <remarks></remarks>
    Public Shared Function RetrieveDataPackagePeptideHitJobInfo(
       connectionString As String,
       dataPackageID As Integer,
       <Out()> errorMsg As String) As List(Of clsDataPackageJobInfo)

        Dim lstAdditionalJobs = New List(Of clsDataPackageJobInfo)
        Return RetrieveDataPackagePeptideHitJobInfo(connectionString, dataPackageID, lstAdditionalJobs, errorMsg)

    End Function

    ''' <summary>
    ''' Lookup the Peptide Hit jobs associated with the current job
    ''' </summary>
    ''' <param name="connectionString">Connection string to the DMS_Pipeline database</param>
    ''' <param name="dataPackageID">Data package ID</param>
    ''' <param name="lstAdditionalJobs">Output: Non Peptide Hit jobs (e.g. DeconTools or MASIC)</param>
    ''' <param name="errorMsg">Output: error message</param>
    ''' <returns>Peptide Hit Jobs (e.g. MSGF+ or Sequest)</returns>
    ''' <remarks></remarks>
    Public Shared Function RetrieveDataPackagePeptideHitJobInfo(
      connectionString As String,
      dataPackageID As Integer,
      <Out()> ByRef lstAdditionalJobs As List(Of clsDataPackageJobInfo),
      <Out()> ByRef errorMsg As String) As List(Of clsDataPackageJobInfo)

        Dim lstDataPackagePeptideHitJobs As List(Of clsDataPackageJobInfo)
        Dim dctDataPackageJobs As Dictionary(Of Integer, clsDataPackageJobInfo)

        ' This list tracks the info for the Peptide Hit jobs (e.g. MSGF+ or Sequest) associated with this aggregation job's data package
        lstDataPackagePeptideHitJobs = New List(Of clsDataPackageJobInfo)
        errorMsg = String.Empty

        ' This list tracks the info for the non Peptide Hit jobs (e.g. DeconTools or MASIC) associated with this aggregation job's data package
        lstAdditionalJobs = New List(Of clsDataPackageJobInfo)

        ' This dictionary will track the jobs associated with this aggregation job's data package
        ' Key is job number, value is an instance of clsDataPackageJobInfo
        dctDataPackageJobs = New Dictionary(Of Integer, clsDataPackageJobInfo)

        Try
            If Not LoadDataPackageJobInfo(connectionString, dataPackageID, dctDataPackageJobs) Then
                errorMsg = "Error looking up datasets and jobs using LoadDataPackageJobInfo"
                Return lstDataPackagePeptideHitJobs
            End If
        Catch ex As Exception
            errorMsg = "Exception calling LoadDataPackageJobInfo: " & ex.Message
            Return lstDataPackagePeptideHitJobs
        End Try

        Try
            For Each kvItem As KeyValuePair(Of Integer, clsDataPackageJobInfo) In dctDataPackageJobs

                If kvItem.Value.PeptideHitResultType = clsPHRPReader.ePeptideHitResultType.Unknown Then
                    lstAdditionalJobs.Add(kvItem.Value)
                Else
                    ' Cache this job info in lstDataPackagePeptideHitJobs
                    lstDataPackagePeptideHitJobs.Add(kvItem.Value)
                End If

            Next

        Catch ex As Exception
            errorMsg = "Exception determining data package jobs for this aggregation job (RetrieveDataPackagePeptideHitJobInfo): " & ex.Message
        End Try

        Try
            ' Look for any SplitFasta jobs
            ' If present, we need to determine the value for job parameter NumberOfClonedSteps

            Dim splitFastaJobs = (From dataPkgJob In lstDataPackagePeptideHitJobs
                                  Where dataPkgJob.Tool.ToLower().Contains("splitfasta")
                                  Select dataPkgJob).ToList()

            If splitFastaJobs.Count > 0 Then

                Dim lastStatusTime = Date.UtcNow
                Dim statusIntervalSeconds = 4
                Dim jobsProcessed = 0

                Using brokerConnection = New SqlConnection(connectionString)
                    brokerConnection.Open()

                    For Each dataPkgJob In splitFastaJobs

                        Dim dataPkgJobParameters As Dictionary(Of String, String) = Nothing

                        Dim success = LookupJobParametersFromHistory(brokerConnection, dataPkgJob.Job, dataPkgJobParameters, errorMsg)

                        If Not success Then
                            Return New List(Of clsDataPackageJobInfo)
                        End If

                        Dim numberOfClonedSteps As String = Nothing
                        If dataPkgJobParameters.TryGetValue("NumberOfClonedSteps", numberOfClonedSteps) Then
                            Dim clonedStepCount = CInt(numberOfClonedSteps)
                            dataPkgJob.NumberOfClonedSteps = clonedStepCount
                        End If

                        jobsProcessed += 1

                        If Date.UtcNow.Subtract(lastStatusTime).TotalSeconds >= statusIntervalSeconds Then
                            Dim pctComplete = jobsProcessed / splitFastaJobs.Count * 100
                            LogDebugMessage("Retrieving job parameters from history for SplitFasta jobs; " & pctComplete.ToString("0") & "% complete", Nothing)

                            lastStatusTime = Date.UtcNow

                            ' Double the status interval, allowing for a maximum of 30 seconds
                            statusIntervalSeconds = Math.Min(30, statusIntervalSeconds * 2)

                        End If
                    Next
                End Using

            End If


        Catch ex As Exception
            errorMsg = "Exception calling LookupJobParametersFromHistory (RetrieveDataPackagePeptideHitJobInfo): " & ex.Message
            Return New List(Of clsDataPackageJobInfo)
        End Try

        Return lstDataPackagePeptideHitJobs

    End Function

    ''' <summary>
    ''' Retrieves the PHRP files for the PeptideHit jobs defined for the data package associated with this aggregation job
    ''' Also creates a batch file that can be manually run to retrieve the instrument data files
    ''' </summary>
    ''' <param name="udtOptions">File retrieval options</param>
    ''' <param name="lstDataPackagePeptideHitJobs">Job info for the peptide_hit jobs associated with this data package (output parameter)</param>
    ''' <returns>True if success, false if an error</returns>
    ''' <remarks></remarks>
    Protected Function RetrieveDataPackagePeptideHitJobPHRPFiles(
      udtOptions As udtDataPackageRetrievalOptionsType,
      ByRef lstDataPackagePeptideHitJobs As List(Of clsDataPackageJobInfo)) As Boolean

        Const progressPercentAtStart As Single = 0
        Const progressPercentAtFinish As Single = 20
        Return RetrieveDataPackagePeptideHitJobPHRPFiles(udtOptions, lstDataPackagePeptideHitJobs, progressPercentAtStart, progressPercentAtFinish)
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
    Protected Function RetrieveDataPackagePeptideHitJobPHRPFiles(
      udtOptions As udtDataPackageRetrievalOptionsType,
      <Out()> ByRef lstDataPackagePeptideHitJobs As List(Of clsDataPackageJobInfo),
      progressPercentAtStart As Single,
      progressPercentAtFinish As Single) As Boolean

        Dim sourceFolderPath = "??"
        Dim sourceFilename = "??"
        Dim dataPackageID = 0

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

        Try
            Dim success = LoadDataPackageDatasetInfo(dctDataPackageDatasets)

            If Not success OrElse dctDataPackageDatasets.Count = 0 Then
                If String.IsNullOrEmpty(m_message) Then
                    m_message = "Did not find any datasets associated with this job's data package ID (" & dataPackageID & ")"
                End If
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, m_message)
                Console.WriteLine(m_message)
                Return False
            End If

        Catch ex As Exception
            LogError("Exception calling LoadDataPackageDatasetInfo", ex)
            Return False
        End Try

        ' The keys in this dictionary are data package job info entries; the values KeyValuePairs of path to the .mzXML file and path to the .hashcheck file (if any)
        ' The KeyValuePair will have empty strings if the .Raw file needs to be retrieved
        dctInstrumentDataToRetrieve = New Dictionary(Of clsDataPackageJobInfo, KeyValuePair(Of String, String))

        ' This list tracks analysis jobs that are not PeptideHit jobs
        Dim lstAdditionalJobs As List(Of clsDataPackageJobInfo) = Nothing

        Try
            lstDataPackagePeptideHitJobs = RetrieveDataPackagePeptideHitJobInfo(dataPackageID, lstAdditionalJobs)

            If lstDataPackagePeptideHitJobs.Count = 0 Then
                ' Did not find any peptide hit jobs associated with this job's data package ID
                ' This is atypical, but is allowed
            End If

        Catch ex As Exception
            ReportStatus("Exception calling RetrieveDataPackagePeptideHitJobInfo", ex)
            Return False
        End Try

        Try

            ' Make sure the MyEMSL download queue is empty
            If m_MyEMSLUtilities.FilesToDownload.Count > 0 Then
                If Not m_MyEMSLUtilities.ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders) Then
                    Return False
                End If
            End If

            ' Cache the current dataset and job info
            Dim currentDatasetAndJobInfo = GetCurrentDatasetAndJobInfo()

            Dim intJobsProcessed = 0

            For Each dataPkgJob As clsDataPackageJobInfo In lstDataPackagePeptideHitJobs

                If Not OverrideCurrentDatasetAndJobInfo(dataPkgJob) Then
                    ' Error message has already been logged
                    Return False
                End If

                If dataPkgJob.PeptideHitResultType = clsPHRPReader.ePeptideHitResultType.Unknown Then
                    Dim msg = "PeptideHit ResultType not recognized for job " & dataPkgJob.Job & ": " & dataPkgJob.ResultType.ToString()
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, msg)
                    Console.WriteLine(msg)
                Else

                    ' Keys in this list are filenames; values are True if the file is required and False if not required
                    Dim lstFilesToGet = New SortedList(Of String, Boolean)
                    Dim LocalFolderPath As String
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

                    If udtOptions.RetrieveMZidFiles AndAlso dataPkgJob.PeptideHitResultType = clsPHRPReader.ePeptideHitResultType.MSGFDB Then
                        ' Retrieve MSGF+ .mzID files
                        ' They will either be stored as .zip files or as .gz files

                        If dataPkgJob.NumberOfClonedSteps > 0 Then
                            For splitFastaResultID = 1 To dataPkgJob.NumberOfClonedSteps
                                AddMzIdFilesToFind(m_DatasetName, splitFastaResultID, zipFileCandidates, gzipFileCandidates, lstFilesToGet)
                            Next
                        Else
                            AddMzIdFilesToFind(m_DatasetName, 0, zipFileCandidates, gzipFileCandidates, lstFilesToGet)
                        End If

                    End If

                    If udtOptions.RetrievePepXMLFiles AndAlso dataPkgJob.PeptideHitResultType <> clsPHRPReader.ePeptideHitResultType.Unknown Then
                        ' Retrieve .pepXML files, which are stored as _pepXML.zip files
                        zippedPepXmlFile = m_DatasetName & "_pepXML.zip"
                        lstFilesToGet.Add(zippedPepXmlFile, False)
                    End If

                    sourceFolderPath = String.Empty

                    ' Check whether a synopsis file by this name has already been copied locally
                    ' If it has, then we have multiple jobs for the same dataset with the same analysis tool, and we'll thus need to add a prefix to each filename
                    If Not udtOptions.CreateJobPathFiles AndAlso File.Exists(Path.Combine(m_WorkingDir, strSynopsisFileName)) Then
                        blnPrefixRequired = True

                        LocalFolderPath = Path.Combine(m_WorkingDir, "FileRename")
                        If Not Directory.Exists(LocalFolderPath) Then
                            Directory.CreateDirectory(LocalFolderPath)
                        End If

                    Else
                        blnPrefixRequired = False
                        LocalFolderPath = String.Copy(m_WorkingDir)
                    End If

                    Dim swJobInfoFile As StreamWriter = Nothing
                    If udtOptions.CreateJobPathFiles Then
                        Dim strJobInfoFilePath As String = GetJobInfoFilePath(dataPkgJob.Job)
                        swJobInfoFile = New StreamWriter(New FileStream(strJobInfoFilePath, FileMode.Append, FileAccess.Write, FileShare.Read))
                    End If

                    For Each sourceFile In lstFilesToGet

                        sourceFilename = sourceFile.Key
                        Dim fileRequired = sourceFile.Value

                        ' Typically only use FindDataFile() for the first file in lstFilesToGet; we will assume the other files are in that folder
                        ' However, if the file resides in MyEMSL then we need to call FindDataFile for every new file because FindDataFile will append the MyEMSL File ID for each file
                        If String.IsNullOrEmpty(sourceFolderPath) OrElse sourceFolderPath.StartsWith(MYEMSL_PATH_FLAG) Then
                            sourceFolderPath = FindDataFile(sourceFilename)

                            If String.IsNullOrEmpty(sourceFolderPath) Then
                                Dim alternateFileName As String = clsPHRPReader.AutoSwitchToLegacyMSGFDBIfRequired(sourceFilename, "Dataset_msgfdb.txt")
                                sourceFolderPath = FindDataFile(alternateFileName)
                            End If
                        End If

                        If Not fileRequired Then
                            ' It's OK if this file doesn't exist, we'll just log a debug message
                            eLogMsgTypeIfNotFound = clsLogTools.LogLevels.DEBUG
                        Else
                            ' This file must exist; log an error if it's not found
                            eLogMsgTypeIfNotFound = clsLogTools.LogLevels.ERROR
                        End If

                        If udtOptions.CreateJobPathFiles And Not sourceFolderPath.StartsWith(MYEMSL_PATH_FLAG) Then
                            Dim sourceFilePath As String = Path.Combine(sourceFolderPath, sourceFilename)
                            Dim alternateFileNamelternateFileName As String = clsPHRPReader.AutoSwitchToLegacyMSGFDBIfRequired(sourceFilePath, "Dataset_msgfdb.txt")

                            If File.Exists(sourceFilePath) Then
                                swJobInfoFile.WriteLine(sourceFilePath)
                            ElseIf File.Exists(alternateFileNamelternateFileName) Then
                                swJobInfoFile.WriteLine(alternateFileNamelternateFileName)
                            Else
                                If eLogMsgTypeIfNotFound <> clsLogTools.LogLevels.DEBUG Then
                                    m_message = "Required PHRP file not found: " & sourceFilename
                                    If sourceFilename.ToLower().EndsWith("_msgfplus.zip") Or sourceFilename.ToLower().EndsWith("_msgfplus.mzid.gz") Then
                                        m_message &= "; Confirm job used MSGF+ and not MSGFDB"
                                    End If
                                    If m_DebugLevel >= 1 Then
                                        ReportStatus("Required PHRP file not found: " & sourceFilePath, 0, True)
                                    End If
                                    Return False
                                End If
                            End If

                        Else
                            ' Note for files in MyEMSL, this call will simply add the file to the download queue; use ProcessMyEMSLDownloadQueue() to retrieve the file
                            blnFileCopied = CopyFileToWorkDir(sourceFilename, sourceFolderPath, LocalFolderPath, eLogMsgTypeIfNotFound)

                            If Not blnFileCopied Then
                                Dim alternateFileName As String = clsPHRPReader.AutoSwitchToLegacyMSGFDBIfRequired(sourceFilename, "Dataset_msgfdb.txt")
                                blnFileCopied = CopyFileToWorkDir(alternateFileName, sourceFolderPath, LocalFolderPath, eLogMsgTypeIfNotFound)
                                If blnFileCopied Then
                                    sourceFilename = alternateFileName
                                End If
                            End If

                            If Not blnFileCopied Then

                                If eLogMsgTypeIfNotFound <> clsLogTools.LogLevels.DEBUG Then
                                    LogError("CopyFileToWorkDir returned False for " + sourceFilename + " using folder " + sourceFolderPath)
                                    Return False
                                End If

                            Else
                                If m_DebugLevel > 1 Then
                                    ReportStatus("Copied " + sourceFilename + " from folder " + sourceFolderPath)
                                End If

                                If blnPrefixRequired Then
                                    lstPendingFileRenames.Add(sourceFilename)
                                Else
                                    m_jobParams.AddResultFileToSkip(sourceFilename)
                                End If
                            End If
                        End If

                    Next sourceFile     ' in lstFilesToGet

                    If m_MyEMSLUtilities.FilesToDownload.Count > 0 Then
                        ' Some of the files were found in MyEMSL; download them now
                        If Not m_MyEMSLUtilities.ProcessMyEMSLDownloadQueue(LocalFolderPath, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders) Then
                            Return False
                        End If
                    End If

                    ' Now perform any required file renames
                    For Each sourceFilename In lstPendingFileRenames
                        If Not RenameDuplicatePHRPFile(LocalFolderPath, sourceFilename, m_WorkingDir, "Job" & dataPkgJob.Job.ToString() & "_", dataPkgJob.Job) Then
                            Return False
                        End If
                    Next

                    If udtOptions.RetrieveDTAFiles Then
                        If udtOptions.CreateJobPathFiles Then
                            ' Find the CDTA file
                            Dim strErrorMessage As String = String.Empty
                            Dim sourceConcatenatedDTAFilePath As String = FindCDTAFile(strErrorMessage)

                            If String.IsNullOrEmpty(sourceConcatenatedDTAFilePath) Then
                                LogError(strErrorMessage)
                                Return False
                            Else
                                swJobInfoFile.WriteLine(sourceConcatenatedDTAFilePath)
                            End If
                        Else
                            If Not RetrieveDtaFiles() Then
                                'Errors were reported in function call, so just return
                                Return False
                            End If
                        End If
                    End If

                    If udtOptions.CreateJobPathFiles Then
                        swJobInfoFile.Close()
                    Else
                        ' Unzip any mzid files that were found
                        If zipFileCandidates.Count > 0 OrElse gzipFileCandidates.Count > 0 Then

                            Dim matchFound = False
                            For Each gzipCandidate In gzipFileCandidates
                                Dim fiFileToUnzip = New FileInfo(Path.Combine(m_WorkingDir, gzipCandidate))
                                If fiFileToUnzip.Exists Then
                                    m_IonicZipTools.GUnzipFile(fiFileToUnzip.FullName)
                                    matchFound = True
                                    Exit For
                                End If
                            Next

                            If Not matchFound Then
                                For Each zipCandidate In zipFileCandidates
                                    Dim fiFileToUnzip = New FileInfo(Path.Combine(m_WorkingDir, zipCandidate))
                                    If fiFileToUnzip.Exists Then
                                        m_IonicZipTools.UnzipFile(fiFileToUnzip.FullName)
                                        matchFound = True
                                        Exit For
                                    End If
                                Next
                            End If

                            If Not matchFound Then
                                LogError("Could not find either the _msgfplus.zip file or the _msgfplus.mzid.gz file for dataset")
                                Return False
                            End If

                            If blnPrefixRequired Then
                                If Not RenameDuplicatePHRPFile(m_WorkingDir, m_DatasetName & "_msgfplus.mzid", m_WorkingDir, "Job" & dataPkgJob.Job.ToString() & "_", dataPkgJob.Job) Then
                                    Return False
                                End If
                            End If

                        End If

                        If Not String.IsNullOrWhiteSpace(zippedPepXmlFile) Then
                            ' Unzip _pepXML.zip if it exists
                            Dim fiFileToUnzip = New FileInfo(Path.Combine(m_WorkingDir, zippedPepXmlFile))
                            If fiFileToUnzip.Exists Then
                                m_IonicZipTools.UnzipFile(fiFileToUnzip.FullName)
                            End If
                        End If
                    End If

                End If

                ' Find the instrument data file or folder if a new dataset
                If Not dctRawFileRetrievalCommands.ContainsKey(dataPkgJob.DatasetID) Then
                    If Not RetrieveDataPackageInstrumentFile(dataPkgJob, udtOptions, dctRawFileRetrievalCommands, dctInstrumentDataToRetrieve, dctDatasetRawFilePaths) Then
                        Return False
                    End If
                End If

                intJobsProcessed += 1
                Dim sngProgress = clsAnalysisToolRunnerBase.ComputeIncrementalProgress(progressPercentAtStart, progressPercentAtFinish, intJobsProcessed, lstDataPackagePeptideHitJobs.Count + lstAdditionalJobs.Count)
                If Not m_StatusTools Is Nothing Then
                    m_StatusTools.CurrentOperation = "RetrieveDataPackagePeptideHitJobPHRPFiles (PeptideHit Jobs)"
                    m_StatusTools.UpdateAndWrite(sngProgress)
                End If

            Next     ' dataPkgJob in lstDataPackagePeptideHitJobs

            ' Now process the additional jobs to retrieve the instrument data for each one
            For Each dataPkgJob In lstAdditionalJobs

                ' Find the instrument data file or folder if a new dataset
                If Not dctRawFileRetrievalCommands.ContainsKey(dataPkgJob.DatasetID) Then
                    If Not RetrieveDataPackageInstrumentFile(dataPkgJob, udtOptions, dctRawFileRetrievalCommands, dctInstrumentDataToRetrieve, dctDatasetRawFilePaths) Then
                        Return False
                    End If
                End If

                intJobsProcessed += 1
                Dim sngProgress = clsAnalysisToolRunnerBase.ComputeIncrementalProgress(progressPercentAtStart, progressPercentAtFinish, intJobsProcessed, lstDataPackagePeptideHitJobs.Count + lstAdditionalJobs.Count)
                If Not m_StatusTools Is Nothing Then
                    m_StatusTools.CurrentOperation = "RetrieveDataPackagePeptideHitJobPHRPFiles (Additional Jobs)"
                    m_StatusTools.UpdateAndWrite(sngProgress)
                End If

            Next        ' in lstAdditionalJobs

            ' Look for any datasets that are associated with this data package yet have no jobs
            For Each datasetItem In dctDataPackageDatasets
                If Not dctRawFileRetrievalCommands.ContainsKey(datasetItem.Key) Then

                    Dim dataPkgJob = GetPseudoDataPackageJobInfo(datasetItem.Value)
                    dataPkgJob.ResultsFolderName = "Undefined_Folder"

                    If Not OverrideCurrentDatasetAndJobInfo(dataPkgJob) Then
                        ' Error message has already been logged
                        Return False
                    End If

                    If Not RetrieveDataPackageInstrumentFile(dataPkgJob, udtOptions, dctRawFileRetrievalCommands, dctInstrumentDataToRetrieve, dctDatasetRawFilePaths) Then
                        Return False
                    End If
                End If
            Next

            If dctRawFileRetrievalCommands.Count = 0 Then
                m_message = "Did not find any datasets associated with this job's data package ID (" & dataPackageID & ")"
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, m_message)
                Console.WriteLine(m_message)
                Return False
            End If

            ' Restore the dataset and job info for this aggregation job
            OverrideCurrentDatasetAndJobInfo(currentDatasetAndJobInfo)

            If dctRawFileRetrievalCommands.Count > 0 Then
                ' Create a batch file with commands for retrieve the dataset files
                Dim strBatchFilePath As String
                strBatchFilePath = Path.Combine(m_WorkingDir, "RetrieveInstrumentData.bat")
                Using swOutfile = New StreamWriter(New FileStream(strBatchFilePath, FileMode.Create, FileAccess.Write, FileShare.Read))
                    For Each item As String In dctRawFileRetrievalCommands.Values
                        swOutfile.WriteLine(item)
                    Next
                End Using

                ' Store the dataset paths in a Packed Job Parameter
                StorePackedJobParameterDictionary(dctDatasetRawFilePaths, JOB_PARAM_DICTIONARY_DATASET_FILE_PATHS)

            End If

            If udtOptions.RetrieveMzXMLFile Then
                ' All of the PHRP data files have been successfully retrieved; now retrieve the mzXML files or the .Raw files
                ' If udtOptions.CreateJobPathFiles = True then we will create StoragePathInfo files
                blnSuccess = RetrieveDataPackageMzXMLFiles(dctInstrumentDataToRetrieve, udtOptions)
            Else
                blnSuccess = True
            End If

        Catch ex As Exception
            ReportStatus("RetrieveDataPackagePeptideHitJobPHRPFiles; Exception during copy of file: " + sourceFilename + " from folder " + sourceFolderPath, ex)
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

            mzXMLFilePath = FindMZXmlFile(hashcheckFilePath)

            If String.IsNullOrEmpty(mzXMLFilePath) Then
                ' mzXML file not found
                If dataPkgJob.RawDataType = RAW_DATA_TYPE_DOT_RAW_FILES Then
                    ' Will need to retrieve the .Raw file for this dataset
                    dctInstrumentDataToRetrieve.Add(dataPkgJob, New KeyValuePair(Of String, String)(String.Empty, String.Empty))
                ElseIf udtOptions.RetrieveMzXMLFile Then
                    LogError("mzXML file not found for dataset " & dataPkgJob.Dataset &
                             " and dataset file type is not a .Raw file and we thus cannot auto-create the missing mzXML file")
                    Return False
                End If
            Else
                dctInstrumentDataToRetrieve.Add(dataPkgJob, New KeyValuePair(Of String, String)(mzXMLFilePath, hashcheckFilePath))
            End If
        End If

        Dim blnIsFolder = False
        Dim rawFilePath = FindDatasetFileOrFolder(blnIsFolder, udtOptions.AssumeInstrumentDataUnpurged)

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
    Protected Function RetrieveDataPackageMzXMLFiles(
      dctInstrumentDataToRetrieve As Dictionary(Of clsDataPackageJobInfo, KeyValuePair(Of String, String)),
      udtOptions As udtDataPackageRetrievalOptionsType) As Boolean

        Dim blnSuccess As Boolean
        Dim createStoragePathInfoOnly As Boolean

        Dim intCurrentJob = 0

        Try

            ' Make sure we don't move the .Raw, .mzXML, .mzML or .gz files into the results folder
            m_jobParams.AddResultFileExtensionToSkip(DOT_RAW_EXTENSION)             ' .Raw file
            m_jobParams.AddResultFileExtensionToSkip(DOT_MZXML_EXTENSION)           ' .mzXML file
            m_jobParams.AddResultFileExtensionToSkip(DOT_MZML_EXTENSION)            ' .mzML file
            m_jobParams.AddResultFileExtensionToSkip(DOT_GZ_EXTENSION)              ' .gz file

            If udtOptions.CreateJobPathFiles Then
                createStoragePathInfoOnly = True
            Else
                createStoragePathInfoOnly = False
            End If

            Dim lstDatasetsProcessed = New SortedSet(Of String)

            ' Cache the current dataset and job info
            Dim currentDatasetAndJobInfo = GetCurrentDatasetAndJobInfo()

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

                    If Not OverrideCurrentDatasetAndJobInfo(kvItem.Key) Then
                        ' Error message has already been logged
                        Return False
                    End If

                    If String.IsNullOrEmpty(strMzXMLFilePath) Then
                        ' The .mzXML or .mzML file was not found; we will need to obtain the .Raw file
                        blnSuccess = False
                    Else
                        ' mzXML or .mzML file exists; either retrieve it or create a StoragePathInfo file
                        blnSuccess = RetrieveMZXmlFileUsingSourceFile(createStoragePathInfoOnly, strMzXMLFilePath, strHashcheckFilePath)
                    End If

                    If blnSuccess Then
                        ' .mzXML or .mzML file found and copied locally
                        Dim msXmlFileExtension = String.Empty

                        If strMzXMLFilePath.ToLower().EndsWith(DOT_GZ_EXTENSION) Then
                            msXmlFileExtension = Path.GetExtension(strMzXMLFilePath.Substring(0, strMzXMLFilePath.Length - 3))
                        Else
                            msXmlFileExtension = Path.GetExtension(strMzXMLFilePath)
                        End If

                        If m_DebugLevel >= 1 Then
                            If udtOptions.CreateJobPathFiles Then
                                LogDebugMessage(msXmlFileExtension & " file found for job " & intCurrentJob & " at " & strMzXMLFilePath)
                            Else
                                LogDebugMessage("Copied " & msXmlFileExtension & " file for job " & intCurrentJob & " from " & strMzXMLFilePath)
                            End If

                        End If
                    Else
                        ' .mzXML file not found (or problem adding to the MyEMSL download queue)
                        ' Find or retrieve the .Raw file, which can be used to create the .mzXML file (the plugin will actually perform the work of converting the file; as an example, see the MSGF plugin)

                        If Not RetrieveSpectra(kvItem.Key.RawDataType, createStoragePathInfoOnly, maxAttempts:=1) Then
                            LogError("Error occurred retrieving instrument data file for job " & intCurrentJob)
                            Return False
                        End If

                    End If

                    lstDatasetsProcessed.Add(kvItem.Key.Dataset)
                End If

                datasetsProcessed += 1

                ' Compute a % complete value between 0 and 2%
                Dim percentComplete = CSng(datasetsProcessed) / datasetsToProcess * 2
                m_StatusTools.UpdateAndWrite(percentComplete)

                If (Date.UtcNow.Subtract(dtLastProgressUpdate).TotalSeconds >= 30) Then

                    dtLastProgressUpdate = Date.UtcNow

                    LogDebugMessage("Retrieving mzXML files: " & datasetsProcessed & " / " & datasetsToProcess & " datasets")
                End If

            Next kvItem

            ' Restore the dataset and job info for this aggregation job
            OverrideCurrentDatasetAndJobInfo(currentDatasetAndJobInfo)

            blnSuccess = True

        Catch ex As Exception
            ReportStatus("RetrieveDataPackageMzXMLFiles; Exception retrieving mzXML file or .Raw file for job " & intCurrentJob, ex)
            blnSuccess = False
        End Try

        Return blnSuccess

    End Function

    ''' <summary>
    ''' Retrieves file PNNLOmicsElementData.xml from the program directory of the program specified by strProgLocName
    ''' </summary>
    ''' <param name="strProgLocName"></param>
    ''' <returns></returns>
    ''' <remarks>strProgLocName is tyipcally DeconToolsProgLoc, LipidToolsProgLoc, or TargetedWorkflowsProgLoc</remarks>
    Protected Function RetrievePNNLOmicsResourceFiles(strProgLocName As String) As Boolean

        Const OMICS_ELEMENT_DATA_FILE = "PNNLOmicsElementData.xml"

        ' Copy the PNNLOmicsElementData.xml file to the working directory
        Dim strProgLoc As String
        Dim fiSourceFile As FileInfo

        Try
            strProgLoc = m_mgrParams.GetParam(strProgLocName)
            If String.IsNullOrEmpty(strProgLocName) Then
                LogError("Manager parameter " + strProgLocName + " is not defined; cannot retrieve file " & OMICS_ELEMENT_DATA_FILE)
                Return False
            End If

            fiSourceFile = New FileInfo(Path.Combine(strProgLoc, OMICS_ELEMENT_DATA_FILE))

            If Not fiSourceFile.Exists Then
                LogError("PNNLOmics Element Data file not found at: " & fiSourceFile.FullName)
                m_message = "PNNLOmics Element Data file not found"
                Return False
            End If

            fiSourceFile.CopyTo(Path.Combine(m_WorkingDir, OMICS_ELEMENT_DATA_FILE))

        Catch ex As Exception
            LogError("Error copying " & OMICS_ELEMENT_DATA_FILE, ex)
            Return False
        End Try

        Return True

    End Function

    ''' <summary>
    ''' Retrieves a dataset file for the analysis job in progress; uses the user-supplied extension to match the file
    ''' </summary>
    ''' <param name="FileExtension">File extension to match; must contain a period, for example ".raw"</param>
    ''' ''' <param name="CreateStoragePathInfoOnly">If true, then create a storage path info file</param>
    ''' <returns>TRUE for success; FALSE for failure</returns>
    ''' <remarks></remarks>
    Protected Function RetrieveDatasetFile(FileExtension As String, createStoragePathInfoOnly As Boolean) As Boolean
        Return RetrieveDatasetFile(FileExtension, createStoragePathInfoOnly, DEFAULT_MAX_RETRY_COUNT)
    End Function

    ''' <summary>
    ''' Retrieves a dataset file for the analysis job in progress; uses the user-supplied extension to match the file
    ''' </summary>
    ''' <param name="FileExtension">File extension to match; must contain a period, for example ".raw"</param>
    ''' <param name="CreateStoragePathInfoOnly">If true, then create a storage path info file</param>
    ''' <param name="maxAttempts">Maximum number of attempts</param>
    ''' <returns>TRUE for success; FALSE for failure</returns>
    ''' <remarks></remarks>
    Protected Function RetrieveDatasetFile(FileExtension As String, createStoragePathInfoOnly As Boolean, maxAttempts As Integer) As Boolean

        Dim DatasetFilePath As String = FindDatasetFile(maxAttempts, FileExtension)
        If String.IsNullOrEmpty(DatasetFilePath) Then
            Return False
        End If

        If (DatasetFilePath.StartsWith(MYEMSL_PATH_FLAG)) Then
            ' Queue this file for download
            m_MyEMSLUtilities.AddFileToDownloadQueue(m_MyEMSLUtilities.RecentlyFoundMyEMSLFiles.First().FileInfo)
            Return True
        End If

        Dim fiDatasetFile = New FileInfo(DatasetFilePath)
        If Not fiDatasetFile.Exists Then
            LogError("Source dataset file not found: " & fiDatasetFile.FullName)
            m_message = "Source dataset file not found"
            Return False
        End If

        If m_DebugLevel >= 1 Then
            LogDebugMessage("Retrieving file " & fiDatasetFile.FullName)
        End If

        If CopyFileToWorkDir(fiDatasetFile.Name, fiDatasetFile.DirectoryName, m_WorkingDir, clsLogTools.LogLevels.ERROR, createStoragePathInfoOnly) Then
            Return True
        Else
            Return False
        End If

    End Function

    ''' <summary>
    ''' Retrieves an Agilent ion trap .mgf file or .cdf/,mgf pair for analysis job in progress
    ''' </summary>
    ''' <param name="GetCdfAlso">TRUE if .cdf file is needed along with .mgf file; FALSE otherwise</param>
    ''' <returns>TRUE for success; FALSE for failure</returns>
    ''' <remarks></remarks>
    Protected Function RetrieveMgfFile(GetCdfAlso As Boolean, createStoragePathInfoOnly As Boolean, maxAttempts As Integer) As Boolean

        Dim strMGFFilePath As String
        Dim fiMGFFile As FileInfo

        strMGFFilePath = FindMGFFile(maxAttempts, assumeUnpurged:=False)

        If String.IsNullOrEmpty(strMGFFilePath) Then
            LogError("Source mgf file not found using FindMGFFile")
            Return False
        End If

        fiMGFFile = New FileInfo(strMGFFilePath)
        If Not fiMGFFile.Exists Then
            LogError("Source mgf file not found: " & fiMGFFile.FullName)
            m_message = "Source mgf file not found"
            Return False
        End If


        'Do the copy
        If Not CopyFileToWorkDirWithRename(fiMGFFile.Name, fiMGFFile.DirectoryName, m_WorkingDir, clsLogTools.LogLevels.ERROR, createStoragePathInfoOnly, MaxCopyAttempts:=3) Then Return False

        'If we don't need to copy the .cdf file, we're done; othewise, find the .cdf file and copy it
        If Not GetCdfAlso Then Return True

        For Each fiCDFFile As FileInfo In fiMGFFile.Directory.GetFiles("*" + DOT_CDF_EXTENSION)
            'Copy the .cdf file that was found
            If CopyFileToWorkDirWithRename(fiCDFFile.Name, fiCDFFile.DirectoryName, m_WorkingDir, clsLogTools.LogLevels.ERROR, createStoragePathInfoOnly, MaxCopyAttempts:=3) Then
                Return True
            Else
                If String.IsNullOrEmpty(m_message) Then
                    LogError("Error obtaining CDF file from " & fiCDFFile.FullName)
                End If
                Return False
            End If
        Next

        ' CDF file not found
        LogError("CDF File not found")

        Return False

    End Function

    ''' <summary>
    ''' Looks for this dataset's mzXML file
    ''' Looks in folders with names like MSXML_Gen_1_154_DatasetID, MSXML_Gen_1_93_DatasetID, or MSXML_Gen_1_39_DatasetID (plus a few others)
    ''' Also examines \\Proto-11\MSXML_Cache
    ''' If the .mzXML file cannot be found, then returns False
    ''' </summary>
    ''' <param name="CreateStoragePathInfoOnly"></param>
    ''' <param name="SourceFilePath">Output parameter: Returns the full path to the file that was retrieved</param>
    ''' <returns>True if the file was found and retrieved, otherwise False</returns>
    ''' <remarks>The retrieved file might be gzipped</remarks>
    Protected Function RetrieveMZXmlFile(createStoragePathInfoOnly As Boolean, <Out()> ByRef sourceFilePath As String) As Boolean

        Dim hashcheckFilePath As String = String.Empty
        sourceFilePath = FindMZXmlFile(hashcheckFilePath)

        If String.IsNullOrEmpty(sourceFilePath) Then
            Return False
        Else
            Return RetrieveMZXmlFileUsingSourceFile(createStoragePathInfoOnly, sourceFilePath, hashcheckFilePath)
        End If

    End Function

    ''' <summary>
    ''' Retrieves this dataset's mzXML file
    ''' </summary>
    ''' <param name="CreateStoragePathInfoOnly"></param>
    ''' <param name="SourceFilePath">Full path to the file that should be retrieved</param>
    ''' <param name="HashcheckFilePath"></param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Protected Function RetrieveMZXmlFileUsingSourceFile(createStoragePathInfoOnly As Boolean, sourceFilePath As String, hashcheckFilePath As String) As Boolean

        Dim fiSourceFile As FileInfo

        If sourceFilePath.StartsWith(MYEMSL_PATH_FLAG) Then
            Return m_MyEMSLUtilities.AddFileToDownloadQueue(sourceFilePath)
        End If

        fiSourceFile = New FileInfo(sourceFilePath)

        If fiSourceFile.Exists Then
            If CopyFileToWorkDir(fiSourceFile.Name, fiSourceFile.Directory.FullName, m_WorkingDir, clsLogTools.LogLevels.ERROR, createStoragePathInfoOnly) Then

                If Not String.IsNullOrEmpty(hashcheckFilePath) AndAlso File.Exists(hashcheckFilePath) Then
                    Return RetrieveMzXMLFileVerifyHash(fiSourceFile, hashcheckFilePath, createStoragePathInfoOnly)
                Else
                    Return True
                End If

            End If
        End If

        If m_DebugLevel >= 1 Then
            ReportStatus("MzXML file not found; will need to generate it: " + fiSourceFile.Name)
        End If

        Return False

    End Function

    ''' <summary>
    ''' Verify the hash value of a given .mzXML file
    ''' </summary>
    ''' <param name="fiSourceFile"></param>
    ''' <param name="HashcheckFilePath"></param>
    ''' <param name="CreateStoragePathInfoOnly"></param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Protected Function RetrieveMzXMLFileVerifyHash(fiSourceFile As FileInfo, HashcheckFilePath As String, createStoragePathInfoOnly As Boolean) As Boolean

        Dim strTargetFilePath As String
        Dim strErrorMessage As String = String.Empty
        Dim blnComputeHash As Boolean

        If createStoragePathInfoOnly Then
            strTargetFilePath = fiSourceFile.FullName
            ' Don't compute the hash, since we're accessing the file over the network
            blnComputeHash = False
        Else
            strTargetFilePath = Path.Combine(m_WorkingDir, fiSourceFile.Name)
            blnComputeHash = True
        End If

        If clsGlobal.ValidateFileVsHashcheck(strTargetFilePath, HashcheckFilePath, strErrorMessage, blnCheckDate:=True, blnComputeHash:=blnComputeHash) Then
            Return True
        End If

        ReportStatus("MzXML file validation error in RetrieveMzXMLFileVerifyHash: " & strErrorMessage, 0, True)

        Try
            If createStoragePathInfoOnly Then
                ' Delete the local StoragePathInfo file
                Dim strStoragePathInfoFile As String = Path.Combine(m_WorkingDir, fiSourceFile.Name & STORAGE_PATH_INFO_FILE_SUFFIX)
                If File.Exists(strStoragePathInfoFile) Then
                    File.Delete(strStoragePathInfoFile)
                End If
            Else
                ' Delete the local file to force it to be re-generated
                File.Delete(strTargetFilePath)
            End If

        Catch ex As Exception
            ' Ignore errors here
        End Try

        Try
            ' Delete the remote mzXML file only if we computed the hash and we had a hash mismatch
            If blnComputeHash Then
                fiSourceFile.Delete()
            End If
        Catch ex As Exception
            ' Ignore errors here
        End Try

        Return False

    End Function

    ''' <summary>
    ''' Looks for this dataset's ScanStats files (previously created by MASIC)
    ''' Looks for the files in any SIC folder that exists for the dataset
    ''' </summary>
    ''' <param name="CreateStoragePathInfoOnly">If true, then creates a storage path info file but doesn't actually copy the files</param>
    ''' <returns>True if the file was found and retrieved, otherwise False</returns>
    ''' <remarks></remarks>
    Protected Function RetrieveScanStatsFiles(createStoragePathInfoOnly As Boolean) As Boolean

        Const RetrieveSICStatsFile = False
        Return RetrieveScanAndSICStatsFiles(RetrieveSICStatsFile, createStoragePathInfoOnly, RetrieveScanStatsFile:=True, RetrieveScanStatsExFile:=True)

    End Function

    ''' <summary>
    ''' Looks for this dataset's ScanStats files (previously created by MASIC)
    ''' Looks for the files in any SIC folder that exists for the dataset
    ''' </summary>
    ''' <param name="CreateStoragePathInfoOnly"></param>
    ''' <param name="RetrieveScanStatsFile">If True, then retrieves the ScanStats.txt file</param>
    ''' <param name="RetrieveScanStatsExFile">If True, then retrieves the ScanStatsEx.txt file</param>
    ''' <returns>True if the file was found and retrieved, otherwise False</returns>
    ''' <remarks></remarks>
    Protected Function RetrieveScanStatsFiles(createStoragePathInfoOnly As Boolean, RetrieveScanStatsFile As Boolean, RetrieveScanStatsExFile As Boolean) As Boolean

        Const RetrieveSICStatsFile = False
        Return RetrieveScanAndSICStatsFiles(RetrieveSICStatsFile, createStoragePathInfoOnly, RetrieveScanStatsFile, RetrieveScanStatsExFile)

    End Function

    ''' <summary>
    ''' Looks for this dataset's MASIC results files
    ''' Looks for the files in any SIC folder that exists for the dataset
    ''' </summary>
    ''' <param name="RetrieveSICStatsFile">If True, then also copies the _SICStats.txt file in addition to the ScanStats files</param>
    ''' <param name="CreateStoragePathInfoOnly">If true, then creates a storage path info file but doesn't actually copy the files</param>
    ''' <returns>True if the file was found and retrieved, otherwise False</returns>
    ''' <remarks></remarks>
    Protected Function RetrieveScanAndSICStatsFiles(RetrieveSICStatsFile As Boolean, createStoragePathInfoOnly As Boolean) As Boolean
        Return RetrieveScanAndSICStatsFiles(RetrieveSICStatsFile, createStoragePathInfoOnly, RetrieveScanStatsFile:=True, RetrieveScanStatsExFile:=True)
    End Function

    ''' <summary>
    ''' Looks for this dataset's MASIC results files
    ''' Looks for the files in any SIC folder that exists for the dataset
    ''' </summary>
    ''' <param name="RetrieveSICStatsFile">If True, then also copies the _SICStats.txt file in addition to the ScanStats files</param>
    ''' <param name="CreateStoragePathInfoOnly">If true, then creates a storage path info file but doesn't actually copy the files</param>
    ''' <param name="RetrieveScanStatsFile">If True, then retrieves the ScanStats.txt file</param>
    ''' <param name="RetrieveScanStatsExFile">If True, then retrieves the ScanStatsEx.txt file</param>
    ''' <returns>True if the file was found and retrieved, otherwise False</returns>
    ''' <remarks></remarks>
    Protected Function RetrieveScanAndSICStatsFiles(
      RetrieveSICStatsFile As Boolean,
      createStoragePathInfoOnly As Boolean,
      RetrieveScanStatsFile As Boolean,
      RetrieveScanStatsExFile As Boolean) As Boolean

        Dim lstNonCriticalFileSuffixes = New List(Of String)
        Const RETRIEVE_REPORTERIONS_FILE = False

        Return RetrieveScanAndSICStatsFiles(
            RetrieveSICStatsFile, createStoragePathInfoOnly, RetrieveScanStatsFile, RetrieveScanStatsExFile,
            RETRIEVE_REPORTERIONS_FILE, lstNonCriticalFileSuffixes)

    End Function

    ''' <summary>
    ''' Looks for this dataset's MASIC results files
    ''' Looks for the files in any SIC folder that exists for the dataset
    ''' </summary>
    ''' <param name="retrieveSICStatsFile">If True, also copies the _SICStats.txt file in addition to the ScanStats files</param>
    ''' <param name="createStoragePathInfoOnly">If true, creates a storage path info file but doesn't actually copy the files</param>
    ''' <param name="retrieveScanStatsFile">If True, retrieves the ScanStats.txt file</param>
    ''' <param name="retrieveScanStatsExFile">If True, retrieves the ScanStatsEx.txt file</param>
    ''' <param name="retrieveReporterIonsFile">If True, retrieves the ReporterIons.txt file</param>
    ''' <param name="lstNonCriticalFileSuffixes">Filename suffixes that can be missing.  For example, "ScanStatsEx.txt"</param>
    ''' <returns>True if the file was found and retrieved, otherwise False</returns>
    ''' <remarks></remarks>
    Protected Function RetrieveScanAndSICStatsFiles(
      retrieveSICStatsFile As Boolean,
      createStoragePathInfoOnly As Boolean,
      retrieveScanStatsFile As Boolean,
      retrieveScanStatsExFile As Boolean,
      retrieveReporterIonsFile As Boolean,
      lstNonCriticalFileSuffixes As List(Of String)) As Boolean

        Dim ServerPath As String
        Dim ScanStatsFilename As String

        Dim BestScanStatsFileTransactionID As Int64 = 0

        Const MAX_ATTEMPTS = 1

        ' Look for the MASIC Results folder
        ' If the folder cannot be found, then FindValidFolder will return the folder defined by "DatasetStoragePath"
        ScanStatsFilename = m_DatasetName + SCAN_STATS_FILE_SUFFIX
        ServerPath = FindValidFolder(m_DatasetName, "", "SIC*", MAX_ATTEMPTS, logFolderNotFound:=False, retrievingInstrumentDataFolder:=False)

        If String.IsNullOrEmpty(ServerPath) Then
            m_message = "Dataset folder path not defined"
        Else

            If ServerPath.StartsWith(MYEMSL_PATH_FLAG) Then
                ' Find the newest _ScanStats.txt file in MyEMSL
                Dim BestSICFolderName = String.Empty

                For Each myEmslFile In m_MyEMSLUtilities.RecentlyFoundMyEMSLFiles
                    If myEmslFile.IsFolder Then
                        Continue For
                    End If

                    If clsGlobal.IsMatch(myEmslFile.FileInfo.Filename, ScanStatsFilename) AndAlso
                      myEmslFile.FileInfo.TransactionID > BestScanStatsFileTransactionID Then
                        Dim fiScanStatsFile = New FileInfo(myEmslFile.FileInfo.RelativePathWindows)
                        BestSICFolderName = fiScanStatsFile.Directory.Name
                        BestScanStatsFileTransactionID = myEmslFile.FileInfo.TransactionID
                    End If
                Next

                If BestScanStatsFileTransactionID = 0 Then
                    m_message = "MASIC ScanStats file not found in the SIC results folder(s) in MyEMSL"
                Else
                    Dim bestSICFolderPath = Path.Combine(MYEMSL_PATH_FLAG, BestSICFolderName)
                    Return RetrieveScanAndSICStatsFiles(
                        bestSICFolderPath,
                        retrieveSICStatsFile,
                        createStoragePathInfoOnly,
                        retrieveScanStatsFile:=retrieveScanStatsFile,
                        retrieveScanStatsExFile:=retrieveScanStatsExFile,
                        retrieveReporterIonsFile:=retrieveReporterIonsFile,
                        lstNonCriticalFileSuffixes:=lstNonCriticalFileSuffixes)
                End If
            Else
                Dim diFolderInfo As DirectoryInfo
                diFolderInfo = New DirectoryInfo(ServerPath)

                If Not diFolderInfo.Exists Then
                    m_message = "Dataset folder with MASIC files not found: " + diFolderInfo.FullName
                Else

                    ' See if the ServerPath folder actually contains a subfolder that starts with "SIC"
                    Dim diSubfolders() As DirectoryInfo = diFolderInfo.GetDirectories("SIC*")
                    If diSubfolders.Length = 0 Then
                        m_message = "Dataset folder does not contain any MASIC results folders: " + diFolderInfo.FullName
                    Else
                        ' MASIC Results Folder Found
                        ' If more than one folder, then use the folder with the newest _ScanStats.txt file						
                        Dim dtNewestScanStatsFileDate As DateTime
                        Dim strNewestScanStatsFilePath As String = String.Empty

                        For Each diSubFolder As DirectoryInfo In diSubfolders
                            Dim fiSourceFile = New FileInfo(Path.Combine(diSubFolder.FullName, ScanStatsFilename))
                            If fiSourceFile.Exists Then
                                If String.IsNullOrEmpty(strNewestScanStatsFilePath) OrElse fiSourceFile.LastWriteTimeUtc > dtNewestScanStatsFileDate Then
                                    strNewestScanStatsFilePath = fiSourceFile.FullName
                                    dtNewestScanStatsFileDate = fiSourceFile.LastWriteTimeUtc
                                End If
                            End If
                        Next

                        If String.IsNullOrEmpty(strNewestScanStatsFilePath) Then
                            m_message = "MASIC ScanStats file not found below " + diFolderInfo.FullName
                        Else
                            Dim fiSourceFile = New FileInfo(strNewestScanStatsFilePath)
                            Dim bestSICFolderPath = fiSourceFile.Directory.FullName
                            Return RetrieveScanAndSICStatsFiles(
                                bestSICFolderPath,
                                retrieveSICStatsFile,
                                createStoragePathInfoOnly,
                                retrieveScanStatsFile:=retrieveScanStatsFile,
                                retrieveScanStatsExFile:=retrieveScanStatsExFile,
                                retrieveReporterIonsFile:=retrieveReporterIonsFile,
                                lstNonCriticalFileSuffixes:=lstNonCriticalFileSuffixes)
                        End If

                    End If
                End If
            End If
        End If

        If m_DebugLevel >= 1 Then
            If String.IsNullOrEmpty(m_message) Then
                ReportStatus("RetrieveScanAndSICStatsFiles v1: Unknown Error", 0, True)
            Else
                ReportStatus("RetrieveScanAndSICStatsFiles v1: " & m_message, 0, True)
            End If
        End If

        Return False

    End Function

    ''' <summary>
    ''' Retrieves the MASIC results for this dataset using the specified folder
    ''' </summary>
    ''' <param name="MASICResultsFolderPath">Source folder to copy files from</param>
    ''' <param name="RetrieveSICStatsFile">If True, then also copies the _SICStats.txt file in addition to the ScanStats files</param>
    ''' <param name="CreateStoragePathInfoOnly">If true, then creates a storage path info file but doesn't actually copy the files</param>
    ''' <param name="RetrieveScanStatsFile">If True, then retrieves the ScanStats.txt file</param>
    ''' <param name="RetrieveScanStatsExFile">If True, then retrieves the ScanStatsEx.txt file</param>
    ''' <returns>True if the file was found and retrieved, otherwise False</returns>
    ''' <remarks></remarks>
    Protected Function RetrieveScanAndSICStatsFiles(
      MASICResultsFolderPath As String,
      RetrieveSICStatsFile As Boolean,
      createStoragePathInfoOnly As Boolean,
      RetrieveScanStatsFile As Boolean,
      RetrieveScanStatsExFile As Boolean) As Boolean

        Dim lstNonCriticalFileSuffixes = New List(Of String)
        Const RETRIEVE_REPORTERIONS_FILE = False

        Return RetrieveScanAndSICStatsFiles(
            MASICResultsFolderPath, RetrieveSICStatsFile, createStoragePathInfoOnly,
            RetrieveScanStatsFile, RetrieveScanStatsExFile, RETRIEVE_REPORTERIONS_FILE,
            lstNonCriticalFileSuffixes)
    End Function

    ''' <summary>
    ''' Retrieves the MASIC results for this dataset using the specified folder
    ''' </summary>
    ''' <param name="masicResultsFolderPath">Source folder to copy files from</param>
    ''' <param name="retrieveSICStatsFile">If True, also copies the _SICStats.txt file in addition to the ScanStats files</param>
    ''' <param name="createStoragePathInfoOnly">If true, creates a storage path info file but doesn't actually copy the files</param>
    ''' <param name="retrieveScanStatsFile">If True, retrieves the ScanStats.txt file</param>
    ''' <param name="retrieveScanStatsExFile">If True, retrieves the ScanStatsEx.txt file</param>
    ''' <param name="retrieveReporterIonsFile">If True, retrieves the ReporterIons.txt file</param>
    ''' <param name="lstNonCriticalFileSuffixes">Filename suffixes that can be missing.  For example, "ScanStatsEx.txt"</param>
    ''' <returns>True if the file was found and retrieved, otherwise False</returns>
    Protected Function RetrieveScanAndSICStatsFiles(
      masicResultsFolderPath As String,
      retrieveSICStatsFile As Boolean,
      createStoragePathInfoOnly As Boolean,
      retrieveScanStatsFile As Boolean,
      retrieveScanStatsExFile As Boolean,
      retrieveReporterIonsFile As Boolean,
      lstNonCriticalFileSuffixes As List(Of String)) As Boolean

        Const MaxCopyAttempts = 2

        ' Copy the MASIC files from the MASIC results folder

        If String.IsNullOrEmpty(masicResultsFolderPath) Then
            m_message = "MASIC Results folder path not defined"

        ElseIf masicResultsFolderPath.StartsWith(MYEMSL_PATH_FLAG) Then

            Dim diSICFolder = New DirectoryInfo(masicResultsFolderPath)

            If retrieveScanStatsFile Then
                ' Look for and copy the _ScanStats.txt file
                If Not RetrieveSICFileMyEMSL(m_DatasetName + SCAN_STATS_FILE_SUFFIX, diSICFolder.Name, lstNonCriticalFileSuffixes) Then
                    Return False
                End If
            End If

            If retrieveScanStatsExFile Then
                ' Look for and copy the _ScanStatsEx.txt file
                If Not RetrieveSICFileMyEMSL(m_DatasetName + SCAN_STATS_EX_FILE_SUFFIX, diSICFolder.Name, lstNonCriticalFileSuffixes) Then
                    Return False
                End If
            End If


            If retrieveSICStatsFile Then
                ' Look for and copy the _SICStats.txt file
                If Not RetrieveSICFileMyEMSL(m_DatasetName + "_SICStats.txt", diSICFolder.Name, lstNonCriticalFileSuffixes) Then
                    Return False
                End If
            End If

            If retrieveReporterIonsFile Then
                ' Look for and copy the _SICStats.txt file
                If Not RetrieveSICFileMyEMSL(m_DatasetName + "_ReporterIons.txt", diSICFolder.Name, lstNonCriticalFileSuffixes) Then
                    Return False
                End If
            End If

            ' All files have been found
            ' The calling process should download them using ProcessMyEMSLDownloadQueue()
            Return True

        Else

            Dim diFolderInfo As DirectoryInfo
            diFolderInfo = New DirectoryInfo(masicResultsFolderPath)

            If Not diFolderInfo.Exists Then
                m_message = "MASIC Results folder not found: " + diFolderInfo.FullName
            Else

                If retrieveScanStatsFile Then
                    ' Look for and copy the _ScanStats.txt file
                    If Not RetrieveSICFileUNC(m_DatasetName + SCAN_STATS_FILE_SUFFIX, masicResultsFolderPath, createStoragePathInfoOnly, MaxCopyAttempts, lstNonCriticalFileSuffixes) Then
                        Return False
                    End If
                End If

                If retrieveScanStatsExFile Then
                    ' Look for and copy the _ScanStatsEx.txt file
                    If Not RetrieveSICFileUNC(m_DatasetName + SCAN_STATS_EX_FILE_SUFFIX, masicResultsFolderPath, createStoragePathInfoOnly, MaxCopyAttempts, lstNonCriticalFileSuffixes) Then
                        Return False
                    End If
                End If

                If retrieveSICStatsFile Then
                    ' Look for and copy the _SICStats.txt file
                    If Not RetrieveSICFileUNC(m_DatasetName + "_SICStats.txt", masicResultsFolderPath, createStoragePathInfoOnly, MaxCopyAttempts, lstNonCriticalFileSuffixes) Then
                        Return False
                    End If
                End If

                If retrieveReporterIonsFile Then
                    ' Look for and copy the _SICStats.txt file
                    If Not RetrieveSICFileUNC(m_DatasetName + "_ReporterIons.txt", masicResultsFolderPath, createStoragePathInfoOnly, MaxCopyAttempts, lstNonCriticalFileSuffixes) Then
                        Return False
                    End If
                End If

                ' All files successfully copied
                Return True

            End If

        End If

        If m_DebugLevel >= 1 Then
            If String.IsNullOrEmpty(m_message) Then
                ReportStatus("RetrieveScanAndSICStatsFiles v2: Unknown Error", 0, True)
            Else
                ReportStatus("RetrieveScanAndSICStatsFiles v2: " & m_message, 0, True)
            End If
        End If

        Return False

    End Function

    Protected Function RetrieveSICFileMyEMSL(strFileToFind As String, strSICFolderName As String, lstNonCriticalFileSuffixes As List(Of String)) As Boolean

        Dim matchingMyEMSLFiles = m_MyEMSLUtilities.FindFiles(strFileToFind, strSICFolderName, m_DatasetName, recurse:=False)

        If matchingMyEMSLFiles.Count > 0 Then
            If m_DebugLevel >= 3 Then
                LogDebugMessage("Found MASIC results file in MyEMSL, " & Path.Combine(strSICFolderName, strFileToFind))
            End If

            m_MyEMSLUtilities.AddFileToDownloadQueue(matchingMyEMSLFiles.First().FileInfo)

        Else
            Dim blnIgnoreFile As Boolean
            blnIgnoreFile = SafeToIgnore(strFileToFind, lstNonCriticalFileSuffixes)

            If Not blnIgnoreFile Then
                m_message = strFileToFind + " not found in MyEMSL, subfolder " + strSICFolderName
                Return False
            End If
        End If

        Return True

    End Function

    Protected Function RetrieveSICFileUNC(strFileToFind As String, MASICResultsFolderPath As String, createStoragePathInfoOnly As Boolean, MaxCopyAttempts As Integer, lstNonCriticalFileSuffixes As List(Of String)) As Boolean

        Dim fiSourceFile = New FileInfo(Path.Combine(MASICResultsFolderPath, strFileToFind))

        If m_DebugLevel >= 3 Then
            LogDebugMessage("Copying MASIC results file: " + fiSourceFile.FullName)
        End If

        Dim blnIgnoreFile = SafeToIgnore(fiSourceFile.Name, lstNonCriticalFileSuffixes)

        Dim eLogMsgTypeIfNotFound As clsLogTools.LogLevels
        If blnIgnoreFile Then
            eLogMsgTypeIfNotFound = clsLogTools.LogLevels.DEBUG
        Else
            eLogMsgTypeIfNotFound = clsLogTools.LogLevels.ERROR
        End If

        Dim success = CopyFileToWorkDir(fiSourceFile.Name, fiSourceFile.Directory.FullName,
                                        m_WorkingDir, eLogMsgTypeIfNotFound, createStoragePathInfoOnly, MaxCopyAttempts)
        If Not success Then
            If blnIgnoreFile Then
                If m_DebugLevel >= 3 Then
                    LogDebugMessage("  File not found; this is not a problem")
                End If
            Else
                LogError(strFileToFind + " not found at " + fiSourceFile.Directory.FullName)
                Return False
            End If
        End If

        Return True

    End Function

    ''' <summary>
    ''' Retrieves the spectra file(s) based on raw data type and puts them in the working directory
    ''' </summary>
    ''' <param name="RawDataType">Type of data to copy</param>
    ''' <returns>TRUE for success; FALSE for failure</returns>
    ''' <remarks></remarks>
    Protected Function RetrieveSpectra(RawDataType As String) As Boolean
        Const createStoragePathInfoOnly = False
        Return RetrieveSpectra(RawDataType, createStoragePathInfoOnly)
    End Function

    ''' <summary>
    ''' Retrieves the spectra file(s) based on raw data type and puts them in the working directory
    ''' </summary>
    ''' <param name="RawDataType">Type of data to copy</param>
    ''' <param name="CreateStoragePathInfoOnly">When true, then does not actually copy the dataset file (or folder), and instead creates a file named Dataset.raw_StoragePathInfo.txt, and this file's first line will be the full path to the spectrum file (or spectrum folder)</param>
    ''' <returns>TRUE for success; FALSE for failure</returns>
    ''' <remarks></remarks>
    Protected Function RetrieveSpectra(RawDataType As String, createStoragePathInfoOnly As Boolean) As Boolean
        Return RetrieveSpectra(RawDataType, createStoragePathInfoOnly, DEFAULT_MAX_RETRY_COUNT)
    End Function

    ''' <summary>
    ''' Retrieves the spectra file(s) based on raw data type and puts them in the working directory
    ''' </summary>
    ''' <param name="RawDataType">Type of data to copy</param>
    ''' <param name="CreateStoragePathInfoOnly">When true, then does not actually copy the dataset file (or folder), and instead creates a file named Dataset.raw_StoragePathInfo.txt, and this file's first line will be the full path to the spectrum file (or spectrum folder)</param>
    ''' <param name="maxAttempts">Maximum number of attempts</param>
    ''' <returns>TRUE for success; FALSE for failure</returns>
    ''' <remarks></remarks>
    Protected Function RetrieveSpectra(RawDataType As String, createStoragePathInfoOnly As Boolean, maxAttempts As Integer) As Boolean

        Dim blnSuccess = False
        Dim StoragePath As String = m_jobParams.GetParam("DatasetStoragePath")
        Dim eRawDataType As eRawDataTypeConstants

        ReportStatus("Retrieving spectra file(s)")

        eRawDataType = GetRawDataType(RawDataType)
        Select Case eRawDataType
            Case eRawDataTypeConstants.AgilentDFolder           'Agilent ion trap data

                If StoragePath.ToLower().Contains("Agilent_SL1".ToLower()) OrElse
                   StoragePath.ToLower().Contains("Agilent_XCT1".ToLower()) Then
                    ' For Agilent Ion Trap datasets acquired on Agilent_SL1 or Agilent_XCT1 in 2005, 
                    '  we would pre-process the data beforehand to create MGF files
                    ' The following call can be used to retrieve the files
                    blnSuccess = RetrieveMgfFile(GetCdfAlso:=True, createStoragePathInfoOnly:=createStoragePathInfoOnly, maxAttempts:=maxAttempts)
                Else
                    ' DeconTools_V2 now supports reading the .D files directly
                    ' Call RetrieveDotDFolder() to copy the folder and all subfolders
                    blnSuccess = RetrieveDotDFolder(createStoragePathInfoOnly, maxAttempts, blnSkipBAFFiles:=True)
                End If

            Case eRawDataTypeConstants.AgilentQStarWiffFile         'Agilent/QSTAR TOF data
                blnSuccess = RetrieveDatasetFile(DOT_WIFF_EXTENSION, createStoragePathInfoOnly, maxAttempts)

            Case eRawDataTypeConstants.ZippedSFolders           'FTICR data
                blnSuccess = RetrieveSFolders(createStoragePathInfoOnly, maxAttempts)

            Case eRawDataTypeConstants.ThermoRawFile            'Finnigan ion trap/LTQ-FT data				
                blnSuccess = RetrieveDatasetFile(DOT_RAW_EXTENSION, createStoragePathInfoOnly, maxAttempts)

            Case eRawDataTypeConstants.MicromassRawFolder           'Micromass QTOF data
                blnSuccess = RetrieveDotRawFolder(createStoragePathInfoOnly, maxAttempts)

            Case eRawDataTypeConstants.UIMF         'IMS UIMF data
                blnSuccess = RetrieveDatasetFile(DOT_UIMF_EXTENSION, createStoragePathInfoOnly, maxAttempts)

            Case eRawDataTypeConstants.mzXML
                blnSuccess = RetrieveDatasetFile(DOT_MZXML_EXTENSION, createStoragePathInfoOnly, maxAttempts)

            Case eRawDataTypeConstants.mzML
                blnSuccess = RetrieveDatasetFile(DOT_MZML_EXTENSION, createStoragePathInfoOnly, maxAttempts)

            Case eRawDataTypeConstants.BrukerFTFolder, eRawDataTypeConstants.BrukerTOFBaf
                ' Call RetrieveDotDFolder() to copy the folder and all subfolders

                ' Both the MSXml step tool and DeconTools require the .Baf file
                ' We previously didn't need this file for DeconTools, but, now that DeconTools is using CompassXtract, we need the file
                ' In contrast, ICR-2LS only needs the ser or FID file, plus the apexAcquisition.method file in the .md folder

                Dim blnSkipBAFFiles = False

                Dim strStepTool = m_jobParams.GetJobParameter("StepTool", "Unknown")

                If strStepTool = "ICR2LS" Then
                    blnSkipBAFFiles = True
                End If

                blnSuccess = RetrieveDotDFolder(createStoragePathInfoOnly, maxAttempts, blnSkipBAFFiles)

            Case eRawDataTypeConstants.BrukerMALDIImaging
                blnSuccess = RetrieveBrukerMALDIImagingFolders(UnzipOverNetwork:=True)

            Case Else
                ' RawDataType is not recognized or not supported by this function
                If eRawDataType = eRawDataTypeConstants.Unknown Then
                    LogError("Invalid data type specified: " + RawDataType)
                Else
                    LogError("Data type " + RawDataType + " is not supported by the RetrieveSpectra function")
                End If
        End Select

        'Return the result of the spectra retrieval
        Return blnSuccess

    End Function

    ''' <summary>
    ''' Retrieves an Agilent or Bruker .D folder for the analysis job in progress
    ''' </summary>
    ''' <returns>TRUE for success; FALSE for failure</returns>
    ''' <remarks></remarks>
    Protected Function RetrieveDotDFolder(createStoragePathInfoOnly As Boolean, maxAttempts As Integer, blnSkipBAFFiles As Boolean) As Boolean

        Dim objFileNamesToSkip As List(Of String)

        objFileNamesToSkip = New List(Of String)
        If blnSkipBAFFiles Then
            objFileNamesToSkip.Add("analysis.baf")
        End If

        Return RetrieveDotXFolder(DOT_D_EXTENSION, createStoragePathInfoOnly, maxAttempts, objFileNamesToSkip)
    End Function

    ''' <summary>
    ''' Retrieves a Micromass .raw folder for the analysis job in progress
    ''' </summary>
    ''' <returns>TRUE for success; FALSE for failure</returns>
    ''' <remarks></remarks>
    Protected Function RetrieveDotRawFolder(createStoragePathInfoOnly As Boolean, maxAttempts As Integer) As Boolean
        Return RetrieveDotXFolder(DOT_RAW_EXTENSION, createStoragePathInfoOnly, maxAttempts, New List(Of String))
    End Function

    ''' <summary>
    ''' Retrieves a folder with a name like Dataset.D or Dataset.Raw
    ''' </summary>
    ''' <param name="FolderExtension">Extension on the folder; for example, ".D"</param>
    ''' <returns>TRUE for success; FALSE for failure</returns>
    ''' <remarks></remarks>
    Protected Function RetrieveDotXFolder(
      FolderExtension As String,
      createStoragePathInfoOnly As Boolean,
      maxAttempts As Integer,
      objFileNamesToSkip As List(Of String)) As Boolean

        'Copies a data folder ending in FolderExtension to the working directory

        'Find the instrument data folder (e.g. Dataset.D or Dataset.Raw) in the dataset folder
        Dim DSFolderPath As String = FindDotXFolder(FolderExtension, assumeUnpurged:=False)

        If String.IsNullOrEmpty(DSFolderPath) Then
            Return False
        End If

        If (DSFolderPath.StartsWith(MYEMSL_PATH_FLAG)) Then
            ' Queue the MyEMSL files for download
            For Each udtArchiveFile In m_MyEMSLUtilities.RecentlyFoundMyEMSLFiles
                m_MyEMSLUtilities.AddFileToDownloadQueue(udtArchiveFile.FileInfo)
            Next
            Return True
        End If

        'Do the copy
        Try
            Dim diSourceFolder As DirectoryInfo
            Dim DestFolderPath As String

            diSourceFolder = New DirectoryInfo(DSFolderPath)
            If Not diSourceFolder.Exists Then
                LogError("Source dataset folder not found: " & diSourceFolder.FullName)
                m_message = "Source dataset folder not found"
                Return False
            End If

            DestFolderPath = Path.Combine(m_WorkingDir, diSourceFolder.Name)

            If createStoragePathInfoOnly Then
                CreateStoragePathInfoFile(diSourceFolder.FullName, DestFolderPath)
            Else
                ' Copy the directory and all subdirectories
                ' Skip any files defined by objFileNamesToSkip
                If m_DebugLevel >= 1 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Retrieving folder " & diSourceFolder.FullName)
                End If
                ResetTimestampForQueueWaitTimeLogging()
                m_FileTools.CopyDirectory(diSourceFolder.FullName, DestFolderPath, objFileNamesToSkip)
            End If

        Catch ex As Exception
            LogError("Error copying folder " + DSFolderPath, ex)
            Return False
        End Try

        ' If we get here, all is fine
        Return True

    End Function

    ''' <summary>
    ''' Retrieves a data from a Bruker MALDI imaging dataset
    ''' The data is stored as zip files with names like 0_R00X433.zip
    ''' This data is unzipped into a subfolder in the Chameleon cached data folder
    ''' </summary>
    ''' <param name="UnzipOverNetwork"></param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public Function RetrieveBrukerMALDIImagingFolders(UnzipOverNetwork As Boolean) As Boolean

        Const ZIPPED_BRUKER_IMAGING_SECTIONS_FILE_MASK = "*R*X*.zip"

        Dim ChameleonCachedDataFolder As String = m_mgrParams.GetParam("ChameleonCachedDataFolder")
        Dim diCachedDataFolder As DirectoryInfo

        Dim ServerPath As String
        Dim strUnzipFolderPathBase As String

        Dim strFilesToDelete As New Queue(Of String)

        Dim strZipFilePathRemote As String = String.Empty
        Dim strZipFilePathToExtract As String

        Dim blnUnzipFile As Boolean

        Dim blnApplySectionFilter As Boolean
        Dim StartSectionX As Integer
        Dim EndSectionX As Integer

        Dim CoordR As Integer, CoordX As Integer, CoordY As Integer

        Try

            If String.IsNullOrEmpty(ChameleonCachedDataFolder) Then
                LogError("Chameleon cached data folder not defined; unable to unzip MALDI imaging data")
                m_message = "Chameleon cached data folder not defined"
                Return False
            End If

            ' Delete any subfolders at ChameleonCachedDataFolder that do not have this dataset's name
            diCachedDataFolder = New DirectoryInfo(ChameleonCachedDataFolder)
            If Not diCachedDataFolder.Exists Then
                LogError("Chameleon cached data folder does not exist: " + diCachedDataFolder.FullName)
                m_message = "Chameleon cached data folder does not exist"
                Return False
            Else
                strUnzipFolderPathBase = Path.Combine(diCachedDataFolder.FullName, m_DatasetName)
            End If

            For Each diSubFolder As DirectoryInfo In diCachedDataFolder.GetDirectories()
                If Not clsGlobal.IsMatch(diSubFolder.Name, m_DatasetName) Then
                    ' Delete this directory
                    Try
                        If m_DebugLevel >= 2 Then
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG,
                                                 "Deleting old dataset subfolder from chameleon cached data folder: " + diSubFolder.FullName)
                        End If

                        If m_mgrParams.GetParam("MgrName").ToLower().Contains("monroe") Then
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG,
                                                 " Skipping delete since this is a development computer")
                        Else
                            diSubFolder.Delete(True)
                        End If

                    Catch ex As Exception
                        LogError("Error deleting cached subfolder " + diSubFolder.FullName, ex)
                        m_message = "Error deleting cached subfolder"
                        Return False
                    End Try
                End If
            Next

            ' Delete any .mis files that do not start with this dataset's name
            For Each fiFile As FileInfo In diCachedDataFolder.GetFiles("*.mis")
                If Not clsGlobal.IsMatch(Path.GetFileNameWithoutExtension(fiFile.Name), m_DatasetName) Then
                    fiFile.Delete()
                End If
            Next

        Catch ex As Exception
            LogError("Error cleaning out old data from the Chameleon cached data folder", ex)
            Return False
        End Try

        ' See if any imaging section filters are defined
        blnApplySectionFilter = GetBrukerImagingSectionFilter(m_jobParams, StartSectionX, EndSectionX)

        ' Look for the dataset folder; it must contain .Zip files with names like 0_R00X442.zip
        ' If a matching folder isn't found, then ServerPath will contain the folder path defined by Job Param "DatasetStoragePath"
        ServerPath = FindValidFolder(m_DatasetName, ZIPPED_BRUKER_IMAGING_SECTIONS_FILE_MASK, RetrievingInstrumentDataFolder:=True)

        Try

            Dim MisFiles() As String
            Dim strImagingSeqFilePathFinal As String

            ' Look for the .mis file (ImagingSequence file) 
            strImagingSeqFilePathFinal = Path.Combine(diCachedDataFolder.FullName, m_DatasetName + ".mis")

            If Not File.Exists(strImagingSeqFilePathFinal) Then

                ' Copy the .mis file (ImagingSequence file) over from the storage server
                MisFiles = Directory.GetFiles(ServerPath, "*.mis")

                If MisFiles.Length = 0 Then
                    ' No .mis files were found; unable to continue
                    LogError("ImagingSequence (.mis) file not found in dataset folder; unable to process MALDI imaging data")
                    Return False
                Else
                    ' We'll copy the first file in MisFiles(0)
                    ' Log a warning if we will be renaming the file

                    If Not clsGlobal.IsMatch(Path.GetFileName(MisFiles(0)), strImagingSeqFilePathFinal) Then
                        ReportStatus("Note: Renaming .mis file (ImagingSequence file) from " + Path.GetFileName(MisFiles(0)) + " to " + Path.GetFileName(strImagingSeqFilePathFinal))
                    End If

                    If Not CopyFileWithRetry(MisFiles(0), strImagingSeqFilePathFinal, True) Then
                        ' Abort processing
                        LogError("Error copying ImagingSequence (.mis) file; unable to process MALDI imaging data")
                        Return False
                    End If

                End If
            End If

        Catch ex As Exception
            LogError("Error obtaining ImagingSequence (.mis) file", ex)
            Return False
        End Try

        Try

            ' Unzip each of the *R*X*.zip files to the Chameleon cached data folder

            ' However, consider limits defined by job params BrukerMALDI_Imaging_StartSectionX and BrukerMALDI_Imaging_EndSectionX
            ' when processing the files

            Dim ZipFiles() As String
            ZipFiles = Directory.GetFiles(ServerPath, ZIPPED_BRUKER_IMAGING_SECTIONS_FILE_MASK)

            For Each strZipFilePathRemote In ZipFiles

                If blnApplySectionFilter Then
                    blnUnzipFile = False

                    ' Determine the R, X, and Y coordinates for this .Zip file
                    If GetBrukerImagingFileCoords(strZipFilePathRemote, CoordR, CoordX, CoordY) Then
                        ' Compare to StartSectionX and EndSectionX
                        If CoordX >= StartSectionX AndAlso CoordX <= EndSectionX Then
                            blnUnzipFile = True
                        End If
                    End If
                Else
                    blnUnzipFile = True
                End If

                ' Open up the zip file over the network and get a listing of all of the files
                ' If they already exist in the cached data folder, then there is no need to continue

                If blnUnzipFile Then

                    ' Set this to false for now
                    blnUnzipFile = False

                    Dim objZipfile As Ionic.Zip.ZipFile

                    objZipfile = New Ionic.Zip.ZipFile(strZipFilePathRemote)

                    For Each objEntry As Ionic.Zip.ZipEntry In objZipfile.Entries
                        If Not objEntry.IsDirectory Then

                            Dim strPathToCheck As String
                            strPathToCheck = Path.Combine(strUnzipFolderPathBase, objEntry.FileName.Replace("/"c, "\"c))

                            If Not File.Exists(strPathToCheck) Then
                                blnUnzipFile = True
                                Exit For
                            End If
                        End If
                    Next
                End If

                If blnUnzipFile Then
                    ' Unzip the file to the Chameleon cached data folder
                    ' If UnzipOverNetwork=True, then we want to copy the file locally first

                    If UnzipOverNetwork Then
                        strZipFilePathToExtract = String.Copy(strZipFilePathRemote)
                    Else
                        Try

                            ' Copy the file to the work directory on the local computer
                            strZipFilePathToExtract = Path.Combine(m_WorkingDir, Path.GetFileName(strZipFilePathRemote))

                            If m_DebugLevel >= 2 Then
                                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Copying " + strZipFilePathRemote)
                            End If

                            If Not CopyFileWithRetry(strZipFilePathRemote, strZipFilePathToExtract, True) Then
                                ' Abort processing
                                LogError("Error copying Zip file; unable to process MALDI imaging data")
                                Return False
                            End If

                        Catch ex As Exception
                            LogError("Error copying zipped instrument data, file " + strZipFilePathRemote, ex)
                            Return False
                        End Try
                    End If

                    ' Now use Ionic to unzip strZipFilePathLocal to the data cache folder
                    ' Do not overwrite existing files (assume they're already valid)
                    Try

                        Using objZipfile = New Ionic.Zip.ZipFile(strZipFilePathToExtract)
                            If m_DebugLevel >= 2 Then
                                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Unzipping " + strZipFilePathToExtract)
                            End If

                            objZipfile.ExtractAll(strUnzipFolderPathBase, Ionic.Zip.ExtractExistingFileAction.DoNotOverwrite)
                        End Using

                    Catch ex As Exception
                        LogError("Error extracting zipped instrument data, file " + strZipFilePathToExtract, ex)
                        Return False
                    End Try

                    If Not UnzipOverNetwork Then
                        ' Need to delete the zip file that we copied locally
                        ' However, Ionic may have a file handle open so we use a queue to keep track of files that need to be deleted

                        DeleteQueuedFiles(strFilesToDelete, strZipFilePathToExtract)
                    End If

                End If

            Next

            If Not UnzipOverNetwork Then
                Dim dtStartTime As DateTime = Date.UtcNow

                Do While strFilesToDelete.Count > 0
                    ' Try to process the files remaining in queue strFilesToDelete

                    DeleteQueuedFiles(strFilesToDelete, String.Empty)

                    If strFilesToDelete.Count > 0 Then
                        If Date.UtcNow.Subtract(dtStartTime).TotalSeconds > 20 Then
                            ' Stop trying to delete files; it's not worth continuing to try
                            ReportStatus("Unable to delete all of the files in queue strFilesToDelete; " &
                                         "Queue Length = " + strFilesToDelete.Count.ToString() +
                                         "; this warning can be safely ignored (function RetrieveBrukerMALDIImagingFolders)")
                            Exit Do
                        End If

                        Thread.Sleep(500)
                    End If
                Loop

            End If

        Catch ex As Exception
            LogError("Error extracting zipped instrument data from " + strZipFilePathRemote, ex)
            Return False
        End Try

        ' If we get here, all is fine
        Return True

    End Function

    ''' <summary>
    ''' Unzips dataset folders to working directory
    ''' </summary>
    ''' <returns>TRUE for success; FALSE for failure</returns>
    ''' <remarks></remarks>
    Private Function RetrieveSFolders(createStoragePathInfoOnly As Boolean, maxAttempts As Integer) As Boolean

        Dim ZipFiles() As String
        Dim DSWorkFolder As String
        Dim UnZipper As clsIonicZipTools

        Dim SourceFilePath As String
        Dim TargetFolderPath As String

        Dim ZipFile As String

        Try

            'First Check for the existence of a 0.ser Folder
            'If 0.ser folder exists, then either store the path to the 0.ser folder in a StoragePathInfo file, or copy the 0.ser folder to the working directory
            Dim DSFolderPath As String = FindValidFolder(
              m_DatasetName,
              fileNameToFind:="",
              folderNameToFind:=BRUKER_ZERO_SER_FOLDER,
              maxRetryCount:=maxAttempts,
              logFolderNotFound:=True,
              retrievingInstrumentDataFolder:=True)

            If Not String.IsNullOrEmpty(DSFolderPath) Then
                Dim diSourceFolder As DirectoryInfo
                Dim diTargetFolder As DirectoryInfo
                Dim fiFile As FileInfo

                diSourceFolder = New DirectoryInfo(Path.Combine(DSFolderPath, BRUKER_ZERO_SER_FOLDER))

                If diSourceFolder.Exists Then
                    If createStoragePathInfoOnly Then
                        If CreateStoragePathInfoFile(diSourceFolder.FullName, m_WorkingDir + "\") Then
                            Return True
                        Else
                            Return False
                        End If
                    Else
                        ' Copy the 0.ser folder to the Work directory
                        ' First create the 0.ser subfolder
                        diTargetFolder = Directory.CreateDirectory(Path.Combine(m_WorkingDir, BRUKER_ZERO_SER_FOLDER))

                        ' Now copy the files from the source 0.ser folder to the target folder
                        ' Typically there will only be two files: ACQUS and ser
                        For Each fiFile In diSourceFolder.GetFiles()
                            If Not CopyFileToWorkDir(fiFile.Name, diSourceFolder.FullName, diTargetFolder.FullName) Then
                                ' Error has alredy been logged
                                Return False
                            End If
                        Next

                        Return True
                    End If
                End If

            End If

            'If the 0.ser folder does not exist, unzip the zipped s-folders
            'Copy the zipped s-folders from archive to work directory
            If Not CopySFoldersToWorkDir(createStoragePathInfoOnly) Then
                'Error messages have already been logged, so just exit
                Return False
            End If

            If createStoragePathInfoOnly Then
                ' Nothing was copied locally, so nothing to unzip
                Return True
            End If


            ' Get a listing of the zip files to process
            ZipFiles = Directory.GetFiles(m_WorkingDir, "s*.zip")
            If ZipFiles.GetLength(0) < 1 Then
                LogError("No zipped s-folders found in working directory")
                Return False
            End If

            ' Create a dataset subdirectory under the working directory
            DSWorkFolder = Path.Combine(m_WorkingDir, m_DatasetName)
            Directory.CreateDirectory(DSWorkFolder)

            ' Set up the unzipper
            UnZipper = New clsIonicZipTools(m_DebugLevel, DSWorkFolder)

            ' Unzip each of the zip files to the working directory
            For Each ZipFile In ZipFiles
                If m_DebugLevel > 3 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Unzipping file " + ZipFile)
                End If
                Try
                    TargetFolderPath = Path.Combine(DSWorkFolder, Path.GetFileNameWithoutExtension(ZipFile))
                    Directory.CreateDirectory(TargetFolderPath)

                    SourceFilePath = Path.Combine(m_WorkingDir, Path.GetFileName(ZipFile))

                    If Not UnZipper.UnzipFile(SourceFilePath, TargetFolderPath) Then
                        LogError("Error unzipping file " + ZipFile)
                        Return False
                    End If
                Catch ex As Exception
                    LogError("Exception while unzipping s-folders", ex)
                    Return False
                End Try
            Next

            Thread.Sleep(125)
            PRISM.Processes.clsProgRunner.GarbageCollectNow()

            ' Delete all s*.zip files in working directory
            For Each ZipFile In ZipFiles
                Try
                    File.Delete(Path.Combine(m_WorkingDir, Path.GetFileName(ZipFile)))
                Catch ex As Exception
                    LogError("Exception deleting file " + ZipFile, ex)
                    Return False
                End Try
            Next

        Catch ex As Exception
            LogError("Error in RetrieveSFolders", ex)
            Return False
        End Try

        ' Got to here, so everything must have worked
        Return True

    End Function

    Protected Function RetrieveOrgDB(LocalOrgDBFolder As String) As Boolean
        Dim udtHPCOptions = New udtHPCOptionsType
        Return RetrieveOrgDB(LocalOrgDBFolder, udtHPCOptions)
    End Function

    ''' <summary>
    ''' Uses Ken's dll to create a fasta file for Sequest, X!Tandem, Inspect, or MSGFPlus analysis
    ''' </summary>
    ''' <param name="LocalOrgDBFolder">Folder on analysis machine where fasta files are stored</param>
    ''' <returns>TRUE for success; FALSE for failure</returns>
    ''' <remarks>Stores the name of the FASTA file as a new job parameter named "generatedFastaName" in section "PeptideSearch"</remarks>
    Protected Function RetrieveOrgDB(LocalOrgDBFolder As String, udtHPCOptions As udtHPCOptionsType) As Boolean

        Console.WriteLine()
        If m_DebugLevel >= 3 Then
            ReportStatus("Obtaining org db file")
        End If

        Try
            Dim proteinCollectionInfo = New clsProteinCollectionInfo(m_jobParams)

            Dim requiredFreeSpaceMB As Double = 0

            If proteinCollectionInfo.UsingLegacyFasta Then
                ' Estimate the drive space required to download the fasta file and its associated MSGF+ the index files
                requiredFreeSpaceMB = LookupLegacyDBDiskSpaceRequiredMB(proteinCollectionInfo)
            End If

            If Not udtHPCOptions.UsingHPC Then
                ' Delete old fasta files and suffix array files if getting low on disk space
                ' Do not delete any files related to the current Legacy Fasta file (if defined)

                Const freeSpaceThresholdPercent = 20

                Dim legacyFastaFileBaseName As String = String.Empty
                If proteinCollectionInfo.UsingLegacyFasta AndAlso
                    Not String.IsNullOrWhiteSpace(proteinCollectionInfo.LegacyFastaName) AndAlso
                    Not proteinCollectionInfo.LegacyFastaName.ToLower() = "na" Then

                    legacyFastaFileBaseName = Path.GetFileNameWithoutExtension(proteinCollectionInfo.LegacyFastaName)
                End If

                PurgeFastaFilesIfLowFreeSpace(LocalOrgDBFolder, freeSpaceThresholdPercent, requiredFreeSpaceMB, legacyFastaFileBaseName)
            End If

            ' Make a new fasta file from scratch
            If Not CreateFastaFile(proteinCollectionInfo, LocalOrgDBFolder) Then
                ' There was a problem. Log entries in lower-level routines provide documentation
                Return False
            End If

            'Fasta file was successfully generated. Put the name of the generated fastafile in the
            '	job data class for other methods to use
            If Not m_jobParams.AddAdditionalParameter("PeptideSearch", "generatedFastaName", m_FastaFileName) Then
                LogError("Error adding parameter 'generatedFastaName' to m_jobParams")
                Return False
            End If

            If Not udtHPCOptions.UsingHPC Then
                ' Delete old fasta files and suffix array files if getting low on disk space
                ' No need to pass a value for legacyFastaFileBaseName because a .fasta.LastUsed file will have been created/updated by CreateFastaFile
                Const freeSpaceThresholdPercent = 20
                PurgeFastaFilesIfLowFreeSpace(LocalOrgDBFolder, freeSpaceThresholdPercent, 0, "")
            End If

        Catch ex As Exception
            LogError("Exception in RetrieveOrgDB", ex)
            Return False
        End Try

        'We got to here OK, so return
        Return True

    End Function

    ''' <summary>
    ''' Overrides base class version of the function to creates a Sequest params file compatible 
    '''	with the Bioworks version on this System. Uses ParamFileGenerator dll provided by Ken Auberry
    ''' </summary>
    ''' <param name="paramFileName">Name of param file to be created</param>
    ''' <returns>True for success; False for failure</returns>
    Protected Function RetrieveGeneratedParamFile(paramFileName As String) As Boolean

        Dim ParFileGen As IGenerateFile = Nothing
        Dim blnSuccess As Boolean

        Try
            ReportStatus("Retrieving parameter file " & paramFileName)

            ParFileGen = New ParamFileGenerator.MakeParams.clsMakeParameterFile()
            ParFileGen.TemplateFilePath = m_mgrParams.GetParam("paramtemplateloc")

            ' Note that job parameter "generatedFastaName" gets defined by RetrieveOrgDB
            ' Furthermore, the full path to the fasta file is only necessary when creating Sequest parameter files
            Dim toolName = m_jobParams.GetParam("ToolName", String.Empty)
            If String.IsNullOrWhiteSpace(toolName) Then
                m_message = "Job parameter ToolName is empty"
                Return False
            End If

            Dim paramFileType = SetParamfileType(toolName)
            If paramFileType = IGenerateFile.ParamFileType.Invalid Then
                m_message = "Tool " & toolName & " is not supported by the ParamFileGenerator; update clsAnalysisResources and ParamFileGenerator.dll"
                Return False
            End If

            Dim fastaFilePath = Path.Combine(m_mgrParams.GetParam("orgdbdir"), m_jobParams.GetParam("PeptideSearch", "generatedFastaName"))

            ' Gigasax.DMS5
            Dim connectionString = m_mgrParams.GetParam("connectionstring")
            Dim datasetID As Integer = m_jobParams.GetJobParameter("JobParameters", "DatasetID", 0)

            blnSuccess = ParFileGen.MakeFile(paramFileName, paramFileType, fastaFilePath, m_WorkingDir, connectionString, datasetID)


            ' Examine the size of the ModDefs.txt file
            ' Add it to the ignore list if it is empty (no point in tracking a 0-byte file)
            Dim fiModDefs = New FileInfo(Path.Combine(m_WorkingDir, Path.GetFileNameWithoutExtension(paramFileName) & "_ModDefs.txt"))
            If fiModDefs.Exists AndAlso fiModDefs.Length = 0 Then
                m_jobParams.AddResultFileToSkip(fiModDefs.Name)
            End If

            If blnSuccess Then
                If m_DebugLevel >= 3 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Successfully retrieved param file: " + paramFileName)
                End If

                Return True
            Else
                LogError(m_message + ": " + ParFileGen.LastError)
                Return False
            End If

        Catch ex As Exception
            If String.IsNullOrWhiteSpace(m_message) Then
                m_message = "Error retrieving parameter file"
            End If

            LogError(m_message, ex)

            If Not ParFileGen Is Nothing Then
                If Not String.IsNullOrWhiteSpace(ParFileGen.LastError) Then
                    ReportStatus("Error converting param file: " + ParFileGen.LastError, 0, True)
                End If
            End If
            Return False
        End Try

    End Function

    ''' <summary>
    ''' This is just a generic function to copy files to the working directory
    ''' </summary>
    ''' <param name="fileName">Name of file to be copied</param>
    ''' <param name="sourceFolderPath">Source folder that has the file</param>
    ''' <returns>TRUE for success; FALSE for failure</returns>
    Protected Function RetrieveFile(fileName As String, sourceFolderPath As String) As Boolean

        'Copy the file
        If Not CopyFileToWorkDir(fileName, sourceFolderPath, m_WorkingDir, clsLogTools.LogLevels.ERROR) Then
            Return False
        End If

        Return True

    End Function

    ''' <summary>
    ''' This is just a generic function to copy files to the working directory
    ''' </summary>
    ''' <param name="fileName">Name of file to be copied</param>
    ''' <param name="sourceFolderPath">Source folder that has the file</param>
    ''' <param name="maxCopyAttempts">Maximum number of attempts to make when errors are encountered while copying the file</param>
    ''' <param name="eLogMsgTypeIfNotFound">Type of message to log if the file is not found</param>
    ''' <returns>TRUE for success; FALSE for failure</returns>
    Protected Function RetrieveFile(
      fileName As String,
      sourceFolderPath As String,
      maxCopyAttempts As Integer,
      Optional eLogMsgTypeIfNotFound As clsLogTools.LogLevels = clsLogTools.LogLevels.ERROR) As Boolean

        'Copy the file
        If maxCopyAttempts < 1 Then maxCopyAttempts = 1
        If Not CopyFileToWorkDir(fileName, sourceFolderPath, m_WorkingDir, clsLogTools.LogLevels.ERROR, createStoragePathInfoOnly:=False, maxCopyAttempts:=maxCopyAttempts) Then
            Return False
        End If

        Return True

    End Function

    ''' <summary>
    ''' Finds the _DTA.txt file for this dataset
    ''' </summary>
    ''' <returns>The path to the _dta.zip file (or _dta.txt file)</returns>
    ''' <remarks></remarks>
    Protected Function FindCDTAFile(<Out()> ByRef strErrorMessage As String) As String

        strErrorMessage = String.Empty

        'Retrieve zipped DTA file
        Dim sourceFileName = m_DatasetName & "_dta.zip"
        Dim sourceFolderPath = FindDataFile(sourceFileName)

        If Not String.IsNullOrEmpty(sourceFolderPath) Then
            If sourceFolderPath.StartsWith(MYEMSL_PATH_FLAG) Then
                ' add the _dta.zip file name to the folder path found by FindDataFile
                Return clsMyEMSLUtilities.AddFileToMyEMSLFolderPath(sourceFolderPath, sourceFileName)
            Else
                ' Return the path to the _dta.zip file
                Return Path.Combine(sourceFolderPath, sourceFileName)
            End If
        End If

        ' Couldn't find a folder with the _dta.zip file; how about the _dta.txt file?

        sourceFileName = m_DatasetName + "_dta.txt"
        sourceFolderPath = FindDataFile(sourceFileName)

        If String.IsNullOrEmpty(sourceFolderPath) Then
            ' No folder found containing the zipped DTA files; return False
            ' (the FindDataFile procedure should have already logged an error)
            strErrorMessage = "Could not find " + sourceFileName + " using FindDataFile"
            Return String.Empty
        Else
            ReportStatus("Warning: could not find the _dta.zip file, but was able to find " + sourceFileName + " in folder " + sourceFolderPath)

            If sourceFolderPath.StartsWith(MYEMSL_PATH_FLAG) Then
                Return sourceFolderPath
            Else
                ' Return the path to the _dta.txt file
                Return Path.Combine(sourceFolderPath, sourceFileName)
            End If

        End If

    End Function

    ''' <summary>
    ''' Finds the .pbf (PNNL Binary Format) file for this dataset
    ''' </summary>
    ''' <returns>The path to the .pbf file</returns>
    ''' <remarks></remarks>
    Protected Function FindPBFFile(<Out()> ByRef strErrorMessage As String) As String

        Dim SourceFileName As String
        Dim SourceFolderPath As String

        strErrorMessage = String.Empty

        SourceFileName = m_DatasetName + DOT_PBF_EXTENSION
        SourceFolderPath = FindDataFile(SourceFileName)

        If Not String.IsNullOrEmpty(SourceFolderPath) Then
            If SourceFolderPath.StartsWith(MYEMSL_PATH_FLAG) Then
                Return SourceFolderPath
            Else
                ' Return the path to the .pbf file
                Return Path.Combine(SourceFolderPath, SourceFileName)
            End If
        End If

        strErrorMessage = "Could not find " + SourceFileName + " using FindDataFile"
        Return String.Empty

    End Function

    ''' <summary>
    ''' Retrieves the _DTA.txt file (either zipped or unzipped).  
    ''' </summary>
    ''' <returns>TRUE for success, FALSE for error</returns>
    ''' <remarks>If the _dta.zip or _dta.txt file already exists in the working folder then will not re-copy it from the remote folder</remarks>
    Public Function RetrieveDtaFiles() As Boolean

        Dim TargetZipFilePath As String = Path.Combine(m_WorkingDir, m_DatasetName + "_dta.zip")
        Dim TargetCDTAFilePath As String = Path.Combine(m_WorkingDir, m_DatasetName + "_dta.txt")

        If Not File.Exists(TargetCDTAFilePath) And Not File.Exists(TargetZipFilePath) Then

            Dim SourceFilePath As String
            Dim strErrorMessage As String = String.Empty

            ' Find the CDTA file
            SourceFilePath = FindCDTAFile(strErrorMessage)

            If String.IsNullOrEmpty(SourceFilePath) Then
                LogError(strErrorMessage)
                Return False
            End If

            If SourceFilePath.StartsWith(MYEMSL_PATH_FLAG) Then
                m_MyEMSLUtilities.AddFileToDownloadQueue(SourceFilePath)
                If m_MyEMSLUtilities.ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders) Then
                    If m_DebugLevel >= 1 Then
                        ReportStatus("Downloaded " + m_MyEMSLUtilities.DownloadedFiles.First().Value.Filename + " from MyEMSL")
                    End If
                Else
                    Return False
                End If
            Else

                Dim fiSourceFile = New FileInfo(SourceFilePath)

                ' Copy the file locally
                If Not CopyFileToWorkDir(fiSourceFile.Name, fiSourceFile.Directory.FullName, m_WorkingDir, clsLogTools.LogLevels.ERROR) Then
                    m_message = "Error copying " + fiSourceFile.Name
                    If m_DebugLevel >= 2 Then
                        ReportStatus("CopyFileToWorkDir returned False for " + fiSourceFile.Name + " using folder " + fiSourceFile.Directory.FullName, 0, True)
                    End If
                    Return False
                Else
                    If m_DebugLevel >= 1 Then
                        ReportStatus("Copied " + fiSourceFile.Name + " from folder " + fiSourceFile.FullName)
                    End If
                End If
            End If

        End If

        If Not File.Exists(TargetCDTAFilePath) Then

            If Not File.Exists(TargetZipFilePath) Then
                LogError(Path.GetFileName(TargetZipFilePath) & " not found in the working directory; cannot unzip in RetrieveDtaFiles")
                Return False
            End If

            ' Unzip concatenated DTA file
            ReportStatus("Unzipping concatenated DTA file")
            If UnzipFileStart(TargetZipFilePath, m_WorkingDir, "RetrieveDtaFiles", False) Then
                If m_DebugLevel >= 1 Then
                    LogDebugMessage("Concatenated DTA file unzipped")
                End If
            End If

            ' Delete the _DTA.zip file to free up some disk space
            Thread.Sleep(100)
            If m_DebugLevel >= 3 Then
                LogDebugMessage("Deleting the _DTA.zip file")
            End If

            Try
                Thread.Sleep(125)
                PRISM.Processes.clsProgRunner.GarbageCollectNow()

                File.Delete(TargetZipFilePath)
            Catch ex As Exception
                ReportStatus("Error deleting the _DTA.zip file", ex)
            End Try

        End If

        Return True

    End Function

    ''' <summary>
    ''' Retrieves zipped, concatenated OUT file, unzips, and splits into individual OUT files
    ''' </summary>
    ''' <param name="UnConcatenate">TRUE to split concatenated file; FALSE to leave the file concatenated</param>
    ''' <returns>TRUE for success, FALSE for error</returns>
    ''' <remarks></remarks>
    Protected Function RetrieveOutFiles(UnConcatenate As Boolean) As Boolean

        'Retrieve zipped OUT file
        Dim ZippedFileName As String = m_DatasetName + "_out.zip"
        Dim ZippedFolderName As String = FindDataFile(ZippedFileName)

        If ZippedFolderName = "" Then Return False 'No folder found containing the zipped OUT files
        'Copy the file
        If Not CopyFileToWorkDir(ZippedFileName, ZippedFolderName, m_WorkingDir, clsLogTools.LogLevels.ERROR) Then
            Return False
        End If

        'Unzip concatenated OUT file
        ReportStatus("Unzipping concatenated OUT file")
        If UnzipFileStart(Path.Combine(m_WorkingDir, ZippedFileName), m_WorkingDir, "RetrieveOutFiles", False) Then
            If m_DebugLevel >= 1 Then
                ReportStatus("Concatenated OUT file unzipped")
            End If
        End If

        'Unconcatenate OUT file if needed
        If UnConcatenate Then
            ReportStatus("Splitting concatenated OUT file")

            Dim fiSourceFile As FileInfo
            fiSourceFile = New FileInfo(Path.Combine(m_WorkingDir, m_DatasetName + "_out.txt"))

            If Not fiSourceFile.Exists Then
                m_message = "_OUT.txt file not found after unzipping"
                Return False
            ElseIf fiSourceFile.Length = 0 Then
                m_message = "_OUT.txt file is empty (zero-bytes)"
                Return False
            End If

            Dim FileSplitter As New clsSplitCattedFiles()
            FileSplitter.SplitCattedOutsOnly(m_DatasetName, m_WorkingDir)

            If m_DebugLevel >= 1 Then
                LogDebugMessage("Completed splitting concatenated OUT file")
            End If
        End If

        Return True

    End Function

    ''' <summary>
    ''' Creates the specified settings file from db info
    ''' </summary>
    ''' <returns>TRUE if file created successfully; FALSE otherwise</returns>
    ''' <remarks>Use this overload with jobs where settings file is retrieved from database</remarks>
    Protected Friend Function RetrieveSettingsFileFromDb() As Boolean

        Dim OutputFile As String = Path.Combine(m_WorkingDir, m_jobParams.GetParam("SettingsFileName"))

        Return CreateSettingsFile(m_jobParams.GetParam("ParameterXML"), OutputFile)

    End Function

    ''' <summary>
    ''' Returns True if the filename ends with any of the suffixes in lstNonCriticalFileSuffixes
    ''' </summary>
    ''' <param name="strFileName"></param>
    ''' <param name="lstNonCriticalFileSuffixes"></param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Protected Function SafeToIgnore(strFileName As String, lstNonCriticalFileSuffixes As List(Of String)) As Boolean

        If Not lstNonCriticalFileSuffixes Is Nothing Then

            strFileName = strFileName.ToLower()
            For Each strSuffix As String In lstNonCriticalFileSuffixes
                If strFileName.EndsWith(strSuffix.ToLower()) Then
                    ' It's OK that this file is missing
                    Return True
                End If
            Next

        End If

        Return False

    End Function

    ''' <summary>
    ''' Specifies the Bioworks version for use by the Param File Generator DLL
    ''' </summary>
    ''' <param name="toolName">Version specified in mgr config file</param>
    ''' <returns>IGenerateFile.ParamFileType based on input version</returns>
    ''' <remarks></remarks>
    Protected Function SetParamfileType(toolName As String) As IGenerateFile.ParamFileType

        Dim toolNameToTypeMapping = New Dictionary(Of String, IGenerateFile.ParamFileType)(StringComparer.CurrentCultureIgnoreCase) From
            {{"sequest", IGenerateFile.ParamFileType.BioWorks_Current},
             {"xtandem", IGenerateFile.ParamFileType.X_Tandem},
             {"inspect", IGenerateFile.ParamFileType.Inspect},
             {"msgfplus", IGenerateFile.ParamFileType.MSGFPlus},
             {"msalign_histone", IGenerateFile.ParamFileType.MSAlignHistone},
             {"msalign", IGenerateFile.ParamFileType.MSAlign},
             {"moda", IGenerateFile.ParamFileType.MODa},
             {"mspathfinder", IGenerateFile.ParamFileType.MSPathFinder},
             {"modplus", IGenerateFile.ParamFileType.MODPlus}
            }

        Dim paramFileType As IGenerateFile.ParamFileType

        If toolNameToTypeMapping.TryGetValue(toolName, paramFileType) Then
            Return paramFileType
        End If

        Dim strToolNameLCase = toolName.ToLower()

        For Each entry In toolNameToTypeMapping
            If strToolNameLCase.Contains(entry.Key.ToLower()) Then
                Return entry.Value
            End If
        Next

        Return IGenerateFile.ParamFileType.Invalid

    End Function

    ''' <summary>
    ''' Converts the dictionary items to a list of key/value pairs separated by an equals sign
    ''' Next, calls StorePackedJobParameterList to store the list (items will be separated by tab characters)
    ''' </summary>
    ''' <param name="dctItems">Dictionary items to store as a packed job parameter</param>
    ''' <param name="strParameterName">Packed job parameter name</param>
    ''' <remarks></remarks>
    Protected Sub StorePackedJobParameterDictionary(dctItems As Dictionary(Of String, Integer), strParameterName As String)

        Dim lstItems = New List(Of String)

        For Each item As KeyValuePair(Of String, Integer) In dctItems
            lstItems.Add(item.Key & "=" & item.Value)
        Next

        StorePackedJobParameterList(lstItems, strParameterName)

    End Sub
    ''' <summary>
    ''' Converts the dictionary items to a list of key/value pairs separated by an equals sign
    ''' Next, calls StorePackedJobParameterList to store the list (items will be separated by tab characters)
    ''' </summary>
    ''' <param name="dctItems">Dictionary items to store as a packed job parameter</param>
    ''' <param name="strParameterName">Packed job parameter name</param>
    ''' <remarks></remarks>
    Protected Sub StorePackedJobParameterDictionary(dctItems As Dictionary(Of String, String), strParameterName As String)

        Dim lstItems = New List(Of String)

        For Each item As KeyValuePair(Of String, String) In dctItems
            lstItems.Add(item.Key & "=" & item.Value)
        Next

        StorePackedJobParameterList(lstItems, strParameterName)

    End Sub

    ''' <summary>
    ''' Convert a string list to a packed job parameter (items are separated by tab characters)
    ''' </summary>
    ''' <param name="lstItems">List items to store as a packed job parameter</param>
    ''' <param name="strParameterName">Packed job parameter name</param>
    ''' <remarks></remarks>
    Protected Sub StorePackedJobParameterList(lstItems As List(Of String), strParameterName As String)

        m_jobParams.AddAdditionalParameter("JobParameters", strParameterName, clsGlobal.FlattenList(lstItems, ControlChars.Tab))

    End Sub

    ''' <summary>
    ''' Unzips all files in the specified Zip file
    ''' If the file is less than 1.25 GB in size (IONIC_ZIP_MAX_FILESIZE_MB) then uses Ionic.Zip
    ''' Otherwise, uses PKZipC (provided PKZipC.exe exists)
    ''' </summary>
    ''' <param name="zipFilePath">File to unzip</param>
    ''' <param name="outFolderPath">Target directory for the extracted files</param>
    ''' <param name="callingFunctionName">Calling function name (used for debugging purposes)</param>
    ''' <param name="forceExternalZipProgramUse">If True, then force use of PKZipC.exe</param>
    ''' <returns>True if success, otherwise false</returns>
    ''' <remarks></remarks>
    Public Function UnzipFileStart(
      zipFilePath As String,
      outFolderPath As String,
      callingFunctionName As String,
      forceExternalZipProgramUse As Boolean) As Boolean

        Dim strUnzipperName As String = "??"

        Try
            If String.IsNullOrEmpty(callingFunctionName) Then
                callingFunctionName = "??"
            End If

            If zipFilePath Is Nothing Then
                LogError(callingFunctionName & " called UnzipFileStart with an empty file path")
                Return False
            End If

            Dim strExternalUnzipperFilePath = m_mgrParams.GetParam("zipprogram", String.Empty)

            Dim fiFileInfo = New FileInfo(zipFilePath)
            Dim fileSizeMB = clsGlobal.BytesToMB(fiFileInfo.Length)

            If Not fiFileInfo.Exists Then
                ' File not found
                LogError("Error unzipping '" + zipFilePath + "': File not found")
                ReportStatus("CallingFunction: " & callingFunctionName)
                Return False
            End If

            Dim blnUseExternalUnzipper As Boolean

            If zipFilePath.ToLower().EndsWith(DOT_GZ_EXTENSION) Then
                ' This is a gzipped file
                ' Use Ionic.Zip
                strUnzipperName = clsIonicZipTools.IONIC_ZIP_NAME
                m_IonicZipTools.DebugLevel = m_DebugLevel
                Return m_IonicZipTools.GUnzipFile(zipFilePath, outFolderPath)
            End If

            ' Use the external zipper if the file size is over IONIC_ZIP_MAX_FILESIZE_MB or if ForceExternalZipProgramUse = True
            ' However, if the .Exe file for the external zipper is not found, then fall back to use Ionic.Zip
            If forceExternalZipProgramUse OrElse fileSizeMB >= IONIC_ZIP_MAX_FILESIZE_MB Then
                If strExternalUnzipperFilePath.Length > 0 AndAlso
                   strExternalUnzipperFilePath.ToLower() <> "na" Then
                    If File.Exists(strExternalUnzipperFilePath) Then
                        blnUseExternalUnzipper = True
                    End If
                End If

                If Not blnUseExternalUnzipper Then
                    ReportStatus("External zip program not found: " + strExternalUnzipperFilePath + "; will instead use Ionic.Zip")
                End If
            End If

            If blnUseExternalUnzipper Then
                strUnzipperName = Path.GetFileName(strExternalUnzipperFilePath)

                Dim UnZipper As New PRISM.Files.ZipTools(outFolderPath, strExternalUnzipperFilePath)

                Dim dtStartTime = Date.UtcNow
                Dim blnSuccess = UnZipper.UnzipFile("", zipFilePath, outFolderPath)
                Dim dtEndTime = Date.UtcNow

                If blnSuccess Then
                    m_IonicZipTools.ReportZipStats(fiFileInfo, dtStartTime, dtEndTime, False, strUnzipperName)
                Else
                    LogError("Error unzipping " + Path.GetFileName(zipFilePath) + " using " + strUnzipperName)
                    ReportStatus("CallingFunction: " & callingFunctionName)
                End If

                Return blnSuccess
            Else
                ' Use Ionic.Zip
                strUnzipperName = clsIonicZipTools.IONIC_ZIP_NAME
                m_IonicZipTools.DebugLevel = m_DebugLevel
                Dim blnSuccess = m_IonicZipTools.UnzipFile(zipFilePath, outFolderPath)

                Return blnSuccess
            End If

        Catch ex As Exception
            Dim errMsg = "Exception while unzipping '" + zipFilePath + "'"
            If Not String.IsNullOrEmpty(strUnzipperName) Then errMsg &= " using " + strUnzipperName

            LogError(errMsg, ex)
            ReportStatus("CallingFunction: " & callingFunctionName)
            Return False
        End Try

    End Function

    ''' <summary>
    ''' Removes any spectra with 2 or fewer ions in a _DTA.txt ifle
    ''' </summary>
    ''' <param name="strWorkDir">Folder with the CDTA file</param>
    ''' <param name="strInputFileName">CDTA filename</param>
    ''' <returns>True if success; false if an error</returns>
    Protected Function ValidateCDTAFileRemoveSparseSpectra(strWorkDir As String, strInputFileName As String) As Boolean
        Dim blnSuccess As Boolean

        blnSuccess = m_CDTAUtilities.RemoveSparseSpectra(strWorkDir, strInputFileName)
        If Not blnSuccess AndAlso String.IsNullOrEmpty(m_message) Then
            m_message = "m_CDTAUtilities.RemoveSparseSpectra returned False"
        End If

        Return blnSuccess

    End Function

    ''' <summary>
    ''' Makes sure the specified _DTA.txt file has scan=x and cs=y tags in the parent ion line
    ''' </summary>
    ''' <param name="strSourceFilePath">Input _DTA.txt file to parse</param>
    ''' <param name="blnReplaceSourceFile">If True, then replaces the source file with and updated file</param>
    ''' <param name="blnDeleteSourceFileIfUpdated">Only valid if blnReplaceSourceFile=True: If True, then the source file is deleted if an updated version is created. If false, then the source file is renamed to .old if an updated version is created.</param>
    ''' <param name="strOutputFilePath">Output file path to use for the updated file; required if blnReplaceSourceFile=False; ignored if blnReplaceSourceFile=True</param>
    ''' <returns>True if success; false if an error</returns>
    Protected Function ValidateCDTAFileScanAndCSTags(
       strSourceFilePath As String,
       blnReplaceSourceFile As Boolean,
       blnDeleteSourceFileIfUpdated As Boolean,
       strOutputFilePath As String) As Boolean

        Dim blnSuccess As Boolean

        blnSuccess = m_CDTAUtilities.ValidateCDTAFileScanAndCSTags(strSourceFilePath, blnReplaceSourceFile, blnDeleteSourceFileIfUpdated, strOutputFilePath)
        If Not blnSuccess AndAlso String.IsNullOrEmpty(m_message) Then
            m_message = "m_CDTAUtilities.ValidateCDTAFileScanAndCSTags returned False"
        End If

        Return blnSuccess

    End Function

    ''' <summary>
    ''' Condenses CDTA files that are over 2 GB in size
    ''' </summary>
    ''' <param name="strWorkDir"></param>
    ''' <param name="strInputFileName"></param>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Protected Function ValidateCDTAFileSize(strWorkDir As String, strInputFileName As String) As Boolean
        Dim blnSuccess As Boolean

        blnSuccess = m_CDTAUtilities.ValidateCDTAFileSize(strWorkDir, strInputFileName)
        If Not blnSuccess AndAlso String.IsNullOrEmpty(m_message) Then
            m_message = "m_CDTAUtilities.ValidateCDTAFileSize returned False"
        End If

        Return blnSuccess

    End Function

    Public Function ValidateCDTAFileIsCentroided(strCDTAPath As String) As Boolean

        Try

            ' Read the m/z values in the _dta.txt file
            ' Examine the data in each spectrum to determine if it is centroided

            mSpectraTypeClassifier = New SpectraTypeClassifier.clsSpectrumTypeClassifier()

            Dim blnSuccess = mSpectraTypeClassifier.CheckCDTAFile(strCDTAPath)

            If Not blnSuccess Then
                If String.IsNullOrEmpty(m_message) Then
                    LogError("SpectraTypeClassifier encountered an error while parsing the _dta.txt file")
                End If

                Return False
            End If

            Dim fractionCentroided = mSpectraTypeClassifier.FractionCentroided

            Dim commentSuffix = " (" & mSpectraTypeClassifier.TotalSpectra & " total spectra)"

            If fractionCentroided > 0.8 Then
                ' At least 80% of the spectra are centroided

                If fractionCentroided > 0.999 Then
                    ReportStatus("All of the spectra are centroided" & commentSuffix)
                Else
                    ReportStatus((fractionCentroided * 100).ToString("0") & "% of the spectra are centroided" & commentSuffix)
                End If

                Return True

            ElseIf fractionCentroided > 0.001 Then
                ' Less than 80% of the spectra are centroided
                ' Post a message similar to:
                '   MSGF+ will likely skip 90% of the spectra because they did not appear centroided
                m_message = "MSGF+ will likely skip " & ((1 - fractionCentroided) * 100).ToString("0") & "% of the spectra because they do not appear centroided"
                ReportStatus(m_message & commentSuffix)
                Return False
            Else
                ' None of the spectra are centroided; unable to process with MSGF+
                m_message = SPECTRA_ARE_NOT_CENTROIDED & " with MSGF+"
                ReportStatus(m_message & commentSuffix, 0, True)
                Return False
            End If

        Catch ex As Exception
            LogError("Exception in ValidateCDTAFileIsCentroided", ex)
            Return False
        End Try

    End Function

    ''' <summary>
    ''' Validate that the specified file exists and has at least one tab-delimited row with a numeric value in the first column
    ''' </summary>
    ''' <param name="strFilePath">Path to the file</param>
    ''' <param name="strFileDescription">File description, e.g. Synopsis</param>
    ''' <returns>True if the file has data; otherwise false</returns>
    ''' <remarks></remarks>
    Public Shared Function ValidateFileHasData(strFilePath As String, strFileDescription As String, <Out()> ByRef strErrorMessage As String) As Boolean
        Const intNumericDataColIndex = 0
        Return ValidateFileHasData(strFilePath, strFileDescription, strErrorMessage, intNumericDataColIndex)
    End Function

    ''' <summary>
    ''' Validate that the specified file exists and has at least one tab-delimited row with a numeric value
    ''' </summary>
    ''' <param name="strFilePath">Path to the file</param>
    ''' <param name="strFileDescription">File description, e.g. Synopsis</param>
    ''' <param name="intNumericDataColIndex">Index of the numeric data column; use -1 to simply look for any text in the file</param>
    ''' <returns>True if the file has data; otherwise false</returns>
    ''' <remarks></remarks>
    Public Shared Function ValidateFileHasData(strFilePath As String, strFileDescription As String, <Out()> ByRef strErrorMessage As String, intNumericDataColIndex As Integer) As Boolean

        Dim fiFileInfo As FileInfo

        Dim strLineIn As String
        Dim strSplitLine() As String

        Dim dblValue As Double
        Dim blnDataFound As Boolean

        strErrorMessage = String.Empty

        Try
            fiFileInfo = New FileInfo(strFilePath)

            If Not fiFileInfo.Exists Then
                strErrorMessage = strFileDescription + " file not found: " + fiFileInfo.Name
                Return False
            End If

            If fiFileInfo.Length = 0 Then
                strErrorMessage = strFileDescription + " file is empty (zero-bytes)"
                Return False
            End If

            ' Open the file and confirm it has data rows
            Using srInFile = New StreamReader(New FileStream(fiFileInfo.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                While Not srInFile.EndOfStream And Not blnDataFound
                    strLineIn = srInFile.ReadLine()
                    If Not String.IsNullOrEmpty(strLineIn) Then
                        If intNumericDataColIndex < 0 Then
                            blnDataFound = True
                        Else
                            ' Split on the tab character and check if the first column is numeric
                            strSplitLine = strLineIn.Split(ControlChars.Tab)

                            If Not strSplitLine Is Nothing AndAlso strSplitLine.Length > intNumericDataColIndex Then
                                If Double.TryParse(strSplitLine(intNumericDataColIndex), dblValue) Then
                                    blnDataFound = True
                                End If
                            End If

                        End If
                    End If
                End While
            End Using

            If Not blnDataFound Then
                strErrorMessage = strFileDescription + " is empty (no data)"
            End If

        Catch ex As Exception
            strErrorMessage = "Exception validating " + strFileDescription + " file"
            Return False
        End Try

        Return blnDataFound

    End Function

    ''' <summary>
    ''' Validates that sufficient free memory is available to run Java
    ''' </summary>
    ''' <param name="strJavaMemorySizeJobParamName">Name of the job parameter that defines the amount of memory (in MB) to reserve for Java</param>
    ''' <param name="strStepToolName">Step tool name to use when posting log entries</param>
    ''' <returns>True if sufficient free memory; false if not enough free memory</returns>
    ''' <remarks>Typical names for strJavaMemorySizeJobParamName are MSGFJavaMemorySize, MSGFDBJavaMemorySize, and MSDeconvJavaMemorySize.  
    ''' These parameters are loaded from DMS Settings Files (table T_Settings_Files in DMS5, copied to table T_Job_Parameters in DMS_Pipeline) </remarks>
    Protected Function ValidateFreeMemorySize(strJavaMemorySizeJobParamName As String, strStepToolName As String) As Boolean

        Const blnLogFreeMemoryOnSuccess = True
        Return ValidateFreeMemorySize(strJavaMemorySizeJobParamName, strStepToolName, blnLogFreeMemoryOnSuccess)

    End Function

    ''' <summary>
    ''' Validates that sufficient free memory is available to run Java
    ''' </summary>
    ''' <param name="strMemorySizeJobParamName">Name of the job parameter that defines the amount of memory (in MB) that must be available on the system</param>
    ''' <param name="strStepToolName">Step tool name to use when posting log entries</param>
    ''' <param name="blnLogFreeMemoryOnSuccess">If True, then post a log entry if sufficient memory is, in fact, available</param>
    ''' <returns>True if sufficient free memory; false if not enough free memory</returns>
    ''' <remarks>Typical names for strJavaMemorySizeJobParamName are MSGFJavaMemorySize, MSGFDBJavaMemorySize, and MSDeconvJavaMemorySize.  
    ''' These parameters are loaded from DMS Settings Files (table T_Settings_Files in DMS5, copied to table T_Job_Parameters in DMS_Pipeline) </remarks>

    Protected Function ValidateFreeMemorySize(strMemorySizeJobParamName As String, strStepToolName As String, blnLogFreeMemoryOnSuccess As Boolean) As Boolean
        Dim intFreeMemoryRequiredMB As Integer

        ' Lookup parameter strMemorySizeJobParamName; assume 2000 MB if not defined
        intFreeMemoryRequiredMB = m_jobParams.GetJobParameter(strMemorySizeJobParamName, 2000)

        ' Require intFreeMemoryRequiredMB be at least 0.5 GB
        If intFreeMemoryRequiredMB < 512 Then intFreeMemoryRequiredMB = 512

        If m_DebugLevel < 1 Then blnLogFreeMemoryOnSuccess = False

        Return ValidateFreeMemorySize(intFreeMemoryRequiredMB, strStepToolName, blnLogFreeMemoryOnSuccess)

    End Function

    Public Shared Function ValidateFreeMemorySize(intFreeMemoryRequiredMB As Integer, strStepToolName As String, blnLogFreeMemoryOnSuccess As Boolean) As Boolean
        Dim sngFreeMemoryMB As Single
        Dim strMessage As String

        sngFreeMemoryMB = GetFreeMemoryMB()

        If intFreeMemoryRequiredMB >= sngFreeMemoryMB Then
            strMessage = "Not enough free memory to run " + strStepToolName

            strMessage &= "; need " + intFreeMemoryRequiredMB.ToString() + " MB but system has " + sngFreeMemoryMB.ToString("0") + " MB available"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, strMessage)
            Console.WriteLine(strMessage)
            Return False
        Else
            If blnLogFreeMemoryOnSuccess Then
                strMessage = strStepToolName + " will use " + intFreeMemoryRequiredMB.ToString() + " MB; system has " + sngFreeMemoryMB.ToString("0") + " MB available"
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, strMessage)
            End If

            Return True
        End If
    End Function

#End Region

#Region "Event Handlers"

    Private Sub m_CDTAUtilities_ErrorEvent(errorMessage As String) Handles m_CDTAUtilities.ErrorEvent
        LogError(errorMessage)
    End Sub

    Private Sub m_CDTAUtilities_InfoEvent(strMessage As String, DebugLevel As Integer) Handles m_CDTAUtilities.InfoEvent
        If m_DebugLevel >= DebugLevel Then
            ReportStatus(strMessage)
        End If
    End Sub

    Private Sub m_CDTAUtilities_ProgressEvent(taskDescription As String, percentComplete As Single) Handles m_CDTAUtilities.ProgressEvent

        Static dtLastUpdateTime As DateTime

        If m_DebugLevel >= 1 Then
            If m_DebugLevel = 1 AndAlso Date.UtcNow.Subtract(dtLastUpdateTime).TotalSeconds >= 60 OrElse
               m_DebugLevel > 1 AndAlso Date.UtcNow.Subtract(dtLastUpdateTime).TotalSeconds >= 20 Then
                dtLastUpdateTime = Date.UtcNow

                LogDebugMessage(" ... CDTAUtilities: " & percentComplete.ToString("0.00") & "% complete")
            End If
        End If

    End Sub

    Private Sub m_CDTAUtilities_WarningEvent(strMessage As String) Handles m_CDTAUtilities.WarningEvent
        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, strMessage)
        Console.WriteLine(strMessage)
    End Sub

    Private Sub m_FastaTools_FileGenerationCompleted(FullOutputPath As String) Handles m_FastaTools.FileGenerationCompleted
        ' Get the name of the fasta file that was generated
        m_FastaFileName = Path.GetFileName(FullOutputPath)
    End Sub

    Private Sub m_FastaTools_FileGenerationProgress(statusMsg As String, fractionDone As Double) Handles m_FastaTools.FileGenerationProgress
        Const MINIMUM_LOG_INTERVAL_SEC = 10
        Static dtLastLogTime As DateTime = Date.UtcNow.Subtract(New TimeSpan(1, 0, 0))
        Static dblFractionDoneSaved As Double = -1

        Dim blnForcelog = m_DebugLevel >= 1 AndAlso statusMsg.Contains(Protein_Exporter.clsGetFASTAFromDMS.LOCK_FILE_PROGRESS_TEXT)

        If m_DebugLevel >= 3 OrElse blnForcelog Then
            ' Limit the logging to once every MINIMUM_LOG_INTERVAL_SEC seconds
            If blnForcelog OrElse
               Date.UtcNow.Subtract(dtLastLogTime).TotalSeconds >= MINIMUM_LOG_INTERVAL_SEC OrElse
               fractionDone - dblFractionDoneSaved >= 0.25 Then
                dtLastLogTime = Date.UtcNow
                dblFractionDoneSaved = fractionDone
                LogDebugMessage("Generating Fasta file, " + (fractionDone * 100).ToString("0.0") + "% complete, " + statusMsg)
            End If
        End If
    End Sub

    Private Sub m_SplitFastaFileUtility_ErrorEvent(strMessage As String) Handles m_SplitFastaFileUtility.ErrorEvent
        ReportStatus(strMessage, 0, True)
    End Sub

    Private Sub m_SplitFastaFileUtility_ProgressUpdate(progressMessage As String, percentComplete As Integer) Handles m_SplitFastaFileUtility.ProgressUpdate

        If m_DebugLevel >= 1 Then
            If m_DebugLevel = 1 AndAlso Date.UtcNow.Subtract(m_SplitFastaLastUpdateTime).TotalSeconds >= 60 OrElse
               m_DebugLevel > 1 AndAlso Date.UtcNow.Subtract(m_SplitFastaLastUpdateTime).TotalSeconds >= 20 OrElse
               percentComplete = 100 And m_SplitFastaLastPercentComplete < 100 Then

                m_SplitFastaLastUpdateTime = Date.UtcNow
                m_SplitFastaLastPercentComplete = percentComplete

                If percentComplete > 0 Then
                    LogDebugMessage(" ... " & progressMessage & ", " & percentComplete & "% complete")
                Else
                    LogDebugMessage(" ... SplitFastaFile: " & progressMessage)
                End If

            End If
        End If
    End Sub

    Private Sub m_SplitFastaFileUtility_SplittingBaseFastafile(strBaseFastaFileName As String, numSplitParts As Integer) Handles m_SplitFastaFileUtility.SplittingBaseFastafile
        LogDebugMessage("Splitting " & strBaseFastaFileName & " into " & numSplitParts & " parts")
    End Sub

#End Region

#Region "MyEMSL Event Handlers"

    Private Sub m_MyEMSLUtilities_ErrorEvent(strMessage As String)
        LogError(m_message)
    End Sub

    Private Sub m_MyEMSLUtilities_WarningEvent(strMessage As String)
        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, strMessage)
        Console.WriteLine(strMessage)
    End Sub

#End Region

#Region "SpectraTypeClassifier Events"
    Private Sub mSpectraTypeClassifier_ErrorEvent(strMessage As String) Handles mSpectraTypeClassifier.ErrorEvent

    End Sub

    Private Sub mSpectraTypeClassifier_ReadingSpectra(spectraProcessed As Integer) Handles mSpectraTypeClassifier.ReadingSpectra
        LogDebugMessage(" ... " & spectraProcessed & " spectra parsed in the _dta.txt file")
    End Sub
#End Region

End Class


