'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2007, Battelle Memorial Institute
' Created 12/19/2007
'
' Last modified 06/11/2009 JDS - Added logging using log4net
'*********************************************************************************************************

Imports System.IO
Imports PRISM.Files
Imports PRISM.Files.clsFileTools
Imports PRISM.Files.ZipTools
Imports ParamFileGenerator.MakeParams
Imports Protein_Exporter
Imports Protein_Exporter.ExportProteinCollectionsIFC
Imports System.Timers
Imports System.Collections.Specialized

Namespace AnalysisManagerBase

	Public MustInherit Class clsAnalysisResources
		Implements IAnalysisResources

		'*********************************************************************************************************
		'Base class for job resource class
		'*********************************************************************************************************

#Region "Constants"
		Protected Const DEFAULT_FILE_EXISTS_RETRY_HOLDOFF_SECONDS As Integer = 15
		Protected Const DEFAULT_FOLDER_EXISTS_RETRY_HOLDOFF_SECONDS As Integer = 5
        Protected Const FASTA_GEN_TIMEOUT_INTERVAL_MINUTES As Integer = 65

		Protected Const SHARPZIPLIB_HANDLES_ZIP64 As Boolean = True
		Protected Const SHARPZIPLIB_MAX_FILESIZE_MB As Integer = 1024				' Maximum file size to process using SharpZipLib; the reason we don't want to process larger files is that SharpZipLib is at least 2x slower than Pkzip

        ' Note: These constants need to be all lower    case
        Public Const RAW_DATA_TYPE_DOT_D_FOLDERS As String = "dot_d_folders"            'Agilent ion trap data
        Public Const RAW_DATA_TYPE_ZIPPED_S_FOLDERS As String = "zipped_s_folders"      'FTICR data
        Public Const RAW_DATA_TYPE_DOT_RAW_FOLDER As String = "dot_raw_folder"          'Micromass QTOF data

        Public Const RAW_DATA_TYPE_DOT_RAW_FILES As String = "dot_raw_files"            'Finnigan ion trap/LTQ-FT data
        Public Const RAW_DATA_TYPE_DOT_WIFF_FILES As String = "dot_wiff_files"          'Agilent/QSTAR TOF data
        Public Const RAW_DATA_TYPE_DOT_UIMF_FILES As String = "dot_uimf_files"          'IMS_UIMF (IMS_Agilent_TOF in DMS)
        Public Const RAW_DATA_TYPE_DOT_MZXML_FILES As String = "dot_mzxml_files"        'mzXML


        Public Const DOT_WIFF_EXTENSION As String = ".wiff"
        Public Const DOT_RAW_EXTENSION As String = ".raw"
        Public Const DOT_UIMF_EXTENSION As String = ".uimf"
        Public Const DOT_MZXML_EXTENSION As String = ".mzxml"

        Public Const DOT_MGF_EXTENSION As String = ".mgf"
        Public Const DOT_CDF_EXTENSION As String = ".cdf"

        Public Const STORAGE_PATH_INFO_FILE_SUFFIX As String = "_StoragePathInfo.txt"

#End Region

#Region "Module variables"
		Protected m_jobParams As IJobParams
        Protected m_mgrParams As IMgrParams
		Protected m_WorkingDir As String
		'		Protected m_JobNum As String
		'		Protected m_MachName As String
		Protected m_message As String
		Protected m_DebugLevel As Short
		'Protected m_DataFileList() As String
		Protected WithEvents m_FastaTools As ExportProteinCollectionsIFC.IGetFASTAFromDMS
		Protected m_GenerationStarted As Boolean = False
		Protected m_GenerationComplete As Boolean = False
		Protected m_FastaToolsCnStr As String = ""
		Protected m_FastaFileName As String = ""
		Protected WithEvents m_FastaTimer As Timer
        Protected m_FastaGenTimeOut As Boolean = False
        Protected m_FastaGenStartTime As DateTime = System.DateTime.Now
#End Region

#Region "Properties"
		' explanation of what happened to last operation this class performed
		Public Overridable ReadOnly Property Message() As String Implements IAnalysisResources.Message
			Get
				Return m_message
			End Get
		End Property

		'Public ReadOnly Property DataFileList() As String() Implements IAnalysisResources.DataFileList
		'	Get
		'		Return m_DataFileList
		'	End Get
		'End Property
#End Region

#Region "Event handlers"
        Private Sub m_FastaTools_FileGenerationStarted(ByVal taskMsg As String) Handles m_FastaTools.FileGenerationStarted

            m_GenerationStarted = True

        End Sub

		Private Sub m_FastaTools_FileGenerationCompleted(ByVal FullOutputPath As String) Handles m_FastaTools.FileGenerationCompleted

			m_FastaFileName = Path.GetFileName(FullOutputPath)		'Get the name of the fasta file that was generated
            m_GenerationComplete = True     'Set the completion flag

		End Sub

		Private Sub m_FastaTools_FileGenerationProgress(ByVal statusMsg As String, ByVal fractionDone As Double) Handles m_FastaTools.FileGenerationProgress
            Const MINIMUM_LOG_INTERVAL_SEC As Integer = 10
            Static dtLastLogTime As DateTime
            Static dblFractionDoneSaved As Double = -1

            If m_DebugLevel >= 3 Then
                ' Limit the logging to once every MINIMUM_LOG_INTERVAL_SEC seconds
                If System.DateTime.Now.Subtract(dtLastLogTime).TotalSeconds >= MINIMUM_LOG_INTERVAL_SEC OrElse _
                   fractionDone - dblFractionDoneSaved >= 0.25 Then
                    dtLastLogTime = System.DateTime.Now
                    dblFractionDoneSaved = fractionDone
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Generating Fasta file, " & (fractionDone * 100).ToString("0.0") & "% complete, " & statusMsg)
                End If
            End If

		End Sub

		Private Sub m_FastaTimer_Elapsed(ByVal sender As Object, ByVal e As System.Timers.ElapsedEventArgs) Handles m_FastaTimer.Elapsed

            If System.DateTime.Now.Subtract(m_FastaGenStartTime).TotalMinutes >= FASTA_GEN_TIMEOUT_INTERVAL_MINUTES Then
                m_FastaGenTimeOut = True      'Set the timeout flag so an error will be reported
                m_GenerationComplete = True     'Set the completion flag so the fasta generation wait loop will exit
            End If

        End Sub
#End Region

#Region "Methods"
		''' <summary>
		''' Constructor
		''' </summary>
		''' <remarks>Does nothing at present</remarks>
		Public Sub New()

		End Sub

		''' <summary>
		''' Initialize class
		''' </summary>
		''' <param name="mgrParams">Manager parameter object</param>
		''' <param name="jobParams">Job parameter object</param>
        ''' <remarks></remarks>
        Public Overridable Sub Setup(ByVal mgrParams As IMgrParams, ByVal jobParams As IJobParams) Implements IAnalysisResources.Setup

            m_mgrParams = mgrParams
            m_jobParams = jobParams
            '			m_JobNum = m_jobParams.GetParam("Job")
            '            m_MachName = m_mgrParams.GetParam("MgrName")
            m_DebugLevel = CShort(m_mgrParams.GetParam("debuglevel"))
            m_FastaToolsCnStr = m_mgrParams.GetParam("fastacnstring")

            m_WorkingDir = m_mgrParams.GetParam("workdir")
        End Sub

		Public MustOverride Function GetResources() As IJobParams.CloseOutType Implements IAnalysisResources.GetResources

        ''' <summary>
        ''' Copies specified file from storage server to local working directory
        ''' </summary>
        ''' <param name="InpFile">Name of file to copy</param>
        ''' <param name="InpFolder">Path to folder where input file is located</param>
        ''' <param name="OutDir">Destination directory for file copy</param>
        ''' <returns>TRUE for success; FALSE for failure</returns>
        ''' <remarks></remarks>
        Protected Overloads Function CopyFileToWorkDir(ByVal InpFile As String, _
                                             ByVal InpFolder As String, _
                                             ByVal OutDir As String) As Boolean
            Return CopyFileToWorkDir(InpFile, InpFolder, OutDir, clsLogTools.LogLevels.ERROR, False)
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
        Protected Overloads Function CopyFileToWorkDir(ByVal InpFile As String, _
                                             ByVal InpFolder As String, _
                                             ByVal OutDir As String, _
                                             ByVal eLogMsgTypeIfNotFound As clsLogTools.LogLevels) As Boolean
            Return CopyFileToWorkDir(InpFile, InpFolder, OutDir, eLogMsgTypeIfNotFound, False)
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
        Protected Overloads Function CopyFileToWorkDir(ByVal InpFile As String, _
                                             ByVal InpFolder As String, _
                                             ByVal OutDir As String, _
                                             ByVal eLogMsgTypeIfNotFound As clsLogTools.LogLevels, _
                                             ByVal CreateStoragePathInfoOnly As Boolean) As Boolean

            Dim SourceFile As String = String.Empty
            Dim DestFilePath As String = String.Empty

            Try
                SourceFile = System.IO.Path.Combine(InpFolder, InpFile)
                DestFilePath = Path.Combine(OutDir, InpFile)

                'Verify source file exists
                If Not FileExistsWithRetry(SourceFile, eLogMsgTypeIfNotFound) Then
                    m_message = "File not found: " & SourceFile
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, eLogMsgTypeIfNotFound, m_message)
                    Return False
                End If

                If CreateStoragePathInfoOnly Then
                    ' Create a storage path info file
                    Return CreateStoragePathInfoFile(SourceFile, DestFilePath)
                End If

                If CopyFileWithRetry(SourceFile, DestFilePath, True) Then
                    If m_DebugLevel > 3 Then
                        Dim Msg As String = "clsAnalysisResources.CopyFileToWorkDir, File copied: " & SourceFile
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, Msg)
                    End If
                    Return True
                Else
                    m_message = "Error copying file " & SourceFile
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, m_message)
                    Return False
                End If

            Catch ex As Exception
                If SourceFile Is Nothing Then SourceFile = InpFile
                If SourceFile Is Nothing Then SourceFile = "??"

                m_message = "Exception in CopyFileToWorkDir for " & SourceFile & ": " & ex.Message
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
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
        Protected Overloads Function CopyFileToWorkDirWithRename(ByVal InpFile As String, _
                                                       ByVal InpFolder As String, _
                                                       ByVal OutDir As String) As Boolean
            Return CopyFileToWorkDirWithRename(InpFile, InpFolder, OutDir, clsLogTools.LogLevels.ERROR, False)
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
        Protected Overloads Function CopyFileToWorkDirWithRename(ByVal InpFile As String, _
                                                       ByVal InpFolder As String, _
                                                       ByVal OutDir As String, _
                                                       ByVal eLogMsgTypeIfNotFound As clsLogTools.LogLevels) As Boolean
            Return CopyFileToWorkDirWithRename(InpFile, InpFolder, OutDir, eLogMsgTypeIfNotFound, False)
        End Function

        ''' <summary>
        ''' Copies specified file from storage server to local working directory, renames destination with dataset name
        ''' </summary>
        ''' <param name="InpFile">Name of file to copy</param>
        ''' <param name="InpFolder">Path to folder where input file is located</param>
        ''' <param name="OutDir">Destination directory for file copy</param>
        ''' <param name="eLogMsgTypeIfNotFound">Type of message to log if the file is not found</param>
        ''' <param name="CreateStoragePathInfoOnly">When true, then does not actually copy the specified file, and instead creates a file named FileName_StoragePathInfo.txt, and this file's first line will be the full path to the source file</param>
        ''' <returns>TRUE for success; FALSE for failure</returns>
        ''' <remarks></remarks>
        Protected Overloads Function CopyFileToWorkDirWithRename(ByVal InpFile As String, _
                                                       ByVal InpFolder As String, _
                                                       ByVal OutDir As String, _
                                                       ByVal eLogMsgTypeIfNotFound As clsLogTools.LogLevels, _
                                                       ByVal CreateStoragePathInfoOnly As Boolean) As Boolean


            Dim SourceFile As String = String.Empty
            Dim DestFilePath As String = String.Empty

            Try
                SourceFile = Path.Combine(InpFolder, InpFile)

                'Verify source file exists
                If Not FileExistsWithRetry(SourceFile, eLogMsgTypeIfNotFound) Then
                    m_message = "File not found: " & SourceFile
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, eLogMsgTypeIfNotFound, m_message)
                    Return False
                End If

                Dim Fi As New FileInfo(SourceFile)
                Dim TargetName As String = m_jobParams.GetParam("datasetNum") & Fi.Extension
                DestFilePath = Path.Combine(OutDir, TargetName)

                If CreateStoragePathInfoOnly Then
                    ' Create a storage path info file
                    Return CreateStoragePathInfoFile(SourceFile, DestFilePath)
                End If

                If CopyFileWithRetry(SourceFile, DestFilePath, True) Then
                    If m_DebugLevel > 3 Then
                        Dim Msg As String = "clsAnalysisResources.CopyFileToWorkDirWithRename, File copied: " & SourceFile
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, Msg)
                    End If
                    Return True
                Else
                    m_message = "Error copying file " & SourceFile
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, m_message)
                    Return False
                End If

            Catch ex As Exception
                If SourceFile Is Nothing Then SourceFile = InpFile
                If SourceFile Is Nothing Then SourceFile = "??"

                m_message = "Exception in CopyFileToWorkDirWithRename for " & SourceFile & ": " & ex.Message
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
            End Try

        End Function

        ''' <summary>
        ''' Creates a file named DestFilePath but with "_StoragePathInfo.txt" appended to the name
        ''' The file's contents is the path given by SourceFilePath
        ''' </summary>
        ''' <param name="SourceFilePath">The path to write to the StoragePathInfo file</param>
        ''' <param name="DestFilePath">The path where the file would have been copied to</param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Protected Function CreateStoragePathInfoFile(ByVal SourceFilePath As String, ByVal DestFilePath As String) As Boolean

            Dim swOutFile As System.IO.StreamWriter
            Dim strInfoFilePath As String = String.Empty

            Try
                If SourceFilePath Is Nothing Or DestFilePath Is Nothing Then
                    Return False
                End If

                strInfoFilePath = DestFilePath & STORAGE_PATH_INFO_FILE_SUFFIX

                swOutFile = New System.IO.StreamWriter(New System.IO.FileStream(strInfoFilePath, FileMode.Create, FileAccess.Write, FileShare.Read))

                swOutFile.WriteLine(SourceFilePath)
                swOutFile.Close()

            Catch ex As Exception
                m_message = "Exception in CreateStoragePathInfoFile for " & strInfoFilePath & ": " & ex.Message
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)

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
        Public Shared Function ResolveStoragePath(ByVal FolderPath As String, ByVal FileName As String) As String

            Dim srInFile As System.IO.StreamReader
            Dim strPhysicalFilePath As String = String.Empty
            Dim strFilePath As String

            Dim strLineIn As String

            strFilePath = System.IO.Path.Combine(FolderPath, FileName)

            If System.IO.File.Exists(strFilePath) Then
                ' The desired file is located in folder FolderPath
                strPhysicalFilePath = strFilePath
            Else
                ' The desired file was not found
                strFilePath &= STORAGE_PATH_INFO_FILE_SUFFIX

                If System.IO.File.Exists(strFilePath) Then
                    ' The _StoragePathInfo.txt file is present
                    ' Open that file to read the file path on the first line of the file

                    srInFile = New System.IO.StreamReader(New System.IO.FileStream(strFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))

                    strLineIn = srInFile.ReadLine
                    strPhysicalFilePath = strLineIn

                    srInFile.Close()
                End If
            End If

            Return strPhysicalFilePath

        End Function

        ''' <summary>
        ''' Looks for the STORAGE_PATH_INFO_FILE_SUFFIX file in the working folder
        ''' If present, looks for a file named _StoragePathInfo.txt; if that file is found, opens the file and reads the path
        ''' If the file named _StoragePathInfo.txt isn't found, then returns an empty string
        ''' </summary>
        ''' <param name="FolderPath">The folder to look in</param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Public Shared Function ResolveSerStoragePath(ByVal FolderPath As String) As String

            Dim srInFile As System.IO.StreamReader
            Dim strPhysicalFilePath As String = String.Empty
            Dim strFilePath As String

            Dim strLineIn As String

            strFilePath = System.IO.Path.Combine(FolderPath, STORAGE_PATH_INFO_FILE_SUFFIX)

            If System.IO.File.Exists(strFilePath) Then
                ' The desired file is located in folder FolderPath
                ' The _StoragePathInfo.txt file is present
                ' Open that file to read the file path on the first line of the file

                srInFile = New System.IO.StreamReader(New System.IO.FileStream(strFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))

                strLineIn = srInFile.ReadLine
                strPhysicalFilePath = strLineIn

                srInFile.Close()
            Else
                ' The desired file was not found
                strPhysicalFilePath = ""
            End If

            Return strPhysicalFilePath

        End Function

        ''' <summary>
        ''' Retrieves the spectra file(s) based on raw data type and puts them in the working directory
        ''' </summary>
        ''' <param name="RawDataType">Type of data to copy</param>
        ''' <param name="WorkDir">Destination directory for copy</param>
        ''' <returns>TRUE for success; FALSE for failure</returns>
        ''' <remarks></remarks>
        Protected Overridable Function RetrieveSpectra(ByVal RawDataType As String, ByVal WorkDir As String) As Boolean
            Return RetrieveSpectra(RawDataType, WorkDir, False)
        End Function

        ''' <summary>
        ''' Retrieves the spectra file(s) based on raw data type and puts them in the working directory
        ''' </summary>
        ''' <param name="RawDataType">Type of data to copy</param>
        ''' <param name="WorkDir">Destination directory for copy</param>
        ''' <param name="CreateStoragePathInfoOnly">When true, then does not actually copy the dataset file (or folder), and instead creates a file named Dataset.raw_StoragePathInfo.txt, and this file's first line will be the full path to the spectrum file (or spectrum folder)</param>
        ''' <returns>TRUE for success; FALSE for failure</returns>
        ''' <remarks></remarks>
        Protected Overridable Function RetrieveSpectra(ByVal RawDataType As String, _
                                                       ByVal WorkDir As String, _
                                                       ByVal CreateStoragePathInfoOnly As Boolean) As Boolean

            Dim OpResult As Boolean = False

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Retrieving spectra file(s)")

            Select Case RawDataType.ToLower
                Case RAW_DATA_TYPE_DOT_D_FOLDERS            'Agilent ion trap data
                    If RetrieveMgfFile(WorkDir, True, CreateStoragePathInfoOnly) Then OpResult = True

                Case RAW_DATA_TYPE_DOT_WIFF_FILES           'Agilent/QSTAR TOF data
                    If RetrieveDatasetFile(WorkDir, DOT_WIFF_EXTENSION, CreateStoragePathInfoOnly) Then OpResult = True

                Case RAW_DATA_TYPE_ZIPPED_S_FOLDERS         'FTICR data
                    If RetrieveSFolders(WorkDir, CreateStoragePathInfoOnly) Then OpResult = True

                Case RAW_DATA_TYPE_DOT_RAW_FILES            'Finnigan ion trap/LTQ-FT data
                    If RetrieveDatasetFile(WorkDir, DOT_RAW_EXTENSION, CreateStoragePathInfoOnly) Then OpResult = True

                Case RAW_DATA_TYPE_DOT_RAW_FOLDER           'Micromass QTOF data
                    If RetrieveDotRawFolder(WorkDir, CreateStoragePathInfoOnly) Then OpResult = True

                Case RAW_DATA_TYPE_DOT_UIMF_FILES           'IMS UIMF data
                    If RetrieveDatasetFile(WorkDir, DOT_UIMF_EXTENSION, CreateStoragePathInfoOnly) Then OpResult = True

                Case RAW_DATA_TYPE_DOT_MZXML_FILES
                    If RetrieveDatasetFile(WorkDir, DOT_MZXML_EXTENSION, CreateStoragePathInfoOnly) Then OpResult = True

                Case Else           'Something bad has happened if we ever get to here
                    m_message = "Invalid data type specified: " & RawDataType
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
            End Select

            'Return the result of the spectra retrieval
            Return OpResult

        End Function

        ''' <summary>
        ''' Retrieves a dataset file for the analysis job in progress; uses the user-supplied extension to match the file
        ''' </summary>
        ''' <param name="WorkDir">Destination directory for copy</param>
        ''' <param name="FileExtension">File extension to match; must contain a period, for example ".raw"</param>
        ''' <returns>TRUE for success; FALSE for failure</returns>
        ''' <remarks></remarks>
        Protected Overridable Function RetrieveDatasetFile(ByVal WorkDir As String, _
                                                           ByVal FileExtension As String, _
                                                           ByVal CreateStoragePathInfoOnly As Boolean) As Boolean

            Dim DSName As String = m_jobParams.GetParam("datasetNum")
            Dim DataFileName As String = DSName & FileExtension
            Dim DSFolderPath As String = FindValidFolder(DSName, DataFileName)

            If CopyFileToWorkDir(DataFileName, DSFolderPath, WorkDir, clsLogTools.LogLevels.ERROR, CreateStoragePathInfoOnly) Then
                Return True
            Else
                Return False
            End If

        End Function

        ''' <summary>
        ''' Retrieves an Agilent ion trap .mgf file or .cdf/,mgf pair for analysis job in progress
        ''' </summary>
        ''' <param name="WorkDir">Destination directory for copy</param>
        ''' <param name="GetCdfAlso">TRUE if .cdf file is needed along with .mgf file; FALSE otherwise</param>
        ''' <returns>TRUE for success; FALSE for failure</returns>
        ''' <remarks></remarks>
        Protected Overridable Function RetrieveMgfFile(ByVal WorkDir As String, _
                                                       ByVal GetCdfAlso As Boolean, _
                                                       ByVal CreateStoragePathInfoOnly As Boolean) As Boolean

            'Data files are in a subfolder off of the main dataset folder
            'Files are renamed with dataset name because MASIC requires this. Other analysis types don't care

            Dim DSName As String = m_jobParams.GetParam("datasetNum")
            Dim ServerPath As String = FindValidFolder(DSName, "", "*.D")

            Dim DSFolders() As String
            Dim DSFiles() As String = Nothing
            Dim DumFolder As String
            Dim FileFound As Boolean = False
            Dim DataFolderPath As String = ""

            'Get a list of the subfolders in the dataset folder
            DSFolders = Directory.GetDirectories(ServerPath)
            'Go through the folders looking for a file with a ".mgf" extension
            For Each DumFolder In DSFolders
                If FileFound Then Exit For
                DSFiles = Directory.GetFiles(DumFolder, "*" & DOT_MGF_EXTENSION)
                If DSFiles.GetLength(0) = 1 Then
                    'Correct folder has been found
                    DataFolderPath = DumFolder
                    FileFound = True
                    Exit For
                End If
            Next DumFolder

            'Exit if no data file was found
            If Not FileFound Then Return False

            'Do the copy
            If Not CopyFileToWorkDirWithRename(DSFiles(0), DataFolderPath, WorkDir, clsLogTools.LogLevels.ERROR, CreateStoragePathInfoOnly) Then Return False

            'If we don't need to copy the .cdf file, we're done; othewise, find the .cdf file and copy it
            If Not GetCdfAlso Then Return True

            DSFiles = Directory.GetFiles(DataFolderPath, "*" & DOT_CDF_EXTENSION)
            If DSFiles.GetLength(0) <> 1 Then
                'Incorrect number of .cdf files found
                Return False
            End If

            'Copy the .cdf file that was found
            If CopyFileToWorkDirWithRename(DSFiles(0), DataFolderPath, WorkDir, clsLogTools.LogLevels.ERROR, CreateStoragePathInfoOnly) Then
                Return True
            Else
                Return False
            End If

        End Function

        ''' <summary>
        ''' Retrieves a .raw folder from Micromass TOF for the analysis job in progress
        ''' </summary>
        ''' <param name="WorkDir">Destination directory for copy</param>
        ''' <returns>TRUE for success; FALSE for failure</returns>
        ''' <remarks></remarks>
        Protected Overridable Function RetrieveDotRawFolder(ByVal WorkDir As String, ByVal CreateStoragePathInfoOnly As Boolean) As Boolean

            'Copies a .raw data folder from the Micromass TOF datafile to the working directory
            Dim DSName As String = m_jobParams.GetParam("datasetNum")
            Dim ServerPath As String = FindValidFolder(DSName, "", "*.raw")
            Dim DestFolderPath As String

            'Find the .raw folder in the dataset folder
            Dim RemFolders() As String = Directory.GetDirectories(ServerPath, "*.raw")
            If RemFolders.GetLength(0) <> 1 Then Return False

            'Set up the file paths
            Dim DSFolderPath As String = Path.Combine(ServerPath, RemFolders(0))

            'Do the copy
            Try
                DestFolderPath = System.IO.Path.Combine(WorkDir, DSName & ".raw")

                If CreateStoragePathInfoOnly Then
                    If Not System.IO.Directory.Exists(DSFolderPath) Then
                        m_message = "Source folder not found: " & DSFolderPath
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, m_message)
                        Return False
                    Else
                        CreateStoragePathInfoFile(DSFolderPath, DestFolderPath)
                    End If
                Else
                    CopyDirectory(DSFolderPath, DestFolderPath)
                End If

            Catch ex As Exception
                m_message = "Error copying folder " & DSFolderPath
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, m_message & " to working directory: " & ex.Message & "; " & clsGlobal.GetExceptionStackTrace(ex))
                Return False
            End Try

            ' If we get here, all is fine
            Return True

        End Function

        ''' <summary>
        ''' Unzips dataset folders to working directory
        ''' </summary>
        ''' <param name="WorkDir">Destination directory for copy</param>
        ''' <returns>TRUE for success; FALSE for failure</returns>
        ''' <remarks></remarks>
        Private Function RetrieveSFolders(ByVal WorkDir As String, ByVal CreateStoragePathInfoOnly As Boolean) As Boolean

            Dim DSName As String = m_jobParams.GetParam("datasetNum")
            Dim ZipFiles() As String
            Dim DSWorkFolder As String
            Dim UnZipper As ZipTools
            Dim TargetFolder As String
            Dim ZipFile As String
            Dim strZipProgramPath As String

            'First Check for the existence of a 0.ser Folder
            'If 0.ser folder exists, then store the 0.ser folder in a file locally
            Dim DSFolderPath As String = FindValidFolder(DSName, "", "0.ser")

            If Not String.IsNullOrEmpty(DSFolderPath) AndAlso Directory.Exists(Path.Combine(DSFolderPath, "0.ser")) Then
                DSFolderPath = Path.Combine(DSFolderPath, "0.ser")
                If CreateStoragePathInfoFile(DSFolderPath, WorkDir & "\") Then
                    Return True
                Else
                    Return False
                End If
            End If

            'If the 0.ser folder does not exist, unzip the zipped s-folders
            'Copy the zipped s-folders from archive to work directory
            If Not CopySFoldersToWorkDir(WorkDir, CreateStoragePathInfoOnly) Then
                'Error messages have already been logged, so just exit
                Return False
            End If

            If CreateStoragePathInfoOnly Then
                ' Nothing was copied locally, so nothing to unzip
                Return True
            End If


            'Get a listing of the zip files to process
            ZipFiles = Directory.GetFiles(WorkDir, "s*.zip")
            If ZipFiles.GetLength(0) < 1 Then
                m_message = "No zipped s-folders found in working directory"
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
                Return False            'No zipped data files found
            End If

            'Create a dataset subdirectory under the working directory
            DSWorkFolder = Path.Combine(WorkDir, DSName)
            Directory.CreateDirectory(DSWorkFolder)

            'Set up the unzipper
            strZipProgramPath = m_mgrParams.GetParam("zipprogram")

            If Not System.IO.File.Exists(strZipProgramPath) Then
                m_message = "Unzip program not found (" & strZipProgramPath & "); unable to continue"
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
                Return False
            End If

            UnZipper = New ZipTools(DSWorkFolder, strZipProgramPath)

            'Unzip each of the zip files to the working directory
            For Each ZipFile In ZipFiles
                If m_DebugLevel > 3 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Unzipping file " & ZipFile)
                End If
                Try
                    TargetFolder = Path.Combine(DSWorkFolder, Path.GetFileNameWithoutExtension(ZipFile))
                    Directory.CreateDirectory(TargetFolder)
                    If Not UnZipper.UnzipFile("", ZipFile, TargetFolder) Then
                        m_message = "Error unzipping file " & ZipFile
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
                        Return False
                    End If
                Catch ex As Exception
                    m_message = "Exception while unzipping s-folders"
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message & "; " & clsGlobal.GetExceptionStackTrace(ex))
                    Return False
                End Try
            Next

            'Delete all s*.zip files in working directory
            For Each ZipFile In ZipFiles
                Try
                    File.Delete(ZipFile)
                Catch ex As Exception
                    m_message = "Exception deleting file " & ZipFile
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & " : " & ex.Message & "; " & clsGlobal.GetExceptionStackTrace(ex))
                    Return False
                End Try
            Next

            'Got to here, so everything must have worked
            Return True

        End Function

        ''' <summary>
        ''' Copies the zipped s-folders to the working directory
        ''' </summary>
        ''' <param name="WorkDir">Destination directory for copy</param>
        ''' <param name="CreateStoragePathInfoOnly">When true, then does not actually copy the specified files, and instead creates a series of files named s*.zip_StoragePathInfo.txt, and each file's first line will be the full path to the source file</param>
        ''' <returns>TRUE for success; FALSE for failure</returns>
        ''' <remarks></remarks>
        Private Function CopySFoldersToWorkDir(ByVal WorkDir As String, ByVal CreateStoragePathInfoOnly As Boolean) As Boolean

            '
            Dim DSName As String = m_jobParams.GetParam("datasetNum")
            Dim DSFolderPath As String = FindValidFolder(DSName, "s*.zip")

            Dim ZipFiles() As String
            Dim DestFilePath As String

            'Verify dataset folder exists
            If Not Directory.Exists(DSFolderPath) Then Return False

            'Get a listing of the zip files to process
            ZipFiles = Directory.GetFiles(DSFolderPath, "s*.zip")
            If ZipFiles.GetLength(0) < 1 Then Return False 'No zipped data files found

            'copy each of the s*.zip files to the working directory
            For Each ZipFilePath As String In ZipFiles

                If m_DebugLevel > 3 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Copying file " & ZipFilePath & " to work directory")
                End If

                DestFilePath = Path.Combine(WorkDir, System.IO.Path.GetFileName(ZipFilePath))

                If CreateStoragePathInfoOnly Then
                    If Not CreateStoragePathInfoFile(ZipFilePath, DestFilePath) Then
                        m_message = "Error creating storage path info file for " & ZipFilePath
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
                        Return False
                    End If
                Else
                    If Not CopyFileWithRetry(ZipFilePath, DestFilePath, False) Then
                        m_message = "Error copying file " & ZipFilePath
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
                        Return False
                    End If
                End If
            Next

            'If we got to here, everything worked
            Return True

        End Function

        ''' <summary>
        ''' Copies a file with retries in case of failure
        ''' </summary>
        ''' <param name="SrcFileName">Full path to source file</param>
        ''' <param name="DestFileName">Full path to destination file</param>
        ''' <param name="Overwrite">TRUE to overwrite existing destination file; FALSE otherwise</param>
        ''' <returns>TRUE for success; FALSE for error</returns>
        ''' <remarks>Logs copy errors</remarks>
        Private Function CopyFileWithRetry(ByVal SrcFileName As String, ByVal DestFileName As String, ByVal Overwrite As Boolean) As Boolean
            Const RETRY_HOLDOFF_SECONDS As Integer = 15

            Dim RetryCount As Integer = 3

            While RetryCount > 0
                Try
                    File.Copy(SrcFileName, DestFileName, Overwrite)
                    'Copy must have worked, so return TRUE
                    Return True
                Catch ex As Exception
                    Dim ErrMsg As String = "Exception copying file " & SrcFileName & " to " & DestFileName & ": " & _
                      ex.Message & "; " & clsGlobal.GetExceptionStackTrace(ex)

                    ErrMsg &= " Retry Count = " & RetryCount.ToString
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, ErrMsg)
                    RetryCount -= 1

                    If Not Overwrite AndAlso System.IO.File.Exists(DestFileName) Then
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Tried to overwrite an existing file when Overwrite = False: " & DestFileName)
                        Return False
                    End If

                    System.Threading.Thread.Sleep(RETRY_HOLDOFF_SECONDS * 1000)    'Wait several seconds before retrying
                End Try
            End While

            'If we got to here, there were too many failures
            If RetryCount < 1 Then
                m_message = "Excessive failures during file copy"
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
                Return False
            End If

        End Function

        ''' <summary>
        ''' Test for file existence with a retry loop in case of temporary glitch
        ''' </summary>
        ''' <param name="FileName"></param>
        ''' <param name="eLogMsgTypeIfNotFound">Type of message to log if the file is not found</param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Private Overloads Function FileExistsWithRetry(ByVal FileName As String, ByVal eLogMsgTypeIfNotFound As clsLogTools.LogLevels) As Boolean

            Return FileExistsWithRetry(FileName, DEFAULT_FILE_EXISTS_RETRY_HOLDOFF_SECONDS, eLogMsgTypeIfNotFound)

        End Function

        ''' <summary>
        ''' Test for file existence with a retry loop in case of temporary glitch
        ''' </summary>
        ''' <param name="FileName"></param>
        ''' <param name="RetryHoldoffSeconds">Number of seconds to wait between subsequent attempts to check for the file</param>
        ''' <param name="eLogMsgTypeIfNotFound">Type of message to log if the file is not found</param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Private Overloads Function FileExistsWithRetry(ByVal FileName As String, ByVal RetryHoldoffSeconds As Integer, ByVal eLogMsgTypeIfNotFound As clsLogTools.LogLevels) As Boolean

            Dim RetryCount As Integer = 3

            If RetryHoldoffSeconds <= 0 Then RetryHoldoffSeconds = DEFAULT_FILE_EXISTS_RETRY_HOLDOFF_SECONDS
            If RetryHoldoffSeconds > 600 Then RetryHoldoffSeconds = 600

            While RetryCount > 0
                If File.Exists(FileName) Then
                    Return True
                Else
                    If eLogMsgTypeIfNotFound = clsLogTools.LogLevels.ERROR Then
                        ' Only log each failed attempt to find the file if eLogMsgTypeIfNotFound = ILogger.logMsgType.logError
                        ' Otherwise, we won't log each failed attempt
                        Dim ErrMsg As String = "File " & FileName & " not found. Retry count = " & RetryCount.ToString
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, eLogMsgTypeIfNotFound, ErrMsg)
                    End If
                    RetryCount -= 1
                    System.Threading.Thread.Sleep(New System.TimeSpan(0, 0, RetryHoldoffSeconds))       'Wait RetryHoldoffSeconds seconds before retrying
                End If
            End While

            'If we got to here, there were too many failures
            If RetryCount < 1 Then
                m_message = "File " & FileName & " could not be found after multiple retries"
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, eLogMsgTypeIfNotFound, m_message)
                Return False
            End If

        End Function

        ''' <summary>
        ''' Test for folder existence with a retry loop in case of temporary glitch
        ''' </summary>
        ''' <param name="FolderName">Folder name to look for</param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Private Overloads Function FolderExistsWithRetry(ByVal FolderName As String) As Boolean

            Return FolderExistsWithRetry(FolderName, DEFAULT_FOLDER_EXISTS_RETRY_HOLDOFF_SECONDS)

        End Function


        ''' <summary>
        ''' Test for folder existence with a retry loop in case of temporary glitch
        ''' </summary>
        ''' <param name="FolderName">Folder name to look for</param>
        ''' <param name="RetryHoldoffSeconds">Time, in seconds, to wait between retrying; if 0, then will default to 5 seconds; maximum value is 600 seconds</param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Private Overloads Function FolderExistsWithRetry(ByVal FolderName As String, ByVal RetryHoldoffSeconds As Integer) As Boolean

            Dim RetryCount As Integer = 3

            If RetryHoldoffSeconds <= 0 Then RetryHoldoffSeconds = DEFAULT_FOLDER_EXISTS_RETRY_HOLDOFF_SECONDS
            If RetryHoldoffSeconds > 600 Then RetryHoldoffSeconds = 600

            While RetryCount > 0
                If System.IO.Directory.Exists(FolderName) Then
                    Return True
                Else
                    Dim ErrMsg As String = "Folder " & FolderName & " not found. Retry count = " & RetryCount.ToString
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, ErrMsg)
                    RetryCount -= 1
                    System.Threading.Thread.Sleep(New System.TimeSpan(0, 0, RetryHoldoffSeconds))       'Wait RetryHoldoffSeconds seconds before retrying
                End If
            End While

            'If we got to here, there were too many failures
            If RetryCount < 1 Then
                Return False
            End If

        End Function

        ''' <summary>
        ''' Determines the most appropriate folder to use to obtain dataset files from
        ''' Optionally, can require that a certain file also be present in the folder for it to be deemed valid
        ''' If no folder is deemed valid, then returns the path defined by "DatasetStoragePath"
        ''' </summary>
        ''' <param name="DSName">Name of the dataset</param>
        ''' <param name="FileNameToFind">Optional: Name of a file that must exist in the folder</param>
        ''' <returns>Path to the most appropriate dataset folder</returns>
        ''' <remarks></remarks>
        Private Function FindValidFolder(ByVal DSName As String, ByVal FileNameToFind As String) As String

            Return FindValidFolder(DSName, FileNameToFind, "")

        End Function

        ''' <summary>
        ''' Determines the most appropriate folder to use to obtain dataset files from
        ''' Optionally, can require that a certain file also be present in the folder for it to be deemed valid
        ''' If no folder is deemed valid, then returns the path defined by "DatasetStoragePath"
        ''' </summary>
        ''' <param name="DSName">Name of the dataset</param>
        ''' <param name="FileNameToFind">Optional: Name of a file that must exist in the folder</param>
        ''' <param name="FolderNameToFind">Optional: Name of a folder that must exist in the folder</param>
        ''' <returns>Path to the most appropriate dataset folder</returns>
        ''' <remarks></remarks>
        Private Function FindValidFolder(ByVal DSName As String, ByVal FileNameToFind As String, ByVal FolderNameToFind As String) As String

            Dim strBestPath As String = String.Empty
            Dim PathsToCheck() As String

            Dim intIndex As Integer
            Dim blnValidFolder As Boolean
            Dim blnFileNotFoundEncountered As Boolean

            Dim objFolderInfo As System.IO.DirectoryInfo

            ReDim PathsToCheck(2)

            Try
                If FileNameToFind Is Nothing Then FileNameToFind = String.Empty

                PathsToCheck(0) = Path.Combine(m_jobParams.GetParam("DatasetStoragePath"), DSName)
                PathsToCheck(1) = Path.Combine(m_jobParams.GetParam("DatasetArchivePath"), DSName)
                PathsToCheck(2) = Path.Combine(Path.Combine(m_jobParams.GetParam("transferFolderPath"), DSName), "InputFolderName")
                blnFileNotFoundEncountered = False

                strBestPath = PathsToCheck(0)
                For intIndex = 0 To PathsToCheck.Length - 1
                    Try
                        If m_DebugLevel > 3 Then
                            Dim Msg As String = "clsAnalysisResources.FindValidDatasetFolder, Looking for folder " & PathsToCheck(intIndex)
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, Msg)
                        End If

                        ' First check whether this folder exists
                        ' Using a 3 second holdoff between retries
                        If FolderExistsWithRetry(PathsToCheck(intIndex), 3) Then
                            If m_DebugLevel > 3 Then
                                Dim Msg As String = "clsAnalysisResources.FindValidDatasetFolder, Folder found " & PathsToCheck(intIndex)
                                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, Msg)
                            End If

                            ' Folder was found
                            blnValidFolder = True

                            ' Optionally look for FileNameToFind
                            If FileNameToFind.Length > 0 Then

                                If FileNameToFind.Contains("*") Then
                                    If m_DebugLevel > 3 Then
                                        Dim Msg As String = "clsAnalysisResources.FindValidDatasetFolder, Looking for files matching " & FileNameToFind
                                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, Msg)
                                    End If

                                    ' Wildcard in the name
                                    ' Look for any files matching FileNameToFind
                                    objFolderInfo = New System.IO.DirectoryInfo(PathsToCheck(intIndex))

                                    If objFolderInfo.GetFiles(FileNameToFind).Length = 0 Then
                                        blnValidFolder = False
                                    End If
                                Else
                                    If m_DebugLevel > 3 Then
                                        Dim Msg As String = "clsAnalysisResources.FindValidDatasetFolder, Looking for file named " & FileNameToFind
                                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, Msg)
                                    End If

                                    ' Look for file FileNameToFind in this folder
                                    ' Note: Using a 1 second holdoff between retries
                                    If Not FileExistsWithRetry(System.IO.Path.Combine(PathsToCheck(intIndex), FileNameToFind), 1, clsLogTools.LogLevels.WARN) Then
                                        blnValidFolder = False
                                    End If
                                End If
                            End If

                            ' Optionally look for FolderNameToFind
                            If blnValidFolder AndAlso FolderNameToFind.Length > 0 Then
                                If FolderNameToFind.Contains("*") Then
                                    If m_DebugLevel > 3 Then
                                        Dim Msg As String = "clsAnalysisResources.FindValidDatasetFolder, Looking for folders matching " & FolderNameToFind
                                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, Msg)
                                    End If

                                    ' Wildcard in the name
                                    ' Look for any folders matching FolderNameToFind
                                    objFolderInfo = New System.IO.DirectoryInfo(PathsToCheck(intIndex))

                                    If objFolderInfo.GetDirectories(FolderNameToFind).Length = 0 Then
                                        blnValidFolder = False
                                    End If
                                Else
                                    If m_DebugLevel > 3 Then
                                        Dim Msg As String = "clsAnalysisResources.FindValidDatasetFolder, Looking for folder named " & FolderNameToFind
                                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, Msg)
                                    End If

                                    ' Look for folder FolderNameToFind in this folder
                                    ' Note: Using a 1 second holdoff between retries
                                    If Not FolderExistsWithRetry(System.IO.Path.Combine(PathsToCheck(intIndex), FolderNameToFind), 1) Then
                                        blnValidFolder = False
                                    End If
                                End If
                            End If

                            If Not blnValidFolder Then
                                blnFileNotFoundEncountered = True
                            Else
                                strBestPath = PathsToCheck(intIndex)

                                If m_DebugLevel >= 4 OrElse m_DebugLevel >= 1 AndAlso blnFileNotFoundEncountered Then
                                    Dim Msg As String = "clsAnalysisResources.FindValidFolder, Valid dataset folder has been found:  " & strBestPath
                                    If FileNameToFind.Length > 0 Then
                                        Msg &= " (matched file " & FileNameToFind & ")"
                                    End If
                                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, Msg)
                                End If

                                Exit For
                            End If
                        Else
                            blnFileNotFoundEncountered = True
                        End If

                    Catch ex As Exception
                        m_message = "Exception looking for folder: " & PathsToCheck(intIndex) & "; " & clsGlobal.GetExceptionStackTrace(ex)
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
                    End Try
                Next intIndex

                If Not blnValidFolder Then
                    m_message = "Could not find a valid dataset folder"
                    If FileNameToFind.Length > 0 Then
                        m_message &= " containing file " & FileNameToFind
                    End If
                    Dim Msg As String = m_message & ", Job " & m_jobParams.GetParam("Job") & ", Dataset " & DSName
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, Msg)
                End If

            Catch ex As Exception
                m_message = "Exception looking for a valid dataset folder"
                Dim ErrMsg As String = m_message & " for dataset " & DSName & "; " & clsGlobal.GetExceptionStackTrace(ex)
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, ErrMsg)
            End Try

            Return strBestPath

        End Function

        ''' <summary>
        ''' Uses Ken's dll to create a fasta file for Sequest or XTandem analysis
        ''' </summary>
        ''' <param name="LocalOrgDBFolder">Folder on analysis machine where fasta files are stored</param>
        ''' <returns>TRUE for success; FALSE for failure</returns>
        ''' <remarks></remarks>
        Protected Overridable Function RetrieveOrgDB(ByVal LocalOrgDBFolder As String) As Boolean

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Obtaining org db file")

            'Make a new fasta file from scratch
            If Not CreateFastaFile(LocalOrgDBFolder) Then
                'There was a problem. Log entries in lower-level routines provide documentation
                Return False
            End If

            'Fasta file was successfully generated. Put the private name of the generated fastafile in the
            '	job data class for other methods to use
            If Not m_jobParams.AddAdditionalParameter("generatedFastaName", m_FastaFileName) Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error adding parameter 'generatedFastaName' to m_jobParams")
                Return False
            End If


            'We got to here OK, so return
            Return True

        End Function

        ''' <summary>
        ''' Creates a Fasta file based on Ken's DLL
        ''' </summary>
        ''' <param name="DestFolder">Folder where file will be created</param>
        ''' <returns>TRUE for success; FALSE for failure</returns>
        ''' <remarks></remarks>
        Public Function CreateFastaFile(ByVal DestFolder As String) As Boolean

            Dim HashString As String
            Dim OrgDBDescription As String = String.Empty

            If m_DebugLevel > 0 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisResources.CreateFastaFile(), Creating fasta file")
            End If

            'Instantiate fasta tool if not already done
            If m_FastaTools Is Nothing Then
                If m_FastaToolsCnStr = "" Then
                    m_message = "Protein database connection string not specified"
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisResources.CreateFastaFile(), " & m_message)
                    Return False
                End If
                m_FastaTools = New clsGetFASTAFromDMS(m_FastaToolsCnStr)
            End If

            'Initialize fasta generation state variables
            m_GenerationStarted = False
            m_GenerationComplete = False

            'Set up variables for fasta creation call
            Dim LegacyFasta As String = m_jobParams.GetParam("LegacyFastaFileName")
            Dim CreationOpts As String = m_jobParams.GetParam("ProteinOptions")
            Dim CollectionList As String = m_jobParams.GetParam("ProteinCollectionList")

            If CollectionList.Length > 0 AndAlso Not CollectionList.ToLower = "na" Then
                OrgDBDescription = "Protein collection: " & CollectionList & " with options " & CreationOpts
            Else
                OrgDBDescription = "Legacy DB: " & LegacyFasta
            End If

            If m_DebugLevel >= 2 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "ProteinCollectionList=" & CollectionList & "; CreationOpts=" & CreationOpts & "; LegacyFasta=" & LegacyFasta)
            End If

            m_FastaTimer = New Timer
            m_FastaTimer.Interval = 5000
            m_FastaTimer.AutoReset = True

            ' Note that m_FastaTools does not spawn a new thread
            '   Since it does not spawn a new thread, the while loop after this Try block won't actually get reached while m_FastaTools.ExportFASTAFile is running
            '   Furthermore, even if m_FastaTimer_Elapsed sets m_FastaGenTimeOut to True, this won't do any good since m_FastaTools.ExportFASTAFile will still be running
            m_FastaGenTimeOut = False
            m_FastaGenStartTime = System.DateTime.Now
            Try
                m_FastaTimer.Start()
                HashString = m_FastaTools.ExportFASTAFile(CollectionList, CreationOpts, LegacyFasta, DestFolder)
            Catch Ex As Exception
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisResources.CreateFastaFile(), Exception generating OrgDb file; " & OrgDBDescription & "; " & _
                 "; " & clsGlobal.GetExceptionStackTrace(Ex))
                Return False
            End Try

            'Wait for fasta creation to finish
            While Not (m_GenerationComplete Or m_FastaGenTimeOut)
                System.Threading.Thread.Sleep(2000)
            End While

            m_FastaTimer.Stop()
            If m_FastaGenTimeOut Then
                'Fasta generator hung - report error and exit
                m_message = "Timeout error while generating OrdDb file (" & FASTA_GEN_TIMEOUT_INTERVAL_MINUTES.ToString & " minutes have elapsed); " & OrgDBDescription
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisResources.CreateFastaFile(), " & m_message)
                Return False
            End If

            If HashString Is Nothing OrElse HashString.Length = 0 Then
                ' Fasta generator returned empty hash string
                m_message = "m_FastaTools.ExportFASTAFile returned an empty Hash string for the OrgDB; unable to continue; " & OrgDBDescription
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisResources.CreateFastaFile(), " & m_message)
                Return False
            End If

            If m_DebugLevel >= 1 Then
                ' Log the name of the .Fasta file we're using
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Fasta generation complete, using database: " & m_FastaFileName)

                If m_DebugLevel >= 2 Then
                    ' Also log the file creation and modification dates
                    Try
                        Dim fiFastaFile As System.IO.FileInfo
                        Dim strFastaFileMsg As String
                        fiFastaFile = New System.IO.FileInfo(System.IO.Path.Combine(DestFolder, m_FastaFileName))

                        strFastaFileMsg = "Fasta file last modified: " & GetHumanReadableTimeInterval(System.DateTime.Now.Subtract(fiFastaFile.LastWriteTime)) & " ago at " & fiFastaFile.LastWriteTime.ToString()
                        strFastaFileMsg &= "; file created: " & GetHumanReadableTimeInterval(System.DateTime.Now.Subtract(fiFastaFile.CreationTime)) & " ago at " & fiFastaFile.CreationTime.ToString()
                        strFastaFileMsg &= "; file size: " & fiFastaFile.Length & " bytes"

                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, strFastaFileMsg)
                    Catch ex As Exception
                        ' Ignore errors here
                    End Try
                End If

            End If

            'If we got to here, everything worked OK
            Return True

        End Function

        ''' <summary>
        ''' Converts the given timespan to the total days, hours, minutes, or seconds as a string
        ''' </summary>
        ''' <param name="dtInterval">Timespan to convert</param>
        ''' <returns>Timespan length in human readable form</returns>
        ''' <remarks></remarks>
        Protected Function GetHumanReadableTimeInterval(ByVal dtInterval As System.TimeSpan) As String

            If dtInterval.TotalDays >= 1 Then
                ' Report Days
                Return dtInterval.TotalDays.ToString("0.00") & " days"
            ElseIf dtInterval.TotalHours >= 1 Then
                ' Report hours
                Return dtInterval.TotalHours.ToString("0.00") & " hours"
            ElseIf dtInterval.TotalMinutes >= 1 Then
                ' Report minutes
                Return dtInterval.TotalMinutes.ToString("0.00") & " minutes"
            Else
                ' Report seconds
                Return dtInterval.TotalSeconds.ToString("0.0") & " seconds"
            End If
        End Function

        ''' <summary>
        ''' Overrides base class version of the function to creates a Sequest params file compatible 
        '''	with the Bioworks version on this system. Uses ParamFileGenerator dll provided by Ken Auberry
        ''' </summary>
        ''' <param name="ParamFileName">Name of param file to be created</param>
        ''' <param name="ParamFilePath">Param file storage path</param>
        ''' <param name="WorkDir">Working directory on analysis machine</param>
        ''' <returns>TRUE for success; FALSE for failure</returns>
        ''' <remarks>NOTE: ParamFilePath isn't used in this override, but is needed in parameter list for compatability</remarks>
        Protected Overridable Function RetrieveGeneratedParamFile(ByVal ParamFileName As String, ByVal ParamFilePath As String, _
          ByVal WorkDir As String) As Boolean

            Dim ParFileGen As IGenerateFile
            Dim blnSuccess As Boolean

            Try
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Retrieving parameter file")

                ParFileGen = New clsMakeParameterFile
                ParFileGen.TemplateFilePath = m_mgrParams.GetParam("paramtemplateloc")

                blnSuccess = ParFileGen.MakeFile(ParamFileName, SetBioworksVersion(m_jobParams.GetParam("ToolName")), _
                 Path.Combine(m_mgrParams.GetParam("orgdbdir"), m_jobParams.GetParam("generatedFastaName")), _
                 WorkDir, m_mgrParams.GetParam("connectionstring"), CInt(m_jobParams.GetParam("DatasetID")))

                If blnSuccess Then
                    If m_DebugLevel >= 3 Then
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Successfully retrieved param file: " & ParamFileName)
                    End If

                    Return True
                Else
                    m_message = "Error converting param file"
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ParFileGen.LastError)
                    Return False
                End If

            Catch ex As Exception
                Dim Msg As String = m_message & ": " & ex.Message & "; " & clsGlobal.GetExceptionStackTrace(ex)
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, Msg)
                If Not ParFileGen Is Nothing Then
                    If Not ParFileGen.LastError Is Nothing Then
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error converting param file: " & ParFileGen.LastError)
                    End If
                End If
                Return False
            End Try

        End Function

        ''' <summary>
        ''' This is just a generic function to copy files to the working directory
        '''	
        ''' </summary>
        ''' <param name="FileName">Name of file to be copied</param>
        ''' <param name="FilePath">File storage path</param>
        ''' <param name="WorkDir">Working directory on analysis machine</param>
        ''' <returns>TRUE for success; FALSE for failure</returns>
        Protected Overridable Function RetrieveFile(ByVal FileName As String, ByVal FilePath As String, _
          ByVal WorkDir As String) As Boolean

            'Copy the file
            If Not CopyFileToWorkDir(FileName, FilePath, m_mgrParams.GetParam("WorkDir"), clsLogTools.LogLevels.ERROR) Then
                Return False
            End If

            Return True

        End Function

        ''' <summary>
        ''' Specifies the Bioworks version for use by the Param File Generator DLL
        ''' </summary>
        ''' <param name="ToolName">Version specified in mgr config file</param>
        ''' <returns>IGenerateFile.ParamFileType based on input version</returns>
        ''' <remarks></remarks>
        Protected Overridable Function SetBioworksVersion(ByVal ToolName As String) As IGenerateFile.ParamFileType

            Dim strToolNameLCase As String

            strToolNameLCase = ToolName.ToLower

            'Converts the setup file entry for the Bioworks version to a parameter type compatible with the
            '	parameter file generator dll
            Select Case strToolNameLCase
                Case "20"
                    Return IGenerateFile.ParamFileType.BioWorks_20
                Case "30"
                    Return IGenerateFile.ParamFileType.BioWorks_30
                Case "31"
                    Return IGenerateFile.ParamFileType.BioWorks_31
                Case "32"
                    Return IGenerateFile.ParamFileType.BioWorks_32
                Case "sequest"
                    Return IGenerateFile.ParamFileType.BioWorks_Current
                Case "xtandem"
                    Return IGenerateFile.ParamFileType.X_Tandem
                Case "inspect"
                    Return IGenerateFile.ParamFileType.Inspect
                Case Else
                    ' Did not find an exact match
                    ' Try a substring match
                    If strToolNameLCase.Contains("sequest") Then
                        Return IGenerateFile.ParamFileType.BioWorks_Current
                    ElseIf strToolNameLCase.Contains("xtandem") Then
                        Return IGenerateFile.ParamFileType.X_Tandem
                    ElseIf strToolNameLCase.Contains("inspect") Then
                        Return IGenerateFile.ParamFileType.Inspect
                    Else
                        Return Nothing
                    End If
            End Select

        End Function

        ''' <summary>
        ''' Retrieves zipped, concatenated DTA file, unzips, and splits into individual DTA files
        ''' </summary>
        ''' <param name="UnConcatenate">TRUE to split concatenated file; FALSE to leave the file concatenated</param>
        ''' <returns>TRUE for success, FALSE for error</returns>
        ''' <remarks></remarks>
        Public Overridable Function RetrieveDtaFiles(ByVal UnConcatenate As Boolean) As Boolean

            Dim SourceFileName As String
            Dim SourceFolderPath As String

            'Retrieve zipped DTA file
            SourceFileName = m_jobParams.GetParam("DatasetNum") & "_dta.zip"
            SourceFolderPath = FindDataFile(SourceFileName)

            If SourceFolderPath = "" Then
                ' Couldn't find a folder with the _dta.zip file; how about the _dta.txt file?

                SourceFileName = m_jobParams.GetParam("DatasetNum") & "_dta.txt"
                SourceFolderPath = FindDataFile(SourceFileName)

                If SourceFolderPath = "" Then
                    ' No folder found containing the zipped DTA files; return False
                    ' (the FindDataFile procedure should have already logged an error)
                    Return False
                Else
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Warning: could not find the _dta.zip file, but was able to find " & SourceFileName & " in folder " & SourceFolderPath)

                    'Copy the _dta.txt file
                    If Not CopyFileToWorkDir(SourceFileName, SourceFolderPath, m_mgrParams.GetParam("WorkDir"), clsLogTools.LogLevels.ERROR) Then
                        If m_DebugLevel >= 2 Then
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "CopyFileToWorkDir returned False for " & SourceFileName & " using folder " & SourceFolderPath)
                        End If
                        Return False
                    End If

                End If

            Else

                'Copy the _dta.zip file
                If Not CopyFileToWorkDir(SourceFileName, SourceFolderPath, m_mgrParams.GetParam("WorkDir"), clsLogTools.LogLevels.ERROR) Then
                    If m_DebugLevel >= 1 Then
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "CopyFileToWorkDir returned False for " & SourceFileName & " using folder " & SourceFolderPath)
                    End If
                    Return False
                Else
                    If m_DebugLevel >= 1 Then
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Copied " & SourceFileName & " from folder " & SourceFolderPath)
                    End If
                End If

                'Unzip concatenated DTA file
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Unzipping concatenated DTA file")
                If UnzipFileStart(Path.Combine(m_mgrParams.GetParam("WorkDir"), SourceFileName), m_mgrParams.GetParam("WorkDir"), "clsAnalysisResources.RetrieveDtaFiles", False) Then
                    If m_DebugLevel >= 1 Then
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Concatenated DTA file unzipped")
                    End If
                End If

            End If

            'Unconcatenate DTA file if needed
            If UnConcatenate Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Splitting concatenated DTA file")
                Dim BackWorker As New System.ComponentModel.BackgroundWorker
                Dim FileSplitter As New clsSplitCattedFiles(BackWorker)
                FileSplitter.SplitCattedDTAsOnly(m_jobParams.GetParam("DatasetNum"), m_mgrParams.GetParam("WorkDir"))

                If m_DebugLevel >= 1 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Completed splitting concatenated DTA file")
                End If
            End If

            Return True

        End Function

        ''' <summary>
        ''' Retrieves zipped, concatenated OUT file, unzips, and splits into individual OUT files
        ''' </summary>
        ''' <param name="UnConcatenate">TRUE to split concatenated file; FALSE to leave the file concatenated</param>
        ''' <returns>TRUE for success, FALSE for error</returns>
        ''' <remarks></remarks>
        Protected Overridable Function RetrieveOutFiles(ByVal UnConcatenate As Boolean) As Boolean

            'Retrieve zipped OUT file
            Dim ZippedFileName As String = m_jobParams.GetParam("DatasetNum") & "_out.zip"
            Dim ZippedFolderName As String = FindDataFile(ZippedFileName)

            If ZippedFolderName = "" Then Return False 'No folder found containing the zipped OUT files
            'Copy the file
            If Not CopyFileToWorkDir(ZippedFileName, ZippedFolderName, m_mgrParams.GetParam("WorkDir"), clsLogTools.LogLevels.ERROR) Then
                Return False
            End If

            'Unzip concatenated OUT file
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Unzipping concatenated OUT file")
            If UnzipFileStart(Path.Combine(m_mgrParams.GetParam("WorkDir"), ZippedFileName), m_mgrParams.GetParam("WorkDir"), "clsAnalysisResources.RetrieveOutFiles", False) Then
                If m_DebugLevel >= 1 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Concatenated OUT file unzipped")
                End If
            End If


            'Unconcatenate OUT file if needed
            If UnConcatenate Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Splitting concatenated OUT file")
                Dim BackWorker As New System.ComponentModel.BackgroundWorker
                Dim FileSplitter As New clsSplitCattedFiles(BackWorker)
                FileSplitter.SplitCattedOutsOnly(m_jobParams.GetParam("DatasetNum"), m_mgrParams.GetParam("WorkDir"))

                If m_DebugLevel >= 1 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Completed splitting concatenated OUT file")
                End If
            End If

            Return True

        End Function

        ''' <summary>
        ''' Finds the server or archive folder where specified file is located
        ''' </summary>
        ''' <param name="FileToFind">Name of the file to search for</param>
        ''' <returns></returns>
        ''' <remarks></remarks>
        Protected Overridable Function FindDataFile(ByVal FileToFind As String) As String

            Dim FoldersToSearch As New StringCollection
            Dim TempDir As String
            Dim FileFound As Boolean = False

            'NOTE: Someday this will have to be able to handle a list as the SharedResultsFolders value

            Try
                'Fill collection with possible folder locations
                TempDir = Path.Combine(m_jobParams.GetParam("transferFolderPath"), m_jobParams.GetParam("DatasetFolderName"))    'Xfer folder/Dataset folder
                TempDir = Path.Combine(TempDir, m_jobParams.GetParam("inputFolderName"))     'Xfer folder (cont)
                FoldersToSearch.Add(TempDir)

                TempDir = Path.Combine(m_jobParams.GetParam("transferFolderPath"), m_jobParams.GetParam("DatasetFolderName"))    'Xfer folder/Shared results folder
                TempDir = Path.Combine(TempDir, m_jobParams.GetParam("SharedResultsFolders"))
                FoldersToSearch.Add(TempDir)

                TempDir = Path.Combine(m_jobParams.GetParam("DatasetStoragePath"), m_jobParams.GetParam("DatasetFolderName"))    'Storage server/Dataset folder
                TempDir = Path.Combine(TempDir, m_jobParams.GetParam("inputFolderName"))
                FoldersToSearch.Add(TempDir)

                TempDir = Path.Combine(m_jobParams.GetParam("DatasetStoragePath"), m_jobParams.GetParam("DatasetFolderName"))    'Storage server/Shared results folder
                TempDir = Path.Combine(TempDir, m_jobParams.GetParam("SharedResultsFolders"))
                FoldersToSearch.Add(TempDir)

                TempDir = Path.Combine(m_jobParams.GetParam("DatasetArchivePath"), m_jobParams.GetParam("DatasetFolderName"))    'Archive/Dataset folder
                TempDir = Path.Combine(TempDir, m_jobParams.GetParam("inputFolderName"))
                FoldersToSearch.Add(TempDir)

                TempDir = Path.Combine(m_jobParams.GetParam("DatasetArchivePath"), m_jobParams.GetParam("DatasetFolderName"))    'Archive/Shared results folder
                TempDir = Path.Combine(TempDir, m_jobParams.GetParam("SharedResultsFolders"))
                FoldersToSearch.Add(TempDir)

                For Each TempDir In FoldersToSearch
                    Try
                        If System.IO.Directory.Exists(TempDir) Then
                            If File.Exists(Path.Combine(TempDir, FileToFind)) Then
                                FileFound = True
                                Exit For
                            End If
                        End If
                    Catch ex As Exception
                        ' Exception checking TempDir; log an error, but continue checking the other folders in FoldersToSearch
                        m_message = "Exception in FindDataFile looking for: " & FileToFind & " in " & TempDir & ": " & ex.Message
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
                    End Try
                Next

                If FileFound Then
                    If m_DebugLevel >= 2 Then
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Data file found: " & FileToFind)
                    End If
                    Return TempDir
                Else
                    'Big problem, data file not found
                    m_message = "Data file not found: " & FileToFind
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
                    Return String.Empty
                End If

            Catch ex As Exception
                m_message = "Exception in FindDataFile looking for: " & FileToFind & ": " & ex.Message
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
            End Try

            ' We'll only get here if an exception occurs
            Return String.Empty

        End Function

        ''' <summary>
        ''' Retrieves specified file from storage server, xfer folder, or archive and unzips if necessary
        ''' </summary>
        ''' <param name="FileName">Name of file to be retrieved</param>
        ''' <param name="Unzip">TRUE if retrieved file should be unzipped after retrieval</param>
        ''' <returns>TRUE for success; FALSE for failure</returns>
        ''' <remarks></remarks>
        Protected Overridable Function FindAndRetrieveMiscFiles(ByVal FileName As String, ByVal Unzip As Boolean) As Boolean

            'Find file location
            Dim FolderName As String = FindDataFile(FileName)

            'Exit if file was not found
            If FolderName = "" Then Return False 'No folder found containing the specified file

            'Copy the file
            If Not CopyFileToWorkDir(FileName, FolderName, m_mgrParams.GetParam("WorkDir"), clsLogTools.LogLevels.ERROR) Then
                Return False
            End If

            'Return or unzip file, as specified
            If Not Unzip Then Return True

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Unzipping file " & FileName)
            If UnzipFileStart(Path.Combine(m_mgrParams.GetParam("WorkDir"), FileName), m_mgrParams.GetParam("WorkDir"), "clsAnalysisResources.FindAndRetrieveMiscFiles", False) Then
                If m_DebugLevel >= 1 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Unzipped file " & FileName)
                End If
            End If


            Return True

        End Function

        ''' <summary>
        ''' Creates the specified settings file from db info
        ''' </summary>
        ''' <returns>TRUE if file created successfully; FALSE otherwise</returns>
        ''' <remarks>Use this overload with jobs where settings file is retrieved from database</remarks>
        Protected Friend Overridable Function RetrieveSettingsFileFromDb() As Boolean

            Dim OutputFile As String = Path.Combine(m_mgrParams.GetParam("workdir"), m_jobParams.GetParam("SettingsFileName"))

            Return CreateSettingsFile(m_jobParams.GetParam("ParameterXML"), OutputFile)

        End Function

        ''' <summary>
        ''' Creates an XML formatted settings file based on data from broker
        ''' </summary>
        ''' <param name="FileText">String containing XML file contents</param>
        ''' <param name="FileNamePath">Name of file to create</param>
        ''' <returns>TRUE for success; FALSE for failure</returns>
        ''' <remarks>XML handling based on code provided by Matt Monroe</remarks>
        Private Function CreateSettingsFile(ByVal FileText As String, ByVal FileNamePath As String) As Boolean

            Dim objFormattedXMLWriter As New clsFormattedXMLWriter

            If Not objFormattedXMLWriter.WriteXMLToFile(FileText, FileNamePath) Then
                m_message = "Error creating settings file"
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & " " & FileNamePath & ": " & objFormattedXMLWriter.ErrMsg)
                Return False
            Else
                Return True
            End If

        End Function

        Public Function UnzipFileStart(ByVal ZipFilePath As String, ByVal OutFolderPath As String, ByVal CallingFunctionName As String, ByVal ForceExternalZipProgramUse As Boolean) As Boolean

            Const TWO_GB As Long = 2L * 1024 * 1024 * 1024

            Dim fi As System.IO.FileInfo
            Dim sngFileSizeMB As Single

            Dim blnUseExternalUnzipper As Boolean = False
            Dim blnSuccess As Boolean = False

            Dim strExternalUnzipperFilePath As String
            Dim strUnzipperName As String = String.Empty

            Dim dtStartTime As DateTime
            Dim dtEndTime As DateTime
            Dim dblUnzipTimeSeconds As Double
            Dim dblUnzipSpeedMBPerSec As Double
            Try
                If ZipFilePath Is Nothing Then ZipFilePath = String.Empty

                If CallingFunctionName Is Nothing OrElse CallingFunctionName.Length = 0 Then
                    CallingFunctionName = "??"
                End If

                strExternalUnzipperFilePath = m_mgrParams.GetParam("zipprogram")
                If strExternalUnzipperFilePath Is Nothing Then strExternalUnzipperFilePath = String.Empty


                fi = New System.IO.FileInfo(ZipFilePath)
                sngFileSizeMB = CSng(fi.Length / 1024 / 1024)

                If Not fi.Exists Then
                    ' File not found
                    m_message = "Error unzipping '" & ZipFilePath & "': File not found (called from " & CallingFunctionName & ")"

                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
                    Return False
                End If

                If ForceExternalZipProgramUse Then
                    blnUseExternalUnzipper = True
                Else
                    ' Examine the size of ZipFilePath
                    If fi.Length >= TWO_GB Then
                        ' File is over 2 GB in size; use the external unzipper if SharpZipLib cannot handle Zip64 files
                        blnUseExternalUnzipper = Not SHARPZIPLIB_HANDLES_ZIP64
                    End If

                    If sngFileSizeMB >= SHARPZIPLIB_MAX_FILESIZE_MB Then
                        If strExternalUnzipperFilePath.Length > 0 AndAlso _
                           strExternalUnzipperFilePath.ToLower <> "na" AndAlso _
                           System.IO.File.Exists(strExternalUnzipperFilePath) Then
                            blnUseExternalUnzipper = True
                        End If
                    End If
                End If

                If blnUseExternalUnzipper Then
                    strUnzipperName = System.IO.Path.GetFileName(strExternalUnzipperFilePath)

                    Dim UnZipper As New ZipTools(OutFolderPath, strExternalUnzipperFilePath)

                    dtStartTime = DateTime.Now
                    blnSuccess = UnZipper.UnzipFile("", ZipFilePath, OutFolderPath)
                    dtEndTime = DateTime.Now

                    If Not blnSuccess Then
                        m_message = "Error unzipping " & System.IO.Path.GetFileName(ZipFilePath)
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, CallingFunctionName & ": " & m_message)
                        UnZipper = Nothing
                    End If
                Else
                    ' Use SharpZipLib

                    strUnzipperName = "SharpZipLib"

                    Dim UnZipper As New ICSharpCode.SharpZipLib.Zip.FastZip

                    dtStartTime = DateTime.Now
                    UnZipper.ExtractZip(ZipFilePath, OutFolderPath, String.Empty)
                    dtEndTime = DateTime.Now

                    blnSuccess = True
                End If

                If blnSuccess Then
                    dblUnzipTimeSeconds = dtEndTime.Subtract(dtStartTime).TotalSeconds
                    dblUnzipSpeedMBPerSec = sngFileSizeMB / dblUnzipTimeSeconds

                    If m_DebugLevel >= 2 Then
                        m_message = "Unzipped " & System.IO.Path.GetFileName(ZipFilePath) & " using " & strUnzipperName & "; elapsed time = " & dblUnzipTimeSeconds.ToString("0.0") & " seconds; rate = " & dblUnzipSpeedMBPerSec.ToString("0.0") & " MB/sec; calling function = " & CallingFunctionName
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, m_message)
                    End If
                End If

            Catch ex As Exception
                m_message = "Exception while unzipping '" & ZipFilePath & "'"
                If strUnzipperName.Length > 0 Then m_message &= " using " & strUnzipperName

                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message & ": " & ex.Message & "; " & clsGlobal.GetExceptionStackTrace(ex))
                blnSuccess = False
            End Try

            Return blnSuccess

        End Function
#End Region

    End Class

End Namespace
