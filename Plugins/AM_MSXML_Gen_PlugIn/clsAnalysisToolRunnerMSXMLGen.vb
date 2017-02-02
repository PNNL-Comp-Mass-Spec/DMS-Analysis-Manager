'*********************************************************************************************************
' Written by John Sandoval for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2009, Battelle Memorial Institute
' Created 02/06/2009
'
'*********************************************************************************************************

Option Strict On

Imports System.Collections.Generic
Imports AnalysisManagerBase
Imports System.IO
Imports System.Runtime.InteropServices

' ReSharper disable once UnusedMember.Global
Public Class clsAnalysisToolRunnerMSXMLGen
    Inherits clsAnalysisToolRunnerBase

    '*********************************************************************************************************
    'Class for running MS XML generator
    'Currently used to generate MZXML or MZML files
    '*********************************************************************************************************

#Region "Module Variables"

    Private Const PROGRESS_PCT_MSXML_GEN_RUNNING As Single = 5

    Private mMSXmlGeneratorAppPath As String = String.Empty

    Private mMSXmlOutputFileType As clsAnalysisResources.MSXMLOutputTypeConstants

    Private mMSXmlCacheFolder As DirectoryInfo

#End Region

#Region "Methods"

    ''' <summary>
    ''' Runs ReAdW or MSConvert
    ''' </summary>
    ''' <returns>CloseOutType enum indicating success or failure</returns>
    ''' <remarks></remarks>
    Public Overrides Function RunTool() As IJobParams.CloseOutType
        Dim result As IJobParams.CloseOutType

        'Do the base class stuff
        If Not MyBase.RunTool = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        ' Store the ReAdW or MSConvert version info in the database
        If Not StoreToolVersionInfo() Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                                 "Aborting since StoreToolVersionInfo returned false")
            LogError("Error determining MSXMLGen version")
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        Dim storeInCache = m_jobParams.GetJobParameter("StoreMSXmlInCache", True)
        If storeInCache Then
            Dim msXMLCacheFolderPath As String = m_mgrParams.GetParam("MSXMLCacheFolderPath", String.Empty)
            mMSXmlCacheFolder = New DirectoryInfo(msXMLCacheFolderPath)

            If Not mMSXmlCacheFolder.Exists Then
                LogError("MSXmlCache folder not found: " & msXMLCacheFolderPath)
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If
        End If

        If CreateMSXMLFile() <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            Return result
        End If

        If Not PostProcessMSXmlFile() Then
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End If

        'Stop the job timer
        m_StopTime = DateTime.UtcNow

        'Add the current job data to the summary file
        If Not UpdateSummaryFile() Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN,
                                 "Error creating summary file, job " & m_JobNum & ", step " &
                                 m_jobParams.GetParam("Step"))
        End If

        result = MakeResultsFolder()
        If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            'MakeResultsFolder handles posting to local log, so set database error message and exit
            m_message = "Error making results folder"
            Return result
        End If

        result = MoveResultFiles()
        If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            ' Note that MoveResultFiles should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
            m_message = "Error moving files into results folder"
            Return result
        End If

        result = CopyResultsFolderToServer()
        If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
            ' Note that CopyResultsFolderToServer should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
            Return result
        End If

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS 'No failures so everything must have succeeded
    End Function

    ''' <summary>
    ''' Generate the mzXML or mzML file
    ''' </summary>
    ''' <returns>CloseOutType enum indicating success or failure</returns>
    ''' <remarks></remarks>
    Private Function CreateMSXMLFile() As IJobParams.CloseOutType

        Try

            If m_DebugLevel > 4 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG,
                                     "clsAnalysisToolRunnerMSXMLGen.CreateMSXMLFile(): Enter")
            End If

            Dim msXmlGenerator As String = m_jobParams.GetParam("MSXMLGenerator")           ' ReAdW.exe or MSConvert.exe
            Dim msXmlFormat As String = m_jobParams.GetParam("MSXMLOutputType")             ' Typically mzXML or mzML

            ' Determine the output type
            Select Case msXmlFormat.ToLower()
                Case "mzxml"
                    mMSXmlOutputFileType = clsAnalysisResources.MSXMLOutputTypeConstants.mzXML
                Case "mzml"
                    mMSXmlOutputFileType = clsAnalysisResources.MSXMLOutputTypeConstants.mzML
                Case Else
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN,
                                         "msXmlFormat string is not mzXML or mzML (" & msXmlFormat &
                                         "); will default to mzXML")
                    mMSXmlOutputFileType = clsAnalysisResources.MSXMLOutputTypeConstants.mzXML
            End Select

            ' Lookup Centroid Settings
            Dim centroidMSXML = m_jobParams.GetJobParameter("CentroidMSXML", False)
            Dim centroidMS1 = m_jobParams.GetJobParameter("CentroidMS1", False)
            Dim centroidMS2 = m_jobParams.GetJobParameter("CentroidMS2", False)

            If centroidMSXML Then
                centroidMS1 = True
                centroidMS2 = True
            End If

            ' Look for parameter CentroidPeakCountToRetain in the MSXMLGenerator section
            ' If the value is -1, then will retain all data points
            Dim centroidPeakCountToRetain = m_jobParams.GetJobParameter("MSXMLGenerator", "CentroidPeakCountToRetain", 0)

            If centroidPeakCountToRetain = 0 Then
                ' Look for parameter CentroidPeakCountToRetain in any section
                centroidPeakCountToRetain = m_jobParams.GetJobParameter("CentroidPeakCountToRetain",
                                                                        clsMSXmlGenMSConvert.
                                                                           DEFAULT_CENTROID_PEAK_COUNT_TO_RETAIN)
            End If

            ' Look for custom processing arguments
            Dim customMSConvertArguments = m_jobParams.GetJobParameter("MSXMLGenerator", "CustomMSConvertArguments", "")

            If String.IsNullOrEmpty(mMSXmlGeneratorAppPath) Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN,
                                     "mMSXmlGeneratorAppPath is empty; this is unexpected")
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            Dim rawDataType As String = m_jobParams.GetParam("RawDataType")
            Dim eRawDataType = clsAnalysisResources.GetRawDataType(rawDataType)

            Dim msXmlGen As clsMSXmlGen

            ' Determine the program path and Instantiate the processing class
            If msXmlGenerator.ToLower.Contains("readw") Then
                ' ReAdW
                ' mMSXmlGeneratorAppPath should have been populated during the call to StoreToolVersionInfo()

                msXmlGen = New clsMSXMLGenReadW(m_WorkDir, mMSXmlGeneratorAppPath, m_Dataset, eRawDataType,
                                                 mMSXmlOutputFileType, centroidMS1 Or centroidMS2)

                If rawDataType <> clsAnalysisResources.RAW_DATA_TYPE_DOT_RAW_FILES Then
                    LogError("ReAdW can only be used with .Raw files, not with " & rawDataType)
                    Return IJobParams.CloseOutType.CLOSEOUT_FAILED
                End If

            ElseIf msXmlGenerator.ToLower.Contains("msconvert") Then
                ' MSConvert

                If String.IsNullOrWhiteSpace(CustomMSConvertArguments) Then
                    msXmlGen = New clsMSXmlGenMSConvert(m_WorkDir, mMSXmlGeneratorAppPath, m_Dataset, eRawDataType,
                                                         mMSXmlOutputFileType, centroidMS1, centroidMS2,
                                                         centroidPeakCountToRetain)
                Else
                    msXmlGen = New clsMSXmlGenMSConvert(m_WorkDir, mMSXmlGeneratorAppPath, m_Dataset, eRawDataType,
                                                         mMSXmlOutputFileType, customMSConvertArguments)
                End If

            Else
                LogError("Unsupported XmlGenerator: " & msXmlGenerator)
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            ' Attach events to msXmlGen
            AddHandler msXmlGen.LoopWaiting, AddressOf MSXmlGen_LoopWaiting
            AddHandler msXmlGen.ProgRunnerStarting, AddressOf MSXmlGen_ProgRunnerStarting

            msXmlGen.DebugLevel = m_DebugLevel

            If Not File.Exists(mMSXmlGeneratorAppPath) Then
                LogError("MsXmlGenerator not found: " & mMSXmlGeneratorAppPath)
                Return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND
            End If

            ' Create the file
            Dim success = msXmlGen.CreateMSXMLFile()

            If Not success Then
                LogError(msXmlGen.ErrorMessage)
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED

            End If

            If msXmlGen.ErrorMessage.Length > 0 Then
                LogError(msXmlGen.ErrorMessage)
            End If

        Catch ex As Exception
            LogError("Exception in CreateMSXMLFile", ex)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try

        Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS
    End Function

    ''' <summary>
    ''' Get the path to the .Exe to use for recalculating precursor ion m/z and charge values
    ''' </summary>
    ''' <param name="recalculatePrecursorsTool"></param>
    ''' <returns>Path to the exe, or an empty string if an error</returns>
    Private Function GetRecalculatePrecursorsToolProgLoc(<Out()> ByRef recalculatePrecursorsTool As String) As String

        recalculatePrecursorsTool = m_jobParams.GetJobParameter("RecalculatePrecursorsTool", String.Empty)
        If String.IsNullOrWhiteSpace(recalculatePrecursorsTool) Then
            LogError("Job parameter RecalculatePrecursorsTool is not defined in the settings file; cannot determine tool to use")
            Return String.Empty
        End If

        If String.Equals(recalculatePrecursorsTool, clsRawConverterRunner.RAWCONVERTER_FILENAME, StringComparison.InvariantCultureIgnoreCase) Then
            Dim rawConverterDir As String = m_mgrParams.GetParam("RawConverterProgLoc")
            If String.IsNullOrWhiteSpace(rawConverterDir) Then
                LogError("Manager parameter RawConverterProgLoc is not defined; cannot find the folder for " & clsRawConverterRunner.RAWCONVERTER_FILENAME)
                Return String.Empty
            Else
                Return Path.Combine(rawConverterDir, clsRawConverterRunner.RAWCONVERTER_FILENAME)
            End If
        Else
            Return String.Empty
        End If

    End Function

    Private Function PostProcessMSXmlFile() As Boolean
        Try
            Dim resultFileExtension As String

            Select Case mMSXmlOutputFileType
                Case clsAnalysisResources.MSXMLOutputTypeConstants.mzML
                    resultFileExtension = clsAnalysisResources.DOT_MZML_EXTENSION
                Case clsAnalysisResources.MSXMLOutputTypeConstants.mzXML
                    resultFileExtension = clsAnalysisResources.DOT_MZXML_EXTENSION
                Case Else
                    Throw New Exception("Unrecognized MSXMLOutputType value")
            End Select

            Dim msXmlFilePath = Path.Combine(m_WorkDir, m_Dataset & resultFileExtension)
            Dim fiMSXmlFile = New FileInfo(msXmlFilePath)

            If Not fiMSXmlFile.Exists Then
                LogError(resultFileExtension & " file not found: " & Path.GetFileName(msXmlFilePath))
                Return False
            End If

            ' Possibly update the file using results from RawConverter

            Dim recalculatePrecursors = m_jobParams.GetJobParameter("RecalculatePrecursors", False)
            If recalculatePrecursors Then
                Dim success = RecalculatePrecursorIons(fiMSXmlFile)
                If Not success Then
                    Return False
                End If
            End If

            ' Compress the file using GZip
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO,
                                 "GZipping " & fiMSXmlFile.Name)

            ' Note that if this process turns out to be slow, we can have MSConvert do this for us using --gzip
            ' However, that will not work if RecalculatePrecursors is true
            fiMSXmlFile = GZipFile(fiMSXmlFile)
            If fiMSXmlFile Is Nothing Then
                Return False
            End If

            Dim storeInDataset = m_jobParams.GetJobParameter("StoreMSXmlInDataset", False)
            Dim storeInCache = m_jobParams.GetJobParameter("StoreMSXmlInCache", True)

            If Not storeInDataset AndAlso Not storeInCache Then storeInCache = True

            If Not storeInDataset Then
                ' Do not move the .mzXML or .mzML file to the result folder
                m_jobParams.AddResultFileExtensionToSkip(resultFileExtension)
                m_jobParams.AddResultFileExtensionToSkip(clsAnalysisResources.DOT_GZ_EXTENSION)
            End If

            If storeInCache Then
                ' Copy the .mzXML or .mzML file to the MSXML cache
                Dim remoteCachefilePath = CopyFileToServerCache(mMSXmlCacheFolder.FullName, fiMSXmlFile.FullName,
                                                                purgeOldFilesIfNeeded:=True)

                If String.IsNullOrEmpty(remoteCachefilePath) Then
                    If String.IsNullOrEmpty(m_message) Then
                        LogError("CopyFileToServerCache returned false for " & fiMSXmlFile.Name)
                    End If
                    Return False
                End If

                ' Create the _CacheInfo.txt file
                Dim cacheInfoFilePath = msXmlFilePath & "_CacheInfo.txt"
                Using swOutFile = New StreamWriter(New FileStream(cacheInfoFilePath, FileMode.Create, FileAccess.Write, FileShare.Read))
                    swOutFile.WriteLine(remoteCachefilePath)
                End Using

            End If

        Catch ex As Exception
            LogError("Exception in PostProcessMSXmlFile", ex)
            Return False
        End Try

        Return True
    End Function

    ''' <summary>
    ''' Recalculate the precursor ions in a MzML file
    ''' The only supported option at present is RawConverter
    ''' </summary>
    ''' <param name="sourceMsXmlFile">MzML file to read</param>
    ''' <returns>True if success, false if an error</returns>
    Private Function RecalculatePrecursorIons(sourceMsXmlFile As FileInfo) As Boolean

        If mMSXmlOutputFileType <> clsAnalysisResources.MSXMLOutputTypeConstants.mzML Then
            LogError("Unsupported file extension for RecalculatePrecursors=True; must be mzML, not " & mMSXmlOutputFileType.ToString())
            Return False
        End If

        Dim rawDataType As String = m_jobParams.GetParam("RawDataType")
        Dim eRawDataType = clsAnalysisResources.GetRawDataType(rawDataType)
        Dim rawFilePath As String

        If eRawDataType = clsAnalysisResources.eRawDataTypeConstants.ThermoRawFile Then
            rawFilePath = Path.Combine(m_WorkDir, m_Dataset & clsAnalysisResources.DOT_RAW_EXTENSION)
        Else
            LogError("Unsupported dataset type for RecalculatePrecursors=True; must be .Raw, not " & eRawDataType.ToString())
            Return False
        End If

        Dim recalculatePrecursorsTool As String = Nothing
        Dim recalculatePrecursorsToolProgLoc = GetRecalculatePrecursorsToolProgLoc(recalculatePrecursorsTool)
        If String.IsNullOrWhiteSpace(recalculatePrecursorsToolProgLoc) Then
            Return False
        End If

        If String.Equals(recalculatePrecursorsTool, clsRawConverterRunner.RAWCONVERTER_FILENAME, StringComparison.InvariantCultureIgnoreCase) Then
            ' Using RawConverter.exe
            Dim mgfFile As FileInfo = Nothing
            Dim rawConverterExe = New FileInfo(recalculatePrecursorsToolProgLoc)

            Dim rawConverterSuccess = RecalculatePrecursorIonsCreateMGF(rawConverterExe.Directory.FullName, rawFilePath, mgfFile)
            If Not rawConverterSuccess Then Return False

            Dim mzMLUpdated = RecalculatePrecursorIonsUpdateMzML(sourceMsXmlFile, mgfFile)
            Return mzMLUpdated
        Else
            LogError("Unsupported tool for precursursor recalculation: " & recalculatePrecursorsTool)
            Return False
        End If

    End Function

    ''' <summary>
    ''' Use RawConverter to process the Thermo .Raw file and recalculate the precursor ion information, writing the results to a .MGF file
    ''' </summary>
    ''' <param name="rawConverterDir"></param>
    ''' <param name="rawFilePath"></param>
    ''' <param name="mgfFile"></param>
    ''' <returns></returns>
    Private Function RecalculatePrecursorIonsCreateMGF(rawConverterDir As String, rawFilePath As String, <Out()> ByRef mgfFile As FileInfo) As Boolean

        Try
            If m_message Is Nothing Then m_message = String.Empty
            Dim messageAtStart = String.Copy(m_message)

            Dim converter = New clsRawConverterRunner(rawConverterDir, m_DebugLevel)
            RegisterEvents(converter)

            Dim success = converter.ConvertRawToMGF(rawFilePath)

            If Not success Then
                If String.IsNullOrWhiteSpace(m_message) OrElse String.Equals(messageAtStart, m_message) Then
                    LogError("Unknown RawConverter error")
                End If
                mgfFile = Nothing
                Return False
            End If

            ' Confirm that RawConverter created a .mgf file

            mgfFile = New FileInfo(Path.Combine(m_WorkDir, m_Dataset & clsAnalysisResources.DOT_MGF_EXTENSION))
            If Not mgfFile.Exists Then
                LogError("RawConverter did not create file " & mgfFile.Name)
                Return False
            End If

            m_jobParams.AddResultFileToSkip(mgfFile.Name)

            Return True

        Catch ex As Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "RawConverter error", ex)
            m_message = "Exception running RawConverter"
            mgfFile = Nothing
            Return False
        End Try

    End Function

    Private Function RecalculatePrecursorIonsUpdateMzML(sourceMsXmlFile As FileInfo, mgfFile As FileInfo) As Boolean

        Try
            Dim messageAtStart = String.Copy(m_message)

            Dim updater = New clsParentIonUpdater()
            AddHandler updater.ErrorEvent, AddressOf ParentIonUpdater_ErrorEvent
            AddHandler updater.WarningEvent, AddressOf ParentIonUpdater_WarningEvent
            AddHandler updater.ProgressUpdate, AddressOf ParentIonUpdater_ProgressEvent

            Dim updatedMzMLPath = updater.UpdateMzMLParentIonInfoUsingMGF(sourceMsXmlFile.FullName, mgfFile.FullName, False)

            If String.IsNullOrEmpty(updatedMzMLPath) Then
                If String.IsNullOrWhiteSpace(m_message) OrElse String.Equals(messageAtStart, m_message) Then
                    LogError("Unknown ParentIonUpdater error")
                End If
                Return False
            End If

            ' Confirm that clsParentIonUpdater created a new .mzML file

            Dim updatedMzMLFile = New FileInfo(updatedMzMLPath)
            If Not updatedMzMLFile.Exists Then
                LogError("ParentIonUpdater did not create file " & mgfFile.Name)
                Return False
            End If

            Dim finalMsXmlFilePath = String.Copy(sourceMsXmlFile.FullName)

            ' Delete the original mzML file
            Threading.Thread.Sleep(125)
            sourceMsXmlFile.Delete()

            Threading.Thread.Sleep(125)

            ' Rename the updated mzML file so that it does not end in _new.mzML
            updatedMzMLFile.MoveTo(finalMsXmlFilePath)

            ' Re-index the mzML file using MSConvert

            Dim success = ReindexMzML(finalMsXmlFilePath)

            Return success

        Catch ex As Exception
            LogError("RecalculatePrecursorIonsUpdateMzML error", ex)
            m_message = "Exception in RecalculatePrecursorIonsUpdateMzML"
            Return False
        End Try

    End Function

    Private Function ReindexMzML(mzMLFilePath As String) As Boolean
        Try

            If m_DebugLevel > 4 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG,
                                     "Re-index the mzML file using MSConvert")
            End If

            Dim msXmlGenerator As String = m_jobParams.GetParam("MSXMLGenerator")           ' Must be MSConvert.exe

            If Not msXmlGenerator.ToLower().Contains("msconvert") Then
                LogError("ParentIonUpdater only supports MSConvert, not " & msXmlGenerator)
                Return False
            End If

            If String.IsNullOrEmpty(mMSXmlGeneratorAppPath) Then
                LogError("mMSXmlGeneratorAppPath is empty; this is unexpected")
                Return False
            End If

            Dim eRawDataType = clsAnalysisResources.eRawDataTypeConstants.mzML
            Dim outputFileType = clsAnalysisResources.MSXMLOutputTypeConstants.mzML
            Dim centroidMS1 = False
            Dim centroidMS2 = False

            Dim sourcefileBase = Path.GetFileNameWithoutExtension(mzMLFilePath)

            Dim msConvertRunner = New clsMSXmlGenMSConvert(m_WorkDir, mMSXmlGeneratorAppPath, sourcefileBase, eRawDataType,
                                                         outputFileType, centroidMS1, centroidMS2, 0)

            msConvertRunner.ConsoleOutputSuffix = "_Reindex"
            msConvertRunner.DebugLevel = m_DebugLevel

            If Not File.Exists(mMSXmlGeneratorAppPath) Then
                LogError("MsXmlGenerator not found: " & mMSXmlGeneratorAppPath)
                Return False
            End If

            ' Create the file
            Dim success = msConvertRunner.CreateMSXMLFile()

            If Not success Then
                LogError(msConvertRunner.ErrorMessage)
                Return False
            Else
                m_jobParams.AddResultFileToSkip(msConvertRunner.ConsoleOutputFileName)
            End If

            If msConvertRunner.ErrorMessage.Length > 0 Then
                LogError(msConvertRunner.ErrorMessage)
            End If

            ' Replace the original .mzML file with the new .mzML file
            Dim reindexedMzMLFile = New FileInfo(Path.Combine(m_WorkDir, msConvertRunner.OutputFileName))

            If Not reindexedMzMLFile.Exists Then
                LogError("Reindexed mzML file not found at " & reindexedMzMLFile.FullName)
                m_message = "Reindexed mzML file not found"
                Return False
            End If

            ' Replace the original .mzML file with the indexed one
            Threading.Thread.Sleep(125)
            File.Delete(mzMLFilePath)
            Threading.Thread.Sleep(125)

            reindexedMzMLFile.MoveTo(mzMLFilePath)
            Return True

        Catch ex As Exception
            LogError("ReindexMzML error", ex)
            m_message = "Exception in ReindexMzML"
            Return False
        End Try

    End Function

    ''' <summary>
    ''' Stores the tool version info in the database
    ''' </summary>
    ''' <remarks></remarks>
    Private Function StoreToolVersionInfo() As Boolean

        Dim strToolVersionInfo As String = String.Empty

        If m_DebugLevel >= 2 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG,
                                 "Determining tool version info")
        End If

        ' Store paths to key files in ioToolFiles
        Dim ioToolFiles As New List(Of FileInfo)

        ' Determine the path to the XML Generator
        Dim msXmlGenerator As String = m_jobParams.GetParam("MSXMLGenerator")           ' ReAdW.exe or MSConvert.exe

        mMSXmlGeneratorAppPath = String.Empty
        If msXmlGenerator.ToLower().Contains("readw") Then
            ' ReAdW
            ' Note that msXmlGenerator will likely be ReAdW.exe
            mMSXmlGeneratorAppPath = MyBase.DetermineProgramLocation("ReAdW", "ReAdWProgLoc", msXmlGenerator)

        ElseIf msXmlGenerator.ToLower().Contains("msconvert") Then
            ' MSConvert
            ' MSConvert.exe is stored in the ProteoWizard folder
            Dim ProteoWizardDir As String = m_mgrParams.GetParam("ProteoWizardDir")
            mMSXmlGeneratorAppPath = Path.Combine(ProteoWizardDir, msXmlGenerator)

        Else
            LogError("Invalid value for MSXMLGenerator; should be 'ReAdW' or 'MSConvert'")
            Return False
        End If

        If Not String.IsNullOrEmpty(mMSXmlGeneratorAppPath) Then
            ioToolFiles.Add(New FileInfo(mMSXmlGeneratorAppPath))
        Else
            ' Invalid value for ProgramPath
            LogError("MSXMLGenerator program path is empty")
            Return False
        End If

        Dim recalculatePrecursors = m_jobParams.GetJobParameter("RecalculatePrecursors", False)
        If recalculatePrecursors Then

            Dim recalculatePrecursorsTool As String = Nothing
            Dim recalculatePrecursorsToolProgLoc = GetRecalculatePrecursorsToolProgLoc(recalculatePrecursorsTool)

            If Not String.IsNullOrEmpty(recalculatePrecursorsToolProgLoc) Then
                ioToolFiles.Add(New FileInfo(recalculatePrecursorsToolProgLoc))
            End If

        End If

        Try
            Return MyBase.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles, blnSaveToolVersionTextFile:=True)
        Catch ex As Exception
            LogError("Exception calling SetStepTaskToolVersion", ex)
            Return False
        End Try

    End Function

#End Region

#Region "Event Handlers"

    ''' <summary>
    ''' Event handler for msXmlGen.LoopWaiting event
    ''' </summary>
    ''' <remarks></remarks>
    Private Sub MSXmlGen_LoopWaiting()
        UpdateStatusFile(PROGRESS_PCT_MSXML_GEN_RUNNING)
        LogProgress("MSXmlGen")
    End Sub

    ''' <summary>
    ''' Event handler for msXmlGen.ProgRunnerStarting event
    ''' </summary>
    ''' <param name="CommandLine">The command being executed (program path plus command line arguments)</param>
    ''' <remarks></remarks>
    Private Sub MSXmlGen_ProgRunnerStarting(CommandLine As String)
        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, CommandLine)
    End Sub

    Private Sub ParentIonUpdater_ErrorEvent(strMessage As String)
        Console.WriteLine(m_message)
        LogError(m_message)
    End Sub

    Private Sub ParentIonUpdater_WarningEvent(strMessage As String)
        Console.WriteLine(strMessage)
        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, strMessage)
    End Sub

    Private Sub ParentIonUpdater_ProgressEvent(progressMessage As String, percentcomplete As Integer)
        Console.WriteLine(progressMessage)
        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, progressMessage)
    End Sub

#End Region
End Class
