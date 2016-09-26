Option Strict On

'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
'
' This class is intended to be instantiated by other Analysis Manager plugins
' For example, see AM_MSGF_PlugIn
'
'*********************************************************************************************************

Imports AnalysisManagerBase
Imports System.IO

Public Class clsMSXMLCreator

#Region "Classwide variables"

    Protected ReadOnly mMSXmlGeneratorAppPath As String
    Protected ReadOnly m_jobParams As IJobParams
    Protected ReadOnly m_WorkDir As String
    Protected m_Dataset As String
    Protected ReadOnly m_DebugLevel As Short

    Protected m_ErrorMessage As String

    Protected WithEvents mMSXmlGen As clsMSXmlGen

    Public Event DebugEvent(msg As String)
    Public Event ErrorEvent(msg As String)
    Public Event WarningEvent(msg As String)

    Public Event LoopWaiting()

#End Region

    Public ReadOnly Property ErrorMessage() As String
        Get
            Return m_ErrorMessage
        End Get
    End Property

    Public Sub New(MSXmlGeneratorAppPath As String, WorkDir As String, Dataset As String, DebugLevel As Short, JobParams As IJobParams)

        mMSXmlGeneratorAppPath = MSXmlGeneratorAppPath
        m_WorkDir = WorkDir
        m_Dataset = Dataset
        m_DebugLevel = DebugLevel
        m_jobParams = JobParams

        m_ErrorMessage = String.Empty
    End Sub

    Public Function ConvertMzMLToMzXML() As Boolean

        Dim oProgRunner As clsRunDosProgram
        Dim ProgLoc As String
        Dim CmdStr As String

        Dim dtStartTimeUTC As DateTime
        Dim strSourceFilePath As String

        ' mzXML filename is dataset plus .mzXML
        Dim strMzXmlFilePath As String
        strMzXmlFilePath = Path.Combine(m_WorkDir, m_Dataset & clsAnalysisResources.DOT_MZXML_EXTENSION)

        If File.Exists(strMzXmlFilePath) OrElse
           File.Exists(strMzXmlFilePath & clsAnalysisResources.STORAGE_PATH_INFO_FILE_SUFFIX) Then
            ' File already exists; nothing to do
            Return True
        End If

        strSourceFilePath = Path.Combine(m_WorkDir, m_Dataset & clsAnalysisResources.DOT_MZML_EXTENSION)

        ProgLoc = mMSXmlGeneratorAppPath
        If Not File.Exists(ProgLoc) Then
            m_ErrorMessage = "MSXmlGenerator not found; unable to convert .mzML file to .mzXML"
            ReportError(m_ErrorMessage & ": " & mMSXmlGeneratorAppPath)
            Return False
        End If

        If m_DebugLevel >= 2 Then
            ReportDebugInfo("Creating the .mzXML file for " & m_Dataset & " using " & Path.GetFileName(strSourceFilePath))
        End If

        'Setup a program runner tool to call MSConvert
        oProgRunner = New clsRunDosProgram(m_WorkDir)

        'Set up command
        CmdStr = " " & clsAnalysisToolRunnerBase.PossiblyQuotePath(strSourceFilePath) & " --32 --mzXML -o " & m_WorkDir

        If m_DebugLevel > 0 Then
            ReportDebugInfo(ProgLoc & " " & CmdStr)
        End If

        With oProgRunner
            .CreateNoWindow = True
            .CacheStandardOutput = True
            .EchoOutputToConsole = True

            .WriteConsoleOutputToFile = False
            .ConsoleOutputFilePath = String.Empty      ' Allow the console output filename to be auto-generated
        End With


        dtStartTimeUTC = DateTime.UtcNow

        If Not oProgRunner.RunProgram(ProgLoc, CmdStr, "MSConvert", True) Then
            ' .RunProgram returned False
            m_ErrorMessage = "Error running " & Path.GetFileNameWithoutExtension(ProgLoc) &
                             " to convert the .mzML file to a .mzXML file"
            ReportError(m_ErrorMessage)
            Return False
        End If

        If m_DebugLevel >= 2 Then
            ReportDebugInfo(" ... mzXML file created")
        End If

        ' Validate that the .mzXML file was actually created
        If Not File.Exists(strMzXmlFilePath) Then
            m_ErrorMessage = ".mzXML file was not created by MSConvert"
            ReportError(m_ErrorMessage & ": " & strMzXmlFilePath)
            Return False
        End If

        If m_DebugLevel >= 1 Then
            mMSXmlGen.LogCreationStatsSourceToMsXml(dtStartTimeUTC, strSourceFilePath, strMzXmlFilePath)
        End If

        Return True
    End Function

    ''' <summary>
    ''' Generate the mzXML
    ''' </summary>
    ''' <returns>True if success; false if an error</returns>
    ''' <remarks></remarks>
    Public Function CreateMZXMLFile() As Boolean

        Dim dtStartTimeUTC As DateTime

        ' Turn on Centroiding, which will result in faster mzXML file generation time and smaller .mzXML files
        Dim CentroidMSXML = True

        Dim eOutputType As clsAnalysisResources.MSXMLOutputTypeConstants

        Dim blnSuccess As Boolean

        ' mzXML filename is dataset plus .mzXML
        Dim strMzXmlFilePath As String
        strMzXmlFilePath = Path.Combine(m_WorkDir, m_Dataset & clsAnalysisResources.DOT_MZXML_EXTENSION)

        If File.Exists(strMzXmlFilePath) OrElse
           File.Exists(strMzXmlFilePath & clsAnalysisResources.STORAGE_PATH_INFO_FILE_SUFFIX) Then
            ' File already exists; nothing to do
            Return True
        End If

        eOutputType = clsAnalysisResources.MSXMLOutputTypeConstants.mzXML

        ' Instantiate the processing class
        ' Note that mMSXmlGeneratorAppPath should have been populated by StoreToolVersionInfo() by an Analysis Manager plugin using clsAnalysisToolRunnerBase.GetMSXmlGeneratorAppPath()
        Dim strMSXmlGeneratorExe As String
        strMSXmlGeneratorExe = Path.GetFileName(mMSXmlGeneratorAppPath)

        If Not File.Exists(mMSXmlGeneratorAppPath) Then
            m_ErrorMessage = "MSXmlGenerator not found; unable to create .mzXML file"
            ReportError(m_ErrorMessage & ": " & mMSXmlGeneratorAppPath)
            Return False
        End If

        If m_DebugLevel >= 2 Then
            ReportDebugInfo("Creating the .mzXML file for " & m_Dataset)
        End If

        Dim rawDataType As String = m_jobParams.GetParam("RawDataType")
        Dim eRawDataType = clsAnalysisResources.GetRawDataType(rawDataType)

        If strMSXmlGeneratorExe.ToLower().Contains("readw") Then
            ' ReAdW
            ' mMSXmlGeneratorAppPath should have been populated during the call to StoreToolVersionInfo()

            mMSXmlGen = New clsMSXMLGenReadW(m_WorkDir, mMSXmlGeneratorAppPath, m_Dataset, eRawDataType, eOutputType, CentroidMSXML)

            If rawDataType <> clsAnalysisResources.RAW_DATA_TYPE_DOT_RAW_FILES Then
                m_ErrorMessage = "ReAdW can only be used with .Raw files, not with " & rawDataType
                ReportError(m_ErrorMessage)
                Return False
            End If

        ElseIf strMSXmlGeneratorExe.ToLower().Contains("msconvert") Then
            ' MSConvert

            ' Lookup Centroid Settings
            CentroidMSXML = m_jobParams.GetJobParameter("CentroidMSXML", True)
            Dim CentroidPeakCountToRetain As Integer

            ' Look for parameter CentroidPeakCountToRetain in the MSXMLGenerator section
            CentroidPeakCountToRetain = m_jobParams.GetJobParameter("MSXMLGenerator", "CentroidPeakCountToRetain", 0)

            If CentroidPeakCountToRetain = 0 Then
                ' Look for parameter CentroidPeakCountToRetain in any section
                CentroidPeakCountToRetain = m_jobParams.GetJobParameter("CentroidPeakCountToRetain",
                                                                        clsMSXmlGenMSConvert.
                                                                           DEFAULT_CENTROID_PEAK_COUNT_TO_RETAIN)
            End If

            ' Look for custom processing arguments
            Dim CustomMSConvertArguments = m_jobParams.GetJobParameter("MSXMLGenerator", "CustomMSConvertArguments", "")

            If String.IsNullOrWhiteSpace(CustomMSConvertArguments) Then
                mMSXmlGen = New clsMSXmlGenMSConvert(m_WorkDir, mMSXmlGeneratorAppPath, m_Dataset, eRawDataType,
                                                     eOutputType, CentroidMSXML, CentroidPeakCountToRetain)
            Else
                mMSXmlGen = New clsMSXmlGenMSConvert(m_WorkDir, mMSXmlGeneratorAppPath, m_Dataset, eRawDataType,
                                                     eOutputType, CustomMSConvertArguments)
            End If

        Else
            m_ErrorMessage = "Unsupported XmlGenerator: " & strMSXmlGeneratorExe
            ReportError(m_ErrorMessage)
            Return False
        End If

        dtStartTimeUTC = DateTime.UtcNow

        ' Create the file
        blnSuccess = mMSXmlGen.CreateMSXMLFile()

        If Not blnSuccess Then
            m_ErrorMessage = mMSXmlGen.ErrorMessage
            ReportError(mMSXmlGen.ErrorMessage)
            Return False

        ElseIf mMSXmlGen.ErrorMessage.Length > 0 Then
            ReportWarning(mMSXmlGen.ErrorMessage)
        End If

        ' Validate that the .mzXML file was actually created
        If Not File.Exists(strMzXmlFilePath) Then
            m_ErrorMessage = ".mzXML file was not created by " & strMSXmlGeneratorExe
            ReportError(m_ErrorMessage & ": " & strMzXmlFilePath)
            Return False
        End If

        If m_DebugLevel >= 1 Then
            mMSXmlGen.LogCreationStatsSourceToMsXml(dtStartTimeUTC, mMSXmlGen.SourceFilePath, strMzXmlFilePath)
        End If

        Return True
    End Function

    Protected Sub ReportDebugInfo(msg As String)
        RaiseEvent DebugEvent(msg)
    End Sub

    Protected Sub ReportError(msg As String)
        RaiseEvent ErrorEvent(msg)
    End Sub

    Protected Sub ReportWarning(msg As String)
        RaiseEvent WarningEvent(msg)
    End Sub

    Public Sub UpdateDatasetName(DatasetName As String)
        m_Dataset = DatasetName
    End Sub

#Region "Event Handlers"

    ''' <summary>
    ''' Event handler for MSXmlGenReadW.LoopWaiting event
    ''' </summary>
    ''' <remarks></remarks>
    Private Sub MSXmlGenReadW_LoopWaiting() Handles mMSXmlGen.LoopWaiting

        RaiseEvent LoopWaiting()
    End Sub

    ''' <summary>
    ''' Event handler for mMSXmlGen.ProgRunnerStarting event
    ''' </summary>
    ''' <param name="CommandLine">The command being executed (program path plus command line arguments)</param>
    ''' <remarks></remarks>
    Private Sub mMSXmlGenReadW_ProgRunnerStarting(CommandLine As String) Handles mMSXmlGen.ProgRunnerStarting
        If m_DebugLevel >= 1 Then
            ReportDebugInfo(CommandLine)
        End If
    End Sub

#End Region
End Class
