'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Created 04/12/2012
'
' Uses MSConvert to create a .MGF file from a .Raw file or .mzXML file or .mzML file
' Next, converts the .MGF file to a _DTA.txt file
'*********************************************************************************************************

Option Strict On

Imports AnalysisManagerBase
Imports System.IO

Public Class clsDtaGenMSConvert
    Inherits clsDtaGenThermoRaw

    Public Const DEFAULT_CENTROID_PEAK_COUNT_TO_RETAIN As Integer = 250

    Private mForceCentroidOn As Boolean = False

    Public Property ForceCentroidOn As Boolean
        Get
            Return mForceCentroidOn
        End Get
        Set(value As Boolean)
            mForceCentroidOn = value
        End Set
    End Property

    Public Overrides Sub Setup(initParams As ISpectraFileProcessor.InitializationParams, toolRunner As clsAnalysisToolRunnerBase)
        MyBase.Setup(initParams, toolRunner)

        ' Tool setup for MSConvert involves creating a
        '  registry entry at HKEY_CURRENT_USER\Software\ProteoWizard
        '  to indicate that we agree to the Thermo license

        Dim objProteowizardTools = New clsProteowizardTools(m_DebugLevel)

        If Not objProteowizardTools.RegisterProteoWizard() Then
            Throw New Exception("Unable to register ProteoWizard")
        End If

    End Sub

    ''' <summary>
    ''' Returns the default path to the DTA generator tool
    ''' </summary>
    ''' <returns></returns>
    ''' <remarks>The default path can be overridden by updating m_DtaToolNameLoc using clsDtaGen.UpdateDtaToolNameLoc</remarks>
    Protected Overrides Function ConstructDTAToolPath() As String

        Dim strDTAToolPath As String

        Dim ProteoWizardDir As String = m_MgrParams.GetParam("ProteoWizardDir")         ' MSConvert.exe is stored in the ProteoWizard folder
        strDTAToolPath = Path.Combine(ProteoWizardDir, MSCONVERT_FILENAME)

        Return strDTAToolPath

    End Function

    Protected Overrides Sub MakeDTAFilesThreaded()

        m_Status = ISpectraFileProcessor.ProcessStatus.SF_RUNNING
        m_ErrMsg = String.Empty

        m_Progress = 10

        If Not ConvertRawToMGF(m_RawDataType) Then
            If m_Status <> ISpectraFileProcessor.ProcessStatus.SF_ABORTING Then
                m_Results = ISpectraFileProcessor.ProcessResults.SF_FAILURE
                m_Status = ISpectraFileProcessor.ProcessStatus.SF_ERROR
            End If
            Return
        End If

        m_Progress = 75

        If Not ConvertMGFtoDTA() Then
            If m_Status <> ISpectraFileProcessor.ProcessStatus.SF_ABORTING Then
                m_Results = ISpectraFileProcessor.ProcessResults.SF_FAILURE
                m_Status = ISpectraFileProcessor.ProcessStatus.SF_ERROR
            End If
            Return
        End If

        m_Results = ISpectraFileProcessor.ProcessResults.SF_SUCCESS
        m_Status = ISpectraFileProcessor.ProcessStatus.SF_COMPLETE

    End Sub

    ''' <summary>
    ''' Convert .mgf file to _DTA.txt using MascotGenericFileToDTA.dll
    ''' This function is called by MakeDTAFilesThreaded
    ''' </summary>
    ''' <returns>TRUE for success; FALSE for failure</returns>
    ''' <remarks></remarks>
    Private Function ConvertMGFtoDTA() As Boolean

        Try
            Dim strRawDataType As String = m_JobParams.GetJobParameter("RawDataType", String.Empty)

            Dim oMGFConverter = New clsMGFConverter(m_DebugLevel, m_WorkDir) With {
                .IncludeExtraInfoOnParentIonLine = True,
                .MinimumIonsPerSpectrum = 0
            }

            Dim eRawDataType = clsAnalysisResources.GetRawDataType(strRawDataType)
            Dim blnSuccess = oMGFConverter.ConvertMGFtoDTA(eRawDataType, m_Dataset)

            If Not blnSuccess Then
                m_ErrMsg = oMGFConverter.ErrorMessage
            End If

            m_SpectraFileCount = oMGFConverter.SpectraCountWritten
            m_Progress = 95

            Return blnSuccess

        Catch ex As Exception
            OnErrorEvent("Exception in ConvertMGFtoDTA", ex)
            Return False
        End Try

    End Function

    ''' <summary>
    ''' Create .mgf file using MSConvert
    ''' This function is called by MakeDTAFilesThreaded
    ''' </summary>
    ''' <param name="eRawDataType">Raw data file type</param>
    ''' <returns>TRUE for success; FALSE for failure</returns>
    ''' <remarks></remarks>
    Private Function ConvertRawToMGF(eRawDataType As clsAnalysisResources.eRawDataTypeConstants) As Boolean

        Try

            If m_DebugLevel > 0 Then
                OnStatusEvent("Creating .MGF file using MSConvert")
            End If

            Dim rawFilePath As String

            ' Construct the path to the .raw file
            Select Case eRawDataType
                Case clsAnalysisResources.eRawDataTypeConstants.ThermoRawFile
                    rawFilePath = Path.Combine(m_WorkDir, m_Dataset & clsAnalysisResources.DOT_RAW_EXTENSION)
                Case clsAnalysisResources.eRawDataTypeConstants.mzXML
                    rawFilePath = Path.Combine(m_WorkDir, m_Dataset & clsAnalysisResources.DOT_MZXML_EXTENSION)
                Case clsAnalysisResources.eRawDataTypeConstants.mzML
                    rawFilePath = Path.Combine(m_WorkDir, m_Dataset & clsAnalysisResources.DOT_MZML_EXTENSION)
                Case Else
                    m_ErrMsg = "Raw data file type not supported: " & eRawDataType.ToString()
                    Return False
            End Select

            m_InstrumentFileName = Path.GetFileName(rawFilePath)
            m_JobParams.AddResultFileToSkip(m_InstrumentFileName)

            Const scanStart = 1
            Dim scanStop = DEFAULT_SCAN_STOP

            If eRawDataType = clsAnalysisResources.eRawDataTypeConstants.ThermoRawFile Then
                'Get the maximum number of scans in the file
                m_MaxScanInFile = GetMaxScan(rawFilePath)
            Else
                m_MaxScanInFile = scanStop
            End If

            Select Case m_MaxScanInFile
                Case -1
                    ' Generic error getting number of scans
                    m_ErrMsg = "Unknown error getting number of scans; Maxscan = " & m_MaxScanInFile.ToString
                    Return False
                Case 0
                    ' Unable to read file; treat this is a warning
                    m_ErrMsg = "Warning: unable to get maxscan; Maxscan = 0"
                Case Is > 0
                    ' This is normal, do nothing
                Case Else
                    ' This should never happen
                    m_ErrMsg = "Critical error getting number of scans; Maxscan = " & m_MaxScanInFile.ToString
                    Return False
            End Select

            Dim blnLimitingScanRange = False

            'Verify max scan specified is in file
            If m_MaxScanInFile > 0 Then
                If scanStart = 1 AndAlso scanStop = 999999 AndAlso scanStop < m_MaxScanInFile Then
                    ' The default scan range for processing all scans has traditionally be 1 to 999999
                    ' This scan range is defined for this job's settings file, but this dataset has over 1 million spectra
                    ' Assume that the user actually wants to analyze all of the spectra
                    scanStop = m_MaxScanInFile
                End If

                If scanStop > m_MaxScanInFile Then scanStop = m_MaxScanInFile
                If scanStop < m_MaxScanInFile Then blnLimitingScanRange = True
                If scanStart > 1 Then blnLimitingScanRange = True
            Else
                If scanStart > 1 Or scanStop < DEFAULT_SCAN_STOP Then blnLimitingScanRange = True
            End If

            'Determine max number of scans to be used
            m_NumScans = scanStop - scanStart + 1

            ' Lookup Centroid Settings
            Dim centroidMGF = m_JobParams.GetJobParameter("CentroidMGF", False)

            ' Look for parameter CentroidPeakCountToRetain in the DtaGenerator section
            Dim centroidPeakCountToRetain = m_JobParams.GetJobParameter("DtaGenerator", "CentroidPeakCountToRetain", 0)

            If centroidPeakCountToRetain = 0 Then
                ' Look for parameter CentroidPeakCountToRetain in any section
                centroidPeakCountToRetain = m_JobParams.GetJobParameter("CentroidPeakCountToRetain", DEFAULT_CENTROID_PEAK_COUNT_TO_RETAIN)
            End If

            If mForceCentroidOn Then
                centroidMGF = True
            End If

            'Set up command
            Dim cmdStr = " " & rawFilePath

            If centroidMGF Then
                ' Centroid the data by first applying the peak-picking algorithm, then keeping the top N data points
                ' Syntax details:
                '   peakPicking prefer_vendor:<true|false>  int_set(MS levels)
                '   threshold <count|count-after-ties|absolute|bpi-relative|tic-relative|tic-cutoff> <threshold> <most-intense|least-intense> [int_set(MS levels)]

                ' So, the following means to apply peak picking to all spectra (MS1 and MS2) and then keep the top 250 peaks (sorted by intensity)
                ' --filter "peakPicking true 1-" --filter "threshold count 250 most-intense"

                If centroidPeakCountToRetain = 0 Then
                    centroidPeakCountToRetain = DEFAULT_CENTROID_PEAK_COUNT_TO_RETAIN
                ElseIf centroidPeakCountToRetain < 25 Then
                    centroidPeakCountToRetain = 25
                End If

                cmdStr &= " --filter ""peakPicking true 1-"" --filter ""threshold count " & centroidPeakCountToRetain & " most-intense"""
            End If

            If blnLimitingScanRange Then
                cmdStr &= " --filter ""scanNumber [" & scanStart & "," & scanStop & "]"""
            End If

            cmdStr &= " --mgf -o " & m_WorkDir

            If m_DebugLevel > 0 Then
                OnStatusEvent(m_DtaToolNameLoc & " " & cmdStr)
            End If

            'Setup a program runner tool to make the spectra files
            mCmdRunner = New clsRunDosProgram(m_WorkDir) With {
                .CreateNoWindow = True,
                .CacheStandardOutput = True,
                .EchoOutputToConsole = True,
                .WriteConsoleOutputToFile = True,
                .ConsoleOutputFilePath = String.Empty      ' Allow the console output filename to be auto-generated
            }
            AddHandler mCmdRunner.ErrorEvent, AddressOf CmdRunner_ErrorEvent
            AddHandler mCmdRunner.LoopWaiting, AddressOf CmdRunner_LoopWaiting

            If Not mCmdRunner.RunProgram(m_DtaToolNameLoc, cmdStr, "MSConvert", True) Then
                ' .RunProgram returned False
                LogDTACreationStats("ConvertRawToMGF", Path.GetFileNameWithoutExtension(m_DtaToolNameLoc), "mCmdRunner.RunProgram returned False")

                m_ErrMsg = "Error running " & Path.GetFileNameWithoutExtension(m_DtaToolNameLoc)
                Return False
            End If

            If m_DebugLevel >= 2 Then
                OnStatusEvent(" ... MGF file created using MSConvert")
            End If

            Return True

        Catch ex As Exception
            OnErrorEvent("Exception in ConvertRawToMGF", ex)
            Return False
        End Try

    End Function

End Class
