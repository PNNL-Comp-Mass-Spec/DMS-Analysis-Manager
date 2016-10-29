
'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Created 05/10/2011
'
' Uses MSConvert to create a .mzXML or .mzML file
' Also used by RecalculatePrecursorIonsUpdateMzML in clsAnalysisToolRunnerMSXMLGen to re-index a .mzML file to create a new .mzML file
'*********************************************************************************************************

Option Strict On

Imports AnalysisManagerBase

Public Class clsMSXmlGenMSConvert
    Inherits clsMSXmlGen

    Public Const DEFAULT_CENTROID_PEAK_COUNT_TO_RETAIN As Integer = 500

    ''' <summary>
    ''' Number of data points to keep when centroiding
    ''' </summary>
    ''' <remarks>0 to keep default (500); -1 to keep all</remarks>
    Protected mCentroidPeakCountToRetain As Integer

    ''' <summary>
    ''' Custom arguments that will override the auto-defined arguments
    ''' </summary>
    ''' <remarks></remarks>
    Protected ReadOnly mCustomMSConvertArguments As String

    Protected Overrides ReadOnly Property ProgramName As String
        Get
            Return "MSConvert"
        End Get
    End Property

#Region "Methods"

    Public Sub New(WorkDir As String,
      msConvertProgramPath As String,
      datasetName As String,
      rawDataType As clsAnalysisResources.eRawDataTypeConstants,
      eOutputType As clsAnalysisResources.MSXMLOutputTypeConstants,
      customMSConvertArguments As String)

        MyBase.New(WorkDir, msConvertProgramPath, datasetName, rawDataType, eOutputType, centroidMSXML:=False)

        mCustomMSConvertArguments = customMSConvertArguments

        mUseProgRunnerResultCode = False
    End Sub

    Public Sub New(workDir As String,
      msConvertProgramPath As String,
      datasetName As String,
      rawDataType As clsAnalysisResources.eRawDataTypeConstants,
      eOutputType As clsAnalysisResources.MSXMLOutputTypeConstants,
      centroidMSXML As Boolean,
      centroidPeakCountToRetain As Integer)

        MyBase.New(workDir, msConvertProgramPath, datasetName, rawDataType, eOutputType, centroidMSXML)

        mCentroidPeakCountToRetain = centroidPeakCountToRetain

        mUseProgRunnerResultCode = False
    End Sub

    Public Sub New(workDir As String,
      msConvertProgramPath As String,
      datasetName As String,
      rawDataType As clsAnalysisResources.eRawDataTypeConstants,
      eOutputType As clsAnalysisResources.MSXMLOutputTypeConstants,
      centroidMS1 As Boolean,
      centroidMS2 As Boolean,
      centroidPeakCountToRetain As Integer)

        MyBase.New(workDir, msConvertProgramPath, datasetName, rawDataType, eOutputType, centroidMS1, centroidMS2)

        mCentroidPeakCountToRetain = centroidPeakCountToRetain

        mUseProgRunnerResultCode = False
    End Sub

    Protected Overrides Function CreateArguments(msXmlFormat As String, rawFilePath As String) As String

        Dim cmdStr = " " & clsGlobal.PossiblyQuotePath(rawFilePath)

        If String.IsNullOrWhiteSpace(mCustomMSConvertArguments) Then

            If mCentroidMS1 OrElse mCentroidMS2 Then
                ' Centroid the data by first applying the peak-picking algorithm, then keeping the top N data points
                ' Syntax details:
                '   peakPicking prefer_vendor:<true|false>  int_set(MS levels)
                '   threshold <count|count-after-ties|absolute|bpi-relative|tic-relative|tic-cutoff> <threshold> <most-intense|least-intense> [int_set(MS levels)]

                ' So, the following means to apply peak picking to all spectra (MS1 and MS2) and then keep the top 150 peaks (sorted by intensity)
                ' --filter "peakPicking true 1-" --filter "threshold count 150 most-intense"

                If mCentroidMS1 And Not mCentroidMS2 Then
                    cmdStr &= " --filter ""peakPicking true 1"""
                ElseIf Not mCentroidMS1 And mCentroidMS2 Then
                    cmdStr &= " --filter ""peakPicking true 2-"""
                Else
                    cmdStr &= " --filter ""peakPicking true 1-"""
                End If

                If mCentroidPeakCountToRetain < 0 Then
                    ' Keep all points
                Else
                    If mCentroidPeakCountToRetain = 0 Then
                        mCentroidPeakCountToRetain = DEFAULT_CENTROID_PEAK_COUNT_TO_RETAIN
                    ElseIf mCentroidPeakCountToRetain < 25 Then
                        mCentroidPeakCountToRetain = 25
                    End If

                    cmdStr &= " --filter ""threshold count " & mCentroidPeakCountToRetain & " most-intense"""
                End If

            End If

            cmdStr &= " --" & msXmlFormat & " --32"
        Else
            cmdStr &= " " & mCustomMSConvertArguments
        End If

        mOutputFileName = GetOutputFileName(msXmlFormat, rawFilePath, mRawDataType)

        ' Specify the output directory and the output file name
        cmdStr &= "  -o " & mWorkDir & " --outfile " & mOutputFileName

        Return cmdStr

    End Function

    Protected Overrides Function GetOutputFileName(msXmlFormat As String, rawFilePath As String, rawDataType As clsAnalysisResources.eRawDataTypeConstants) As String

        If String.Equals(msXmlFormat, "mzML", StringComparison.InvariantCultureIgnoreCase) AndAlso
          mRawDataType = clsAnalysisResources.eRawDataTypeConstants.mzML Then
            ' Input and output files are both .mzML
            Return IO.Path.GetFileNameWithoutExtension(rawFilePath) & "_new" & clsAnalysisResources.DOT_MZML_EXTENSION
        ElseIf String.Equals(msXmlFormat, "mzXML", StringComparison.InvariantCultureIgnoreCase) AndAlso
               mRawDataType = clsAnalysisResources.eRawDataTypeConstants.mzXML Then
            ' Input and output files are both .mzXML
            Return IO.Path.GetFileNameWithoutExtension(rawFilePath) & "_new" & clsAnalysisResources.DOT_MZXML_EXTENSION
        Else
            Return IO.Path.GetFileName(IO.Path.ChangeExtension(rawFilePath, msXmlFormat))
        End If

    End Function

    Protected Overrides Function SetupTool() As Boolean

        ' Tool setup for MSConvert involves creating a
        '  registry entry at HKEY_CURRENT_USER\Software\ProteoWizard
        '  to indicate that we agree to the Thermo license

        Dim objProteowizardTools As clsProteowizardTools
        objProteowizardTools = New clsProteowizardTools(DebugLevel)

        Return objProteowizardTools.RegisterProteoWizard()
    End Function

#End Region
End Class
