
'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Created 05/10/2011
'
' Uses MSConvert to create a .mzXML or .mzML file
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
	Protected mCustomMSConvertArguments As String

#Region "Methods"

	Public Sub New(ByVal WorkDir As String,
	  ByVal MSConvertProgramPath As String,
	  ByVal DatasetName As String,
	  ByVal RawDataType As clsAnalysisResources.eRawDataTypeConstants,
	  ByVal eOutputType As clsAnalysisResources.MSXMLOutputTypeConstants,
	  ByVal CustomMSConvertArguments As String)

		MyBase.New(WorkDir, MSConvertProgramPath, DatasetName, RawDataType, eOutputType, CentroidMSXML:=False)

		mCustomMSConvertArguments = CustomMSConvertArguments

		mUseProgRunnerResultCode = False

	End Sub

	Public Sub New(ByVal WorkDir As String,
	  ByVal MSConvertProgramPath As String,
	  ByVal DatasetName As String,
	  ByVal RawDataType As clsAnalysisResources.eRawDataTypeConstants,
	  ByVal eOutputType As clsAnalysisResources.MSXMLOutputTypeConstants,
	  ByVal CentroidMSXML As Boolean,
	  ByVal CentroidPeakCountToRetain As Integer)

		MyBase.New(WorkDir, MSConvertProgramPath, DatasetName, RawDataType, eOutputType, CentroidMSXML)

		mCentroidPeakCountToRetain = CentroidPeakCountToRetain

		mUseProgRunnerResultCode = False

	End Sub

	Public Sub New(ByVal WorkDir As String,
	  ByVal MSConvertProgramPath As String,
	  ByVal DatasetName As String,
	  ByVal RawDataType As clsAnalysisResources.eRawDataTypeConstants,
	  ByVal eOutputType As clsAnalysisResources.MSXMLOutputTypeConstants,
	  ByVal CentroidMS1 As Boolean,
	  ByVal CentroidMS2 As Boolean,
	  ByVal CentroidPeakCountToRetain As Integer)

		MyBase.New(WorkDir, MSConvertProgramPath, DatasetName, RawDataType, eOutputType, CentroidMS1, CentroidMS2)

		mCentroidPeakCountToRetain = CentroidPeakCountToRetain

		mUseProgRunnerResultCode = False

	End Sub

	Protected Overrides Function CreateArguments(ByVal msXmlFormat As String, ByVal RawFilePath As String) As String

		Dim CmdStr As String

		CmdStr = " " & RawFilePath

		If String.IsNullOrWhiteSpace(mCustomMSConvertArguments) Then

			If mCentroidMS1 OrElse mCentroidMS2 Then
				' Centroid the data by first applying the peak-picking algorithm, then keeping the top N data points
				' Syntax details:
				'   peakPicking prefer_vendor:<true|false>  int_set(MS levels)
				'   threshold <count|count-after-ties|absolute|bpi-relative|tic-relative|tic-cutoff> <threshold> <most-intense|least-intense> [int_set(MS levels)]

				' So, the following means to apply peak picking to all spectra (MS1 and MS2) and then keep the top 150 peaks (sorted by intensity)
				' --filter "peakPicking true 1-" --filter "threshold count 150 most-intense"

				If mCentroidMS1 And Not mCentroidMS2 Then
					CmdStr &= " --filter ""peakPicking true 1"""
				ElseIf Not mCentroidMS1 And mCentroidMS2 Then
					CmdStr &= " --filter ""peakPicking true 2-"""
				Else
					CmdStr &= " --filter ""peakPicking true 1-"""
				End If

				If mCentroidPeakCountToRetain < 0 Then
					' Keep all points
				Else
					If mCentroidPeakCountToRetain = 0 Then
						mCentroidPeakCountToRetain = DEFAULT_CENTROID_PEAK_COUNT_TO_RETAIN
					ElseIf mCentroidPeakCountToRetain < 25 Then
						mCentroidPeakCountToRetain = 25
					End If

					CmdStr &= " --filter ""threshold count " & mCentroidPeakCountToRetain & " most-intense"""
				End If
				
			End If

			CmdStr &= " --" & msXmlFormat & " --32"
		Else
			CmdStr &= " " & mCustomMSConvertArguments
		End If

		CmdStr &= "  -o " & mWorkDir

		Return CmdStr

	End Function

	Protected Overrides Function SetupTool() As Boolean

		' Tool setup for MSConvert involves creating a
		'  registry entry at HKEY_CURRENT_USER\Software\ProteoWizard
		'  to indicate that we agree to the Thermo license

		Dim objProteowizardTools As clsProteowizardTools
		objProteowizardTools = New clsProteowizardTools(mDebugLevel)

		Return objProteowizardTools.RegisterProteoWizard()

	End Function

#End Region

End Class
