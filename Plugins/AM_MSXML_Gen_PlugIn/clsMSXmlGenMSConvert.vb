
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

	Public Const DEFAULT_CENTROID_PEAK_COUNT_TO_RETAIN As Integer = 150

	Protected mCentroidPeakCountToRetain As Integer

#Region "Methods"

	Public Sub New(ByVal WorkDir As String, _
				   ByVal MSConvertProgramPath As String, _
				   ByVal DatasetName As String, _
				   ByVal eOutputType As MSXMLOutputTypeConstants, _
				   ByVal CentroidMSXML As Boolean, _
				   ByVal CentroidPeakCountToRetain As Integer)

		MyBase.New(WorkDir, MSConvertProgramPath, DatasetName, eOutputType, CentroidMSXML)

		mCentroidPeakCountToRetain = CentroidPeakCountToRetain

		mUseProgRunnerResultCode = False

	End Sub

    
    Protected Overrides Function CreateArguments(ByVal msXmlFormat As String, ByVal RawFilePath As String) As String

        Dim CmdStr As String

		CmdStr = " " & RawFilePath

		If mCentroidMSXML Then
			' Centroid the data by first applying the peak-picking algorithm, then keeping the top N data points
			' Syntax details:
			'   peakPicking prefer_vendor:<true|false>  int_set(MS levels)
			'   threshold <count|count-after-ties|absolute|bpi-relative|tic-relative|tic-cutoff> <threshold> <most-intense|least-intense> [int_set(MS levels)]

			' So, the following means to apply peak picking to all spectra (MS1 and MS2) and then keep the top 150 peaks (sorted by intensity)
			' --filter "peakPicking true 1-" --filter "threshold count 150 most-intense"

			If mCentroidPeakCountToRetain = 0 Then
				mCentroidPeakCountToRetain = DEFAULT_CENTROID_PEAK_COUNT_TO_RETAIN
			ElseIf mCentroidPeakCountToRetain < 25 Then
				mCentroidPeakCountToRetain = 25
			End If

			CmdStr &= " --filter ""peakPicking true 1-"" --filter ""threshold count " & mCentroidPeakCountToRetain & " most-intense"""
		End If

		CmdStr &= " --" & msXmlFormat & " -o " & mWorkDir

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
