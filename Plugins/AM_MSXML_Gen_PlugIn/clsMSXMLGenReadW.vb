'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Created 07/19/2010
'
' Uses ReAdW to create a .mzXML or .mzML file
'*********************************************************************************************************

Option Strict On

Imports AnalysisManagerBase
Imports System.IO

Public Class clsMSXMLGenReadW
	Inherits clsMSXmlGen

    Public Overrides ReadOnly Property ProgramName As String
        Get
            Return "ReAdW"
        End Get
    End Property

#Region "Methods"

    Public Sub New(ByVal WorkDir As String,
      ByVal ReadWProgramPath As String,
      ByVal DatasetName As String,
      ByVal RawDataType As clsAnalysisResources.eRawDataTypeConstants,
      ByVal eOutputType As clsAnalysisResources.MSXMLOutputTypeConstants,
      ByVal CentroidMSXML As Boolean)

        MyBase.New(WorkDir, ReadWProgramPath, DatasetName, clsAnalysisResources.eRawDataTypeConstants.ThermoRawFile, eOutputType, CentroidMSXML)

        If RawDataType <> clsAnalysisResources.eRawDataTypeConstants.ThermoRawFile Then
            Const message As String = "clsMSXMLGenReadW can only be used to process Thermo .Raw files"
            Throw New ArgumentOutOfRangeException(message)
        End If

        mUseProgRunnerResultCode = True

    End Sub

	Protected Overrides Function CreateArguments(ByVal msXmlFormat As String, ByVal RawFilePath As String) As String

		Dim CmdStr As String

		If mProgramPath.ToLower.Contains("\v2.") Then
			' Version 2.x syntax
			' Syntax is: readw <raw file path> <c/p> [<output file>]

			If mCentroidMS1 OrElse mCentroidMS2 Then
				' Centroiding is enabled
				CmdStr = " " & RawFilePath & " c"
			Else
				CmdStr = " " & RawFilePath & " p"
			End If

		Else
			' Version 3 or higher
			' Syntax is ReAdW [options] <raw file path> [<output file>]
			'  where Options will include --mzXML and possibly -c

			If mCentroidMS1 OrElse mCentroidMS2 Then
				' Centroiding is enabled
				CmdStr = " --" & msXmlFormat & " " & " -c " & RawFilePath
			Else
				CmdStr = " --" & msXmlFormat & " " & RawFilePath
			End If
		End If


		Return CmdStr
	End Function

	Protected Overrides Function SetupTool() As Boolean

        ' No special setup is required for ReAdW
		Return True

    End Function

#End Region

End Class
