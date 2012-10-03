'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Created 07/19/2010
'
' Uses ReadW to create a .mzXML or .mzML file
'*********************************************************************************************************

Option Strict On

Public Class clsMSXMLGenReadW
    Inherits clsMSXmlGen

#Region "Methods"

    Public Sub New(ByVal WorkDir As String, _
                   ByVal ReadWProgramPath As String, _
                   ByVal DatasetName As String, _
                   ByVal eOutputType As MSXMLOutputTypeConstants, _
                   ByVal CentroidMSXML As Boolean)

        MyBase.New(WorkDir, ReadWProgramPath, DatasetName, eOutputType, CentroidMSXML)

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

        ' No special setup is required for ReadW
        Return True

    End Function

#End Region

End Class
