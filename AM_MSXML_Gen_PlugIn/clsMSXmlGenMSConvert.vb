
'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Created 05/10/2011
'
' Uses MSConvert to create a .mzXML or .mzML file
'*********************************************************************************************************

Option Strict On

Public Class clsMSXmlGenMSConvert
    Inherits clsMSXmlGen

#Region "Methods"

    Public Sub New(ByVal WorkDir As String, _
                   ByVal MSConvertProgramPath As String, _
                   ByVal DatasetName As String, _
                   ByVal eOutputType As MSXMLOutputTypeConstants, _
                   ByVal CentroidMSXML As Boolean)

        MyBase.New(WorkDir, MSConvertProgramPath, DatasetName, eOutputType, CentroidMSXML)

    End Sub

    
    Protected Overrides Function CreateArguments(ByVal msXmlFormat As String, ByVal RawFilePath As String) As String

        Dim CmdStr As String

        If mCentroidMSXML Then
            ' Note: MSConvert does not support centroiding
            CmdStr = " " & RawFilePath & " --" & msXmlFormat
        Else
            CmdStr = " " & RawFilePath & " --" & msXmlFormat
        End If

        Return CmdStr
    End Function

    Protected Overrides Function SetupTool() As Boolean

        Try
            ' Create a registry entry at HKEY_CURRENT_USER\Software\ProteoWizard
            ' to indicate that we agree to the Thermo license
            Dim regProteoWizard As Microsoft.Win32.RegistryKey = Microsoft.Win32.Registry.CurrentUser.CreateSubKey("ProteoWizard")

            regProteoWizard.SetValue("Thermo MSFileReader", "True")
            regProteoWizard.Close()

        Catch ex As Exception
            mErrorMessage = "Error creating ProteoWizard registry key: " & ex.Message
            Return False
        End Try

        Return True
    End Function

#End Region

End Class
